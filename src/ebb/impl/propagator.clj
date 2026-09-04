;; Derived from missionary/impl/Propagator.cljs (missionary, EPL 2.0).
;; See Propagator.java for the synchronisation strategy (ADR-001 rule 1).
(ns ^:no-doc ebb.impl.propagator
  (:require [ebb.impl.pairing-heap :as ph])
  (:require [ebb.impl.util :as u :refer [cancelled]]))

(declare sub unsub transfer)

(defn lt [x y]
  (let [xl (alength x)
        yl (alength y)
        ml (min xl yl)]
    (loop [i 0]
      (if (< i ml)
        (let [xi (aget x i)
              yi (aget y i)]
          (if (= xi yi)
            (recur (inc i))
            (< xi yi)))
        (> xl yl)))))

(deftype Publisher [ranks strategy arg effect ^:unsynchronized-mutable node ^:unsynchronized-mutable current]
  clojure.lang.IFn
  (invoke [this lcb rcb]
    (sub this lcb rcb))

  IComparable
  (-compare [this that]
    (if (identical? this that)
      0 (if (lt (.-ranks this) (.-ranks that))
          -1 +1))))

(deftype Process [parent ^:unsynchronized-mutable pressure ^:unsynchronized-mutable owned ^:unsynchronized-mutable input ^:unsynchronized-mutable child ^:unsynchronized-mutable sibling ^:unsynchronized-mutable ready ^:unsynchronized-mutable pending ^:unsynchronized-mutable failed ^:unsynchronized-mutable dirty ^:unsynchronized-mutable flag ^:unsynchronized-mutable state])

(deftype Subscription [source target lcb rcb ^:unsynchronized-mutable prev ^:unsynchronized-mutable next ^:unsynchronized-mutable ready ^:unsynchronized-mutable state]
  clojure.lang.IFn
  (invoke [this]
    (unsub this))
  clojure.lang.IDeref
  (deref [this]
    (transfer this)))

(deftype Context [^:unsynchronized-mutable cursor ^:unsynchronized-mutable process ^:unsynchronized-mutable reacted ^:unsynchronized-mutable delayed ^:unsynchronized-mutable buffer])

(defprotocol Strategy
  (tick [_ ps])
  (publish [_ ps])
  (refresh [_ ps])
  (subscribe [_ sub idle])
  (unsubscribe [_ sub idle])
  (accept [_ sub idle])
  (reject [_ sub idle]))

;; THE PROPAGATION CONTEXT, ADR-001 rules 1 and 6.
;;
;; `Propagator.java` keeps this in a `ThreadLocal<Context>` with
;; `withInitial(Context::new)`; `Propagator.cljs` uses one global, which is the
;; same thing when there is one thread. Ebb had the global, and it is the exact
;; defect ebb-8nq.30 fixed in the reactor: `enter` claims the outermost slot by
;; finding `cursor` nil and `leave` releases it, so two propagations sharing one
;; Context see each other's -- one of them is told it is nested, does not drain
;; `delayed`, and the process waiting there is never ticked. It shows up as a
;; flow that simply stops.
;;
;; Two cells, because the ThreadLocal is doing two jobs at once:
;;
;;   `cached`  a Context per executor, reused between propagations. That is
;;             `withInitial`: nobody else may share it, and reallocating one per
;;             propagation would be waste.
;;   `active`  the Context of the propagation IN FLIGHT, and a CARRIED local
;;             (ADR-001 rule 6) -- it travels with `server/call!`, both ways, so
;;             a notifier marshalled onto an ap's owner fiber propagates into
;;             the caller's Context exactly as the JVM's inline call does.
;;
;; Splitting them is what keeps `server/cast!` a cast. A carried local that is
;; always set would make every callback synchronous again (see `cast!`); this
;; one is set only between `enter` and the `leave` that matches it.
(def ^:private cached (u/context-local))
(def ^:private active (u/carried-local))

(defn context
  "The Context this propagation belongs to."
  ^Context []
  (or (u/local-get active)
      (u/local-get cached)
      (u/local-set! cached (->Context nil nil nil nil (object-array 1)))))

(def ceiling (object-array 0))

;;
;; MUTABLE TOP-LEVEL STATE. The cljs impl uses plain `def` plus `set!`,
;; which ClojureScript allows and jolt (like Clojure) does not -- `set!`
;; on a var needs a thread-local binding, and a `binding` is wrong here
;; anyway. These are atoms instead, read with @ and written with reset!,
;; which is what missionary's `fiber` global amounts to as well.
(def root (atom 0))

(defn acquire [_])

(defn release [_])

(def impl
  (ph/impl
    (fn [_ ^Process x ^Process y] (lt (.-ranks (.-parent x)) (.-ranks (.-parent y))))
    (fn [_ ^Process x] (.-child x))
    (fn [_ ^Process x y] (set! (.-child x) y))
    (fn [_ ^Process x] (.-sibling x))
    (fn [_ ^Process x y] (set! (.-sibling x) y))))

(defn enqueue [^Process r ^Process p]
  (set! (.-child p) nil)
  (if (nil? r) p (ph/meld impl nil p r)))

(defn schedule [^Process ps]
  (let [^Context ctx (context)
        ^Publisher pub (.-parent ps)]
    (if (lt (.-cursor ctx) (.-ranks pub))
      (set! (.-reacted ctx) (enqueue (.-reacted ctx) ps))
      (set! (.-delayed ctx) (enqueue (.-delayed ctx) ps)))))

(defn crash [^Process ps e]
  (let [^Publisher pub (.-parent ps)]
    (set! (.-state ps) e)
    (set! (.-failed ps) true)
    (when (identical? (.-current pub) ps)
      ((.-input ps)))))

(defn ack [^Process ps]
  (zero? (set! (.-pressure ps) (dec (.-pressure ps)))))

(defn detach [^Subscription s]
  (let [^Subscription p (.-prev s)
        ^Subscription n (.-next s)]
    (set! (.-prev s) nil)
    (set! (.-next s) nil)
    (when-not (identical? p s)
      (set! (.-next p) n)
      (set! (.-prev n) p))))

(defn attach [^Subscription p ^Subscription s]
  (if (nil? p)
    (do (set! (.-prev s) s)
        (set! (.-next s) s))
    (let [^Subscription n (.-next p)]
      (set! (.-next p) s)
      (set! (.-prev s) p)
      (set! (.-next s) n)
      (set! (.-prev n) s))))

(defn union [^Subscription p ^Subscription s]
  (when-not (nil? p)
    (let [^Subscription n (.-next p)
          ^Subscription t (.-next s)]
      (set! (.-next p) t)
      (set! (.-prev t) p)
      (set! (.-next s) n)
      (set! (.-prev n) s))))

(defn clear [^Subscription head]
  (loop [^Subscription sub head]
    (let [n (.-next sub)]
      (set! (.-prev sub) nil)
      (set! (.-next sub) nil)
      (when-not (identical? n head)
        (recur n)))))

(defn bufferize [^Context ctx ^Subscription head]
  (loop [i 0
         ^Subscription sub head
         ^objects buf (.-buffer ctx)]
    (set! (.-ready sub) false)
    (aset buf i sub)
    (let [i (inc i)
          sub (.-next sub)
          cap (alength buf)
          buf (if (= i cap)
                (let [arr (object-array (bit-shift-left cap 1))]
                  (dotimes [i cap]
                    (aset arr i (aget buf i)))
                  arr) buf)]
      (if (identical? sub head)
        (set! (.-buffer ctx) buf)
        (recur i sub buf)))))

(defn terminate [^Context ctx ^Process ps]
  (let [^Publisher pub (.-parent ps)]
    (set! (.-input ps) nil)
    ;; NOT named `ready` -- see doc/conformance.md: the snapshot would come back
    ;; as the nil just assigned.
    (when-some [^Subscription v-ready (.-ready ps)]
      (set! (.-ready ps) nil)
      (bufferize ctx v-ready)
      (clear v-ready))
    (release pub)))

(defn invalidate [^Context ctx ^Process ps]
  (let [^Publisher pub (.-parent ps)]
    (set! (.-dirty ps) true)
    (when-some [^Subscription v-ready (.-ready ps)]
      (set! (.-ready ps) nil)
      (bufferize ctx v-ready)
      (union (.-pending ps)
        (set! (.-pending ps) v-ready)))
    (release pub)))

(defn step-all [^Process ps]
  (let [^Context ctx (context)]
    (invalidate ctx ps)
    (let [^objects buf (.-buffer ctx)]
      (loop [i 0]
        (when-some [^Subscription sub (aget buf i)]
          (aset buf i nil)
          (set! (.-process ctx) (.-source sub))
          ((.-lcb sub))
          (recur (inc i)))))))

(defn done-all [^Process ps]
  (let [^Context ctx (context)]
    (terminate ctx ps)
    (let [^objects buf (.-buffer ctx)]
      (loop [i 0]
        (when-some [^Subscription sub (aget buf i)]
          (aset buf i nil)
          (set! (.-process ctx) (.-source sub))
          ((.-rcb sub))
          (recur (inc i)))))))

(defn success-all [^Process ps]
  (let [^Context ctx (context)]
    (terminate ctx ps)
    (let [^objects buf (.-buffer ctx)]
      (loop [i 0]
        (when-some [^Subscription sub (aget buf i)]
          (aset buf i nil)
          (set! (.-process ctx) (.-source sub))
          ((.-lcb sub) (.-state ps))
          (recur (inc i)))))))

(defn failure-all [^Process ps]
  (let [^Context ctx (context)]
    (terminate ctx ps)
    (let [^objects buf (.-buffer ctx)]
      (loop [i 0]
        (when-some [^Subscription sub (aget buf i)]
          (aset buf i nil)
          (set! (.-process ctx) (.-source sub))
          ((.-rcb sub) (.-state ps))
          (recur (inc i)))))))

(defn detach-ready [^Subscription sub]
  (let [^Process ps (.-target sub)
        cb (if (.-failed ps) (.-rcb sub) (.-lcb sub))]
    (set! (.-ready ps) (detach sub))
    (release (.-parent ps))
    (cb)))

(defn stream-ack [^Subscription sub]
  (let [^Process ps (.-target sub)]
    (when (nil? (set! (.-pending ps) (detach sub)))
      (when-not (.-owned ps)
        (set! (.-state ps) ps)
        (when (ack ps) (schedule ps))))))

(defn stream-emit [^Process ps]
  (if (.-flag ps)
    (done-all ps)
    (do (set! (.-owned ps) true)
        (schedule ps)
        (step-all ps))))

(defn signal-emit [^Process ps]
  (if (.-flag ps)
    (done-all ps)
    (step-all ps)))

(defn failed-emit [^Process ps]
  (let [^Publisher pub (.-parent ps)]
    (loop []
      (if (.-flag ps)
        (done-all ps)
        (do (try @(.-input ps) (catch Throwable _ nil))
            (if (ack ps) (recur) (release pub)))))))

(defn leave [^Context ctx idle]
  (when idle
    (loop []
      (when-some [ps (.-delayed ctx)]
        (set! (.-delayed ctx) nil)
        (loop [^Process ps ps]
          (let [^Publisher pub (.-parent ps)]
            (set! (.-cursor ctx) (.-ranks pub))
            (set! (.-reacted ctx) (ph/dmin impl nil ps))
            (acquire pub)
            (if (.-failed ps)
              (if (.-owned ps)
                (do (set! (.-owned ps) false)
                    (if (ack ps)
                      (failed-emit ps)
                      (release pub)))
                (failed-emit ps))
              (tick (.-strategy pub) ps))
            (when-some [ps (.-reacted ctx)]
              (recur ps))))
        (recur)))
    (set! (.-cursor ctx) nil)
    (set! (.-process ctx) nil)
    (u/local-set! active nil)))

(defn enter [^Context ctx]
  (if (nil? (.-cursor ctx))
    (do (set! (.-cursor ctx) ceiling)
        ;; from here until the matching `leave`, this Context travels with every
        ;; marshalling hop -- and a `cast!` made under it becomes a `call!`,
        ;; because the callee's writes to `delayed` have to come back
        (u/local-set! active ctx)
        true) false))

(defn request [^Subscription sub idle]
  (let [^Process ps (.-target sub)
        ^Publisher pub (.-parent ps)
        ^Context ctx (context)]
    (set! (.-ready sub) true)
    (attach (.-ready ps) (set! (.-ready ps) sub))
    (release pub)
    (leave ctx idle)))

(defn step [^Subscription sub idle]
  (let [^Process ps (.-target sub)
        ^Publisher pub (.-parent ps)
        ^Context ctx (context)]
    (attach (.-pending ps) (set! (.-pending ps) sub))
    (release pub)
    ((.-lcb sub))
    (leave ctx idle)))

(defn done [^Subscription sub idle]
  (let [^Process ps (.-target sub)
        ^Publisher pub (.-parent ps)
        ^Context ctx (context)]
    (release pub)
    ((.-rcb sub))
    (leave ctx idle)))

(defn success [^Subscription sub idle]
  (let [^Process ps (.-target sub)
        ^Publisher pub (.-parent ps)
        ^Context ctx (context)]
    (release pub)
    ((.-lcb sub) (.-state ps))
    (leave ctx idle)))

(defn failure [^Subscription sub idle]
  (let [^Process ps (.-target sub)
        ^Publisher pub (.-parent ps)
        ^Context ctx (context)]
    (release pub)
    ((.-rcb sub) (.-state ps))
    (leave ctx idle)))

(defn ready [^Process ps]
  (if (.-owned ps)
    (set! (.-owned ps) false)
    (when (zero? (set! (.-pressure ps) (inc (.-pressure ps))))
      (let [^Context ctx (context)
            idle (enter ctx)]
        (schedule ps)
        (leave ctx idle)))))

(defn sub [^Publisher pub lcb rcb]
  (let [^Context ctx (context)
        idle (enter ctx)]
    (acquire pub)
    (let [^Process source (.-process ctx)
          ^Process target (if-some [ps (.-current pub)]
                            ps (let [ps (->Process pub 0 true nil nil nil nil nil false false false nil)]
                                 (set! (.-child ps) ps)
                                 (set! (.-state ps) ps)
                                 (set! (.-current pub) ps)
                                 (set! (.-process ctx) ps)
                                 (set! (.-input ps)
                                   ((.-effect pub)
                                    (fn
                                      ([]
                                       (ready ps))
                                      ([x]
                                       (set! (.-state ps) x)
                                       (ready ps)))
                                    (fn
                                      ([]
                                       (set! (.-flag ps) true)
                                       (ready ps))
                                      ([x]
                                       (set! (.-flag ps) true)
                                       (set! (.-state ps) x)
                                       (ready ps)))))
                                 (set! (.-process ctx) source)
                                 (publish (.-strategy pub) ps) ps))
          sub (->Subscription source target lcb rcb nil nil false nil)]
      (if (.-failed target)
        (step sub idle)
        (subscribe (.-strategy pub) sub idle))
      sub)))

(defn unsub [^Subscription sub]
  (let [^Process ps (.-target sub)
        ^Publisher pub (.-parent ps)
        ^Context ctx (context)
        idle (enter ctx)]
    (acquire pub)
    (if (or (nil? (.-next sub)) (nil? (.-input ps))
          (not (identical? (.-current pub) ps)))
      (do (release pub)
          (leave ctx idle))
      (if (and (nil? (if (.-ready sub) (.-pending ps) (.-ready ps)))
            (identical? (.-next sub) sub))
        (do (set! (.-current pub) nil)
            ((.-input ps))
            (release pub)
            (leave ctx idle))
        (unsubscribe (.-strategy pub) sub idle)))))

(defn transfer [^Subscription sub]
  (let [^Process ps (.-target sub)
        ^Publisher pub (.-parent ps)
        ^Context ctx (context)
        idle (enter ctx)]
    (acquire pub)
    (if (nil? (.-next sub))
      (reject (.-strategy pub) sub idle)
      (do (when (.-dirty ps)
            (set! (.-dirty ps) false)
            (let [^Process p (.-process ctx)]
              (set! (.-process ctx) ps)
              (refresh (.-strategy pub) ps)
              (set! (.-process ctx) p)))
          (if (.-failed ps)
            (do (set! (.-pending ps) (detach sub))
                (if (nil? (.-input ps))
                  (done sub idle)
                  (request sub idle))
                (throw (.-state ps)))
            (accept (.-strategy pub) sub idle))))))

(defn ranks [^Context ctx]
  (if-some [^Process ps (.-process ctx)]
    (let [^Publisher pub (.-parent ps)
          r (.-ranks pub)
          n (alength r)
          a (object-array (inc n))
          i (.-node pub)]
      (set! (.-node pub) (inc i))
      (dotimes [i n] (aset a i (aget r i)))
      (aset a n i) a)
    (let [a (object-array 1)
          i @root]
      (reset! root (inc i))
      (aset a 0 i) a)))

(defn publisher [strategy arg effect]
  (->Publisher (ranks (context)) strategy arg effect 0 nil))

(def memo
  (reify Strategy
    (tick [_ ^Process ps]
      (if (.-flag ps)
        (failure-all ps)
        (success-all ps)))
    (publish [_ ^Process ps]
      (if (.-owned ps)
        (do (set! (.-owned ps) false)
            (when (ack ps) (schedule ps)))
        (set! (.-input ps) nil)))
    (subscribe [_ ^Subscription sub idle]
      (let [^Process ps (.-target sub)]
        (if (nil? (.-input ps))
          (if (.-flag ps)
            (failure sub idle)
            (success sub idle))
          (request sub idle))))
    (unsubscribe [_ ^Subscription sub idle]
      (let [^Process ps (.-target sub)
            ^Publisher pub (.-parent ps)]
        (set! (.-ready ps) (detach sub))
        (release pub)
        ((.-rcb sub) (cancelled "Memo subscription cancelled."))
        (leave (context) idle)))))

(def stream
  (reify Strategy
    (tick [_ ^Process ps]
      (let [^Publisher pub (.-parent ps)]
        (if (.-owned ps)
          (do (set! (.-owned ps) false)
              (if (nil? (.-pending ps))
                (do (set! (.-state ps) ps)
                    (if (ack ps)
                      (stream-emit ps)
                      (release pub)))
                (release pub)))
          (stream-emit ps))))
    (publish [_ ^Process ps]
      (if (.-owned ps)
        (do (set! (.-owned ps) false)
            (when (ack ps) (schedule ps)))
        (if (.-flag ps)
          (set! (.-input ps) nil)
          (do (set! (.-dirty ps) true)
              (set! (.-owned ps) true)
              (schedule ps)))))
    (refresh [_ ^Process ps]
      (let [o (.-owned ps)]
        (try (set! (.-owned ps) false)
             (set! (.-state ps) @(.-input ps))
             (set! (.-owned ps) o)
             (catch Throwable e
               (crash ps e)
               (set! (.-owned ps) o)
               (when-not o
                 (when (ack ps)
                   (schedule ps)))))))
    (subscribe [_ ^Subscription sub idle]
      (let [^Process ps (.-target sub)]
        (if (nil? (.-input ps))
          (done sub idle)
          (if (.-owned ps)
            (step sub idle)
            (request sub idle)))))
    (unsubscribe [_ ^Subscription sub idle]
      (if (.-ready sub)
        (detach-ready sub)
        (let [^Process ps (.-target sub)]
          (stream-ack sub)
          (release (.-parent ps))))
      (leave (context) idle))
    (accept [_ ^Subscription sub idle]
      (let [^Process ps (.-target sub)
            result (.-state ps)]
        (stream-ack sub)
        (if (nil? (.-input ps))
          (done sub idle)
          (request sub idle))
        result))
    (reject [_ ^Subscription sub idle]
      (done sub idle)
      (throw (cancelled "Stream subscription cancelled.")))))

(def signal
  (reify Strategy
    (tick [_ ^Process ps]
      (let [^Publisher pub (.-parent ps)]
        (if (.-owned ps)
          (do (set! (.-owned ps) false)
              (if (ack ps)
                (signal-emit ps)
                (release pub)))
          (signal-emit ps))))
    (publish [_ ^Process ps]
      (if (.-owned ps)
        (do (crash ps (ex-info "Uninitialized flow." {}))
            (schedule ps))
        (if (.-flag ps)
          (do (crash ps (ex-info "Empty flow." {}))
              (set! (.-input ps) nil))
          (set! (.-dirty ps) true))))
    (refresh [_ ^Process ps]
      (let [^Publisher pub (.-parent ps)]
        (set! (.-owned ps) true)
        (schedule ps)
        (try (let [input (.-input ps)
                   sg (.-arg pub)
                   r (loop [r @input]
                       (if (or (.-owned ps) (.-flag ps))
                         r (do (set! (.-owned ps) true)
                               (recur (sg r @input)))))]
               (set! (.-state ps)
                 (if (identical? (.-state ps) ps)
                   r (sg (.-state ps) r)))
               (let [^Subscription head (.-pending ps)]
                 (loop [^Subscription sub head]
                   (set! (.-state sub)
                     (if (identical? (.-state sub) ps)
                       r (sg (.-state sub) r)))
                   (let [sub (.-next sub)]
                     (when-not (identical? sub head)
                       (recur sub))))))
             (catch Throwable e
               (crash ps e)))))
    (subscribe [_ ^Subscription sub idle]
      (let [^Process ps (.-target sub)]
        (set! (.-state sub) (.-state ps))
        (step sub idle)))
    (unsubscribe [_ ^Subscription sub idle]
      (if (.-ready sub)
        (detach-ready sub)
        (let [^Process ps (.-target sub)]
          (set! (.-pending ps) (detach sub))
          (release (.-parent ps))))
      (leave (context) idle))
    (accept [_ ^Subscription sub idle]
      (let [^Process ps (.-target sub)
            result (.-state sub)]
        (set! (.-state sub) ps)
        (set! (.-pending ps) (detach sub))
        (if (nil? (.-input ps))
          (done sub idle)
          (request sub idle))
        result))
    (reject [_ ^Subscription sub idle]
      (done sub idle)
      (throw (cancelled "Signal subscription cancelled.")))))