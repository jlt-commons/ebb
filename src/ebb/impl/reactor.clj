;; Derived from missionary/impl/Reactor.cljs (missionary, EPL 2.0).
;; See Reactor.java for the synchronisation strategy (ADR-001 rule 1).
(ns ^:no-doc ebb.impl.reactor
  (:require [ebb.impl.util :as u :refer [cancelled]]))

(declare unsubscribe push free subscribe event)

(deftype Failer [t e]
  clojure.lang.IFn
  (invoke [_])
  clojure.lang.IDeref
  (deref [_] (t) (throw e)))

(deftype Subscription [notifier terminator ^:unsynchronized-mutable subscriber ^:unsynchronized-mutable subscribed ^:unsynchronized-mutable prev ^:unsynchronized-mutable next]
  clojure.lang.IFn
  (invoke [this] (unsubscribe this) nil)
  clojure.lang.IDeref
  (deref [this] (push this)))

(deftype Publisher [process ^:unsynchronized-mutable iterator ranks ^:unsynchronized-mutable pending ^:unsynchronized-mutable children ^:unsynchronized-mutable live ^:unsynchronized-mutable busy ^:unsynchronized-mutable done ^:unsynchronized-mutable value ^:unsynchronized-mutable prev ^:unsynchronized-mutable next ^:unsynchronized-mutable child ^:unsynchronized-mutable sibling ^:unsynchronized-mutable active ^:unsynchronized-mutable subs]
  clojure.lang.IFn
  (invoke [this] (free this) nil)
  (invoke [this n t] (subscribe this n t)))

(deftype Process [success failure ^:unsynchronized-mutable result ^:unsynchronized-mutable kill ^:unsynchronized-mutable boot ^:unsynchronized-mutable alive ^:unsynchronized-mutable active ^:unsynchronized-mutable current ^:unsynchronized-mutable reaction ^:unsynchronized-mutable schedule ^:unsynchronized-mutable subscriber ^:unsynchronized-mutable delayed ^:unsynchronized-mutable claimed ^:unsynchronized-mutable queued]
  clojure.lang.IFn
  (invoke [_] (event kill) nil))

(def stale (Object.))
(def error (Object.))

;;
;; WHO IS PROPAGATING, and WHO HAS DELAYED WORK. Reactor.java holds both in a
;; ThreadLocal<Process>; the .cljs impl uses plain globals, which is the same
;; thing when there is one thread.
;;
;; Ebb ported the globals, and that is wrong in a way that shows: `event` reads
;; `current` to decide whether it is INSIDE a propagation, and a global answers
;; yes to every fiber in the process. An event raised from a timer fiber while
;; some reactor happened to be propagating took the in-context branch and read
;; `(.-current ps)` -- which the propagation nils on its way out -- as a null
;; `.-ranks`. That was `reactor-delayed`, at 2 full-suite runs in 50.
;;
;; Per-fiber was tried next and is also wrong, deterministically: an `ap` or
;; `cp` inside a reactor runs on its OWN owner fiber (ADR-001 discipline 2), so
;; the subscription made from that fiber found no context at all
;; ("Subscription failure : not in reactor context").
;;
;; Neither unit is the ThreadLocal's. What the ThreadLocal actually delimits is
;; the DYNAMIC EXTENT of the propagating call, and ebb's stand-in for that
;; extent is `ebb.impl.server/call!` -- so these are CARRIED locals
;; (ebb.impl.util), which travel with the marshalling hop and no further. The
;; ap pulled by a propagation sees the reactor; the same ap resumed by a timer
;; does not; a second reactor on another fiber never does. That is the JVM's
;; behaviour, arrived at the long way round.
(def ^:private current (u/carried-local))
(def ^:private delayed (u/carried-local))

(defn lt [x y]
  (if (nil? x)
    true
    (if (nil? y)
      false
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
            (> xl yl)))))))

(defn link [^Publisher x ^Publisher y]
  (if (lt (.-ranks x) (.-ranks y))
    (do (set! (.-sibling y) (.-child x))
        (set! (.-child x) y) x)
    (do (set! (.-sibling x) (.-child y))
        (set! (.-child y) x) y)))

(defn dequeue [^Publisher pub]
  (let [head (.-child pub)]
    (set! (.-child pub) nil)
    (loop [heap nil
           prev nil
           head head]
      (if (nil? head)
        (if (nil? prev) heap (if (nil? heap) prev (link heap prev)))
        (let [next (.-sibling head)]
          (set! (.-sibling head) nil)
          (if (nil? prev)
            (recur heap head next)
            (let [head (link prev head)]
              (recur (if (nil? heap) head (link heap head)) nil next))))))))

(defn schedule [^Publisher pub]
  (let [ps (.-process pub)]
    (if-some [sch (.-schedule ps)]
      (set! (.-schedule ps) (link pub sch))
      (do (set! (.-schedule ps) pub)
          (set! (.-delayed ps) (u/local-get delayed))
          (u/local-set! delayed ps)))))

(defn pull [^Publisher pub]
  (let [ps (.-process pub)
        cur (.-subscriber ps)]
    (set! (.-subscriber ps) pub)
    (set! (.-value pub) error)
    (try
      (set! (.-value pub) @(.-iterator pub))
      (catch Throwable e
        (when (identical? ps (.-result ps))
          (set! (.-result ps) e)
          (let [k (.-kill ps)]
            (when (set! (.-busy k) (not (.-busy k)))
              (schedule k))))))
    (set! (.-subscriber ps) cur)))

(defn sample [^Publisher pub]
  (loop []
    (pull pub)
    (when (set! (.-busy pub) (not (.-busy pub)))
      (if (.-done pub)
        (schedule pub)
        (recur)))))

(defn touch [^Publisher pub]
  (let [ps (.-process pub)]
    (if (.-done pub)
      (let [prv (.-prev pub)]
        (set! (.-prev pub) nil)
        (if (identical? pub prv)
          (set! (.-alive ps) nil)
          (let [nxt (.-next pub)]
            (set! (.-prev nxt) prv)
            (set! (.-next prv) nxt)
            (when (identical? pub (.-alive ps))
              (set! (.-alive ps) prv)))))
      (if (identical? pub (.-active pub))
        (do (set! (.-active pub) (.-active ps))
            (set! (.-active ps) pub)
            (set! (.-pending pub) 1)
            (pull pub))
        (if (.-live pub)
          (set! (.-value pub) stale)
          (sample pub))))))

(defn ack [^Publisher pub]
  (when (zero? (set! (.-pending pub) (dec (.-pending pub))))
    (set! (.-value pub) nil)
    (when (set! (.-busy pub) (not (.-busy pub)))
      (schedule pub))))

(defn propagate [^Publisher pub]
  (let [ps (.-process pub)]
    (u/local-set! current ps)
    (loop [pub pub]
      (set! (.-reaction ps) (dequeue pub))
      (set! (.-current ps) pub)
      (touch pub)
      (when-some [t (.-subs pub)]
        (set! (.-subs pub) nil)
        (loop [s t]
          (let [s (.-next s)]
            (set! (.-prev s) nil)
            (when-not (identical? s t)
              (recur s))))
        (loop [n (.-next t)]
          (let [s n
                n (.-next s)]
            (set! (.-next s) nil)
            (when (pos? (.-pending pub))
              (set! (.-pending pub) (inc (.-pending pub))))
            (set! (.-subscriber ps) (.-subscriber s))
            ((if (nil? (.-prev pub))
               (.-terminator s)
               (.-notifier s)))
            (set! (.-subscriber ps) nil)
            (when-not (identical? s t)
              (recur n)))))
      (when-some [r (.-reaction ps)]
        (recur r)))
    (set! (.-current ps) nil)
    (u/local-set! current nil)
    (loop []
      (when-some [pub (.-active ps)]
        (set! (.-active ps) (.-active pub))
        (set! (.-active pub) (when-not (identical? error (.-value pub)) pub))
        (ack pub)
        (recur)))
    (when (nil? (.-alive ps))
      (when-some [pub (.-boot ps)]
        (set! (.-boot ps) nil)
        (if (identical? (.-result ps) ps)
          (do (set! (.-result ps) (.-value pub))
              (.-success ps)) (.-failure ps))))))

(defn hook [^Subscription s]
  (let [pub (.-subscribed s)]
    (if (nil? (.-prev pub))
      ((.-terminator s))
      (let [p (.-subs pub)]
        (set! (.-subs pub) s)
        (if (nil? p)
          (->> s
            (set! (.-prev s))
            (set! (.-next s)))
          (let [n (.-next p)]
            (set! (.-next p) s)
            (set! (.-prev n) s)
            (set! (.-prev s) p)
            (set! (.-next s) n)))))))

(defn cancel [^Publisher pub]
  (when (.-live pub)
    (set! (.-live pub) false)
    (let [ps (.-process pub)
          cur (.-subscriber ps)]
      (set! (.-subscriber ps) pub)
      ((.-iterator pub))
      (set! (.-subscriber ps) cur)
      (when (identical? stale (.-value pub))
        (sample pub)))))

(defn failer [n t e]
  (n) (->Failer t e))

(defn free [^Publisher pub]
  (when-not (identical? (.-process pub) (u/local-get current))
    (throw (ex-info "Cancellation failure : not in reactor context." {})))
  (cancel pub))

(defn subscribe [^Publisher pub n t]
  (let [ps (.-process pub)
        sub (.-subscriber ps)]
    (if-not (identical? ps (u/local-get current))
      (failer n t (ex-info "Subscription failure : not in reactor context." {}))
      (if (identical? sub (.-boot ps))
        (failer n t (ex-info "Subscription failure : not a subscriber." {}))
        (let [s (->Subscription n t sub pub nil nil)]
          (if (identical? (.-active pub) pub)
            (hook s)
            (do (when (pos? (.-pending pub))
                  (set! (.-pending pub) (inc (.-pending pub))))
                (n))) s)))))

(defn unsubscribe [^Subscription s]
  (let [sub (.-subscriber s)
        ps (.-process sub)]
    (when-not (identical? ps (u/local-get current))
      (throw (ex-info "Unsubscription failure : not in reactor context." {})))
    (when-some [pub (.-subscribed s)]
      (set! (.-subscribed s) nil)
      (if-some [p (.-prev s)]
        (let [n (.-next s)]
          (set! (.-prev s) nil)
          (set! (.-next s) nil)
          (if (identical? p s)
            (set! (.-subs pub) nil)
            (do (set! (.-prev n) p)
                (set! (.-next p) n)
                (when (identical? s (.-subs pub))
                  (set! (.-subs pub) p))))
          (let [cur (.-subscriber ps)]
            (set! (.-subscriber ps) sub)
            ((.-notifier s))
            (set! (.-subscriber ps) cur)))
        (when (pos? (.-pending pub)) (ack pub))))))

(defn push [^Subscription s]
  (let [sub (.-subscriber s)
        ps (.-process sub)]
    (when-not (identical? ps (u/local-get current))
      (throw (ex-info "Transfer failure : not in reactor context." {})))
    ;; NOT named `value`. A local sharing a name with a mutable deftype field
    ;; reads the FIELD, not the binding, so the snapshot below came back
    ;; mutated: `ack` sets (.-value pub) to nil once pending reaches zero, and
    ;; this transfer then returned nil instead of the value it had just read.
    ;; Only the last acking subscriber hits it, which is why it surfaced as an
    ;; intermittent reactor failure. See doc/conformance.md.
    (let [v-val (if-some [pub (.-subscribed s)]
                  (let [v-cur (.-value pub)]
                    (if (pos? (.-pending pub))
                      (do (ack pub) v-cur)
                      (if (identical? v-cur stale)
                        (do (sample pub) (.-value pub))
                        v-cur))) error)
          cur   (.-subscriber ps)]
      (set! (.-subscriber ps) sub)
      (if (identical? v-val error)
        (do ((.-terminator s))
            (set! (.-subscriber ps) cur)
            (throw (cancelled "Subscription cancelled.")))
        (do (hook s)
            (set! (.-subscriber ps) cur)
            v-val)))))

(defn- propagate-claimed!
  "Propagate pub, guaranteeing that at most one fiber is inside `propagate` for
  a given process at a time.

  Reactor.java gets this from `synchronized (pub.process)` held ACROSS the
  propagation. Ebb cannot hold a lock there: propagate invokes notifiers, ap
  and cp notifiers park (ebb-8nq.29), and a fiber may not park while its
  carrier holds a counted lock -- ADR-001 rule 5. A straight port of the
  monitor hangs the suite, which was measured.

  So this follows jolt's own rule for the situation (host/chez/locks.ss):
  COMMIT UNDER THE LOCK AND SWITCH OUTSIDE IT. Under the process monitor we
  decide who propagates; the propagation itself runs with no lock held; then we
  retake the monitor to hand the claim on. A fiber that loses leaves its
  publisher on the queue and returns, so no notification is dropped and no
  caller waits.

  `flip?` is the subtle part. `busy` is a per-PUBLISHER toggle and flipping it
  to true is how a caller claims the notification. A publisher taken off the
  queue has ALREADY been flipped by whoever queued it, so flipping again would
  hand the notification back and lose it."
  [^Publisher pub flip?]
  (let [^Process ps (.-process pub)]
    (loop [pub pub, flip? flip?]
      (let [go? (locking ps
                  (if (and flip? (not (set! (.-busy pub) (not (.-busy pub)))))
                    false                       ; lost the flip: the other side owns it
                    (if (.-claimed ps)
                      (do (set! (.-queued ps) (conj (.-queued ps) pub)) false)
                      (do (set! (.-claimed ps) true) true))))]
        (when go?
          ;; The release MUST be in a finally. Without it a throw out of
          ;; propagate strands the claim set forever, every later event for
          ;; this process queues behind it and the reactor wedges -- which is
          ;; what the first version of this did.
          ;; The callback runs with the claim RELEASED, as Reactor.java invokes
          ;; it outside the monitor. Holding it across the callback deadlocks
          ;; the completion path: cb settles the reactor task, whose consumer
          ;; can re-enter and need the claim we are sitting on.
          (let [cb (try (propagate pub)
                        (finally (locking ps (set! (.-claimed ps) false))))]
            (when cb (cb (.-result ps))))
          ;; Pick up anything queued while we ran. Re-entering the loop rather
          ;; than propagating directly is deliberate: another fiber may have
          ;; taken the claim in between, and then the loop simply queues this
          ;; one back for them.
          (when-some [nxt (locking ps
                            (when-some [q (seq (.-queued ps))]
                              (set! (.-queued ps) (vec (rest q)))
                              (first q)))]
            (recur nxt false)))))))

(defn event [^Publisher pub]
  (if-some [ps (u/local-get current)]
    (when (set! (.-busy pub) (not (.-busy pub)))
      (if (identical? ps (.-process pub))
        (if (lt (.-ranks (.-current ps)) (.-ranks pub))
          (set! (.-reaction ps)
            (if-some [r (.-reaction ps)]
              (link pub r) pub))
          (schedule pub))
        (schedule pub)))
    (do (propagate-claimed! pub true)
        (loop []
          (when-some [ps (u/local-get delayed)]
            (u/local-set! delayed (.-delayed ps))
            (set! (.-delayed ps) nil)
            (let [pub (.-schedule ps)]
              (set! (.-schedule ps) nil)
              ;; already flipped by `schedule`, so do not flip again
              (propagate-claimed! pub false)
              (recur)))))))

(def kill
  (reify
    clojure.lang.IDeref
    (deref [_]
      (let [ps (u/local-get current)]
        (when-some [t (.-alive ps)]
          (loop [pub (.-next t)]
            (cancel pub)
            (when-some [t (.-alive ps)]
              (let [pub (loop [pub pub]
                          (let [pub (.-next pub)]
                            (if (nil? (.-prev pub))
                              (recur pub) pub)))]
                (when-not (identical? pub (.-next t))
                  (recur pub)))))) true))))

(def zero (object-array 0))

(defn run [init s f]
  (let [ps (->Process s f nil nil nil nil nil nil nil nil nil nil false [])
        k (->Publisher ps kill nil 0 0 false false false false nil nil nil nil nil nil)
        b (->Publisher ps (reify clojure.lang.IDeref (deref [_] (init))) zero 0 0 false false false nil nil nil nil nil nil nil)]
    (set! (.-kill ps) k)
    (set! (.-boot ps) b)
    (set! (.-result ps) ps)
    (event b) ps))

(defn publish [flow continuous]
  (let [ps (doto (u/local-get current) (-> nil? (when (throw (ex-info "Publication failure : not in reactor context." {})))))
        cur (.-subscriber ps)
        pub (->Publisher
              ps nil
              (let [n (alength (.-ranks cur))
                    a (object-array (inc n))]
                (dotimes [i n] (aset a i (aget (.-ranks cur) i)))
                (doto a (aset n (doto (.-children cur) (->> (inc) (set! (.-children cur)))))))
              0 0 true true false nil nil nil nil nil nil nil)]
    (when-not continuous (set! (.-active pub) pub))
    (if-some [p (.-alive ps)]
      (let [n (.-next p)]
        (set! (.-next p) pub)
        (set! (.-prev n) pub)
        (set! (.-prev pub) p)
        (set! (.-next pub) n))
      (->> pub
        (set! (.-prev pub))
        (set! (.-next pub))))
    (set! (.-alive ps) pub)
    (set! (.-subscriber ps) pub)
    (set! (.-iterator pub)
      (flow #(event pub)
        #(do (set! (.-done pub) true)
             (event pub))))
    (set! (.-subscriber ps) cur)
    (when (.-value (.-kill ps)) (cancel pub))
    (if (set! (.-busy pub) (not (.-busy pub)))
      (touch pub)
      (when continuous
        (cancel pub)
        (throw (ex-info "Publication failure : undefined continuous flow." {}))))
    pub))