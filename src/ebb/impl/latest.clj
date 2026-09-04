;; Derived from missionary (EPL 2.0):
;;   logic           <- missionary/impl/Latest.cljs
;;   synchronisation <- missionary/impl/Latest.java  (a CAS'd volatile word)
;;
;; ADR-001 rule 1. Latest has no lock and no `busy` toggle -- it has `sync`, a
;; three-state word saying who owns the process:
;;
;;   nil     somebody is running it
;;   idle    nobody is; a notifier may take ownership
;;   an int  the head of a stack of input indices whose notifications are
;;           queued, chained through `siblings`
;;
;; plus `owner`, naming the executor currently inside `transfer` so a notifier
;; running there can fold its event into the pass already in flight.
;;
;; Latest.java drives `sync` with `SYNC.compareAndSet` throughout. The header
;; this file used to carry said the cljs logic "is already serialised by its own
;; owner/sync protocol, which is what ebb keeps" -- it kept the state machine
;; and dropped the CAS, which IS the protocol. A plain read then `set!` lets two
;; notifiers both find `idle` and both take ownership, or lets one overwrite the
;; other's `siblings` link so that notification is never consumed. Either way
;; the process goes idle with an input still notified, and downstream waits
;; forever. Measured at 15 runs in 40 of two `m/watch`es advanced from two
;; threads into `(m/latest vector ...)`.
;;
;; Same defect as ebb-8nq.35 found in zip, and missed by ebb-8nq.32's audit for
;; the same reason: that pass went looking for a `busy` field, and this file
;; counts with `sync`.
;;
;; `owner` was a BOOLEAN -- "is somebody in transfer" rather than the Java's
;; "am I". Under ADR-001 that is not a simplification, it is the wrong
;; question: a notification raised by our own deref of an ap or cp input
;; arrives on THAT input's owner fiber, and one raised by an unrelated `watch`
;; arrives on whichever thread swapped the reference. A boolean cannot tell
;; them apart, so the second was taking the inline branch and mutating the heap
;; underneath a transfer. It holds an executor identity now
;; (`ebb.impl.util/context-key` -- the fiber, or the thread off one), compared
;; against the caller's own, which answers correctly however the read is
;; scheduled. The branch that still matters is construction: `run` claims
;; ownership so that an input notifying synchronously during `spawn` -- which
;; is what a continuous flow does -- is counted rather than queued, and `spawn`
;; reads `pending` to decide whether the input initialised at all.
(ns ^:no-doc ebb.impl.latest
  (:require [ebb.impl.pairing-heap :as ph]
            [ebb.impl.util :as u]))

(declare cancel transfer)

(deftype Process [^:unsynchronized-mutable step
                  done combinator
                  ^:unsynchronized-mutable value
                  owner                              ; atom: executor in transfer, or nil
                  args inputs children siblings
                  ^:unsynchronized-mutable pending
                  ^:unsynchronized-mutable head
                  sync]                              ; atom: nil | idle | input index
  clojure.lang.IFn (invoke [ps] (cancel ps))
  clojure.lang.IDeref (deref [ps] (transfer ps)))

(def impl
  (ph/impl
    (fn [_ x y] (< x y))
    (fn [^Process ps i] (aget (.-children ps) i))
    (fn [^Process ps i x] (aset (.-children ps) i x))
    (fn [^Process ps i] (aget (.-siblings ps) i))
    (fn [^Process ps i x] (aset (.-siblings ps) i x))))

(def idle (Object.))

(defn terminated [^Process ps i]
  (identical? idle (aget (.-children ps) i)))

(defn event [^Process ps i]
  (set! (.-pending ps) (dec (.-pending ps)))
  (aset (.-siblings ps) i nil)
  (when-not (terminated ps i)
    (let [h (.-head ps)]
      (set! (.-head ps)
        (if (nil? h)
          i (ph/meld impl ps h i)))
      nil)))

(defn consume [^Process ps i]
  (loop [i i]
    (let [s (aget (.-siblings ps) i)]
      (event ps i)
      (when (some? s)
        (recur s)))))

(defn dequeue [^Process ps]
  (set! (.-pending ps) (inc (.-pending ps)))
  (let [h (.-head ps)]
    (set! (.-head ps) (ph/dmin impl ps h))
    (aset (.-siblings ps) h idle) h))

(defn cancel [^Process ps]
  (let [inputs (.-inputs ps)]
    (dotimes [i (alength inputs)]
      ((aget inputs i)))))

(defn- take-sync!
  "Take the queued stack of input indices, spinning until the CAS lands -- the
  Java's inner `for(;;)`. Only reached when `sync` is known to hold an index:
  `idle` is written by the round's owner alone, and an index only by a notifier
  that found `sync` not idle."
  [^Process ps]
  (loop []
    (let [s @(.-sync ps)]
      (if (compare-and-set! (.-sync ps) s nil) s (recur)))))

(defn ready [^Process ps]
  (let [step (.-step ps)
        done (.-done ps)]
    (loop []
      (if (nil? (.-head ps))
        (if (zero? (.-pending ps))
          (done)
          ;; hand ownership back, or take whatever queued behind us while we
          ;; were trying to and go round again
          (when-not (compare-and-set! (.-sync ps) nil idle)
            (consume ps (take-sync! ps))
            (recur)))
        (if (nil? step)
          (do (reset! (.-owner ps) (u/context-key))
              (try @(aget (.-inputs ps) (dequeue ps))
                   (catch Throwable _ nil))
              (reset! (.-owner ps) nil)
              (recur))
          (step))))))

(defn transfer [^Process ps]
  (when (nil? (.-step ps))
    (ready ps)
    (throw (ex-info "Uninitialized continuous flow." {:type :ebb.impl.util/undefined})))
  (reset! (.-owner ps) (u/context-key))
  (try
    (when (pos? (.-pending ps))
      (when (some? @(.-sync ps))
        (consume ps (take-sync! ps))))
    (let [args (.-args ps)
          inputs (.-inputs ps)]
      (loop [x (.-value ps)]
        (if (nil? (.-head ps))
          (if (identical? x ps)
            (set! (.-value ps) (apply (.-combinator ps) (vec args))) x)
          (let [i (dequeue ps)
                prev (aget args i)
                curr (aset args i @(aget inputs i))]
            (recur (if (identical? x ps) x (if (= prev curr) x ps)))))))
    (catch Throwable e
      (set! (.-step ps) nil)
      (cancel ps)
      (throw e))
    (finally
      (reset! (.-owner ps) nil)
      (ready ps))))

(defn step [^Process ps i]
  (if (identical? (u/context-key) @(.-owner ps))
    ;; we ARE the pass that provoked this: fold it in rather than queue it
    (event ps i)
    (loop []
      (let [s @(.-sync ps)]
        ;; losing either CAS means somebody moved `sync` under us, so re-read
        ;; and decide again -- the Java's `for(;;)`
        (if (identical? s idle)
          (if (compare-and-set! (.-sync ps) idle nil)
            (do (event ps i) (ready ps))
            (recur))
          (do (aset (.-siblings ps) i s)
              (when-not (compare-and-set! (.-sync ps) s i) (recur))))))))

(defn spawn [^Process ps i flow]
  (let [p (.-pending ps)]
    (aset (.-args ps) i ps)
    (aset (.-siblings ps) i idle)
    (aset (.-inputs ps) i
      (flow #(step ps i)
        #(do (aset (.-children ps) i idle)
             (step ps i))))
    (or (= p (.-pending ps)) (terminated ps i))))

(defn run [c fs s d]
  (let [it (.iterator (seq fs))
        arity (count fs)
        ps (->Process s d c nil (atom (u/context-key))
             (object-array arity)
             (object-array arity)
             (object-array arity)
             (object-array arity)
             arity nil (atom nil))]
    (set! (.-value ps) ps)
    (loop [i 0
           initialized 0]
      (if (< i arity)
        (recur (inc i)
          (if (spawn ps i (.next it))
            initialized (inc initialized)))
        (if (= arity initialized)
          (set! (.-step ps) s)
          (cancel ps))))
    (reset! (.-owner ps) nil)
    (s) ps))