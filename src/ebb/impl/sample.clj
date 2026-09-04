;; Derived from missionary (EPL 2.0):
;;   logic           <- missionary/impl/Sample.cljs
;;   synchronisation <- missionary/impl/Sample.java  (the process monitor)
;;
;; ADR-001 rule 1 via `ebb.impl.arbiter`; see that header for the protocol.
;;
;; Sample has TWO entry points, not one. `ready` -- the sampler's notifier and
;; terminator -- runs the pull loop and breaks out holding the claim, so
;; `transfer` never races another round. But every sampled input also has its
;; own notifier, `dirty`, which is outside the loop entirely and marks a slot in
;; `args`. `transfer` reads and clears those same slots, and `(set! alive ...)`
;; is a read-modify-write shared with each input's terminator.
;;
;; So the claim protocol covers the loop and `locking` covers the slots, held
;; for the marking only -- both the discard in `dirty` and the resample in
;; `transfer` deref an input, and for an ap or cp input that parks (rule 5).
;;
;; Slot protocol, unchanged from the Java:
;;   args[i] identical to args   notified, not yet sampled
;;   args[i] nil                 a deref is in flight (or the input never spoke)
;;   anything else               the value last sampled
;;
;; Two places diverge from the monitor, both forced by keeping derefs outside
;; the lock, and both recorded in doc/conformance.md:
;;
;; * The combinator is applied to a SNAPSHOT taken slot by slot as each one
;;   settles, not to `args` itself. Under the monitor no `dirty` can land during
;;   a transfer at all; here one can, and it would otherwise put the sentinel
;;   into the combinator's argument list. The snapshot keeps the value that was
;;   current when the slot was read; the new notification is the next transfer's.
;; * `dirty`'s discard -- the drain owed when an input notifies twice before the
;;   combinator gets to it -- is not mutually exclusive with `transfer`'s deref
;;   of the same slot. The monitor makes those two orderly; one atom cannot,
;;   because the slot's claim would have to be held across a park (ebb-8nq.33).
(ns ^:no-doc ebb.impl.sample
  (:require [ebb.impl.arbiter :as arb]))

(declare transfer)

(deftype Process [^:unsynchronized-mutable combinator
                  ^:unsynchronized-mutable notifier
                  terminator args inputs state
                  ^:unsynchronized-mutable done
                  alive]
  clojure.lang.IFn
  (invoke [_] (dotimes [i (alength inputs)] ((aget inputs i))) nil)
  clojure.lang.IDeref
  (deref [p] (transfer p)))

(defn- mark!
  "Record a notification on slot i. True when the slot was ALREADY dirty, in
  which case the caller owes the input a drain -- outside the lock."
  [^Process p i]
  (let [args (.-args p)]
    (locking p
      (if (identical? (aget args i) args)
        true
        (do (aset args i args) false)))))

(defn- drain!
  "Pull and drop one value from input i. The input may not exist yet -- a
  cross-executor second notification landing while `run` is still building the
  array -- and then the slot simply stays dirty for the next transfer."
  [^Process p i]
  (when-some [it (aget (.-inputs p) i)]
    (try @it (catch Throwable _ nil)))
  nil)

(defn dirty [^Process p i]
  (when (mark! p i) (drain! p i))
  nil)

(defn- rounds
  "Run rounds while the claim is held; answer the callback to fire outside, or
  nil. Both exits keep the claim, as the Java's two `break`s do."
  [^Process ps]
  (let [args    (.-args ps)
        inputs  (.-inputs ps)
        sampled (dec (alength inputs))]
    (loop []
      (if (.-done ps)
        (do (dotimes [i sampled]
              ;; cancel first, then take the transfer it owes us -- both outside
              ;; the lock, both able to park.
              ((aget inputs i))
              (when (mark! ps i) (drain! ps i)))
            (when (zero? (swap! (.-alive ps) dec)) (.-terminator ps)))
        (if (identical? (aget args sampled) args)
          (do (try @(aget inputs sampled) (catch Throwable _ nil))
              (when (arb/release! (.-state ps)) (recur)))
          (.-notifier ps))))))

(defn ready [^Process ps]
  (when (arb/claim! (.-state ps)) (rounds ps)))

(defn- resample!
  "The current value of slot i, deref'ing the input if it has notified. Loops
  while notifications keep landing during the deref, as the Java's do/while
  does."
  [^Process ps i]
  (let [args  (.-args ps)
        input (aget (.-inputs ps) i)]
    (loop []
      ;; ONE region for both questions. Asking "is it dirty?" and reading the
      ;; cached value in two regions leaves a window for a `dirty` in between,
      ;; and the read then answers with the SENTINEL -- the args array itself --
      ;; which goes straight into the combinator's arguments. Caught by
      ;; stress_test's sample case, about one run in a hundred and only under
      ;; load. The reply is wrapped so that a cached nil is still distinguishable
      ;; from "you have the slot".
      (let [r (locking ps
                (if (identical? (aget args i) args)
                  (do (aset args i nil) nil)
                  [(aget args i)]))]
        (if (nil? r)
          (let [x @input]
            (if (locking ps (if (identical? (aget args i) args)
                              true
                              (do (aset args i x) false)))
              (recur)
              x))
          (nth r 0))))))

(defn transfer [^Process ps]
  (let [c       (.-combinator ps)
        args    (.-args ps)
        inputs  (.-inputs ps)
        sampled (dec (alength inputs))
        sampler (aget inputs sampled)
        snap    (object-array (alength inputs))
        x (try
            (try
              (when (nil? c) (throw (ex-info "Undefined continuous flow." {:type :ebb.impl.util/undefined})))
              (dotimes [i sampled] (aset snap i (resample! ps i)))
              (catch Throwable e
                (try @sampler (catch Throwable _ nil))
                (throw e)))
            (aset args sampled (aset snap sampled @sampler))
            (apply c (vec snap))
            (catch Throwable e
              (set! (.-notifier ps) nil)
              (sampler)
              (aset args sampled args) e))]
    ;; the notification this transfer answers came from a `break`, so the claim
    ;; is ours to give back.
    (when-some [cb (when (arb/release! (.-state ps)) (rounds ps))] (cb))
    (if (nil? (.-notifier ps))
      (throw x) x)))

(defn run [c f fs n t]
  (let [it (.iterator (seq fs))
        arity (inc (count fs))
        args (object-array arity)
        inputs (object-array arity)
        ;; the claim starts held: Sample.java holds the monitor across the whole
        ;; of this construction, and the sampler's notifier must not run a round
        ;; against an `inputs` array that is not filled in yet.
        ps (->Process c n t args inputs (arb/arbiter) false (atom arity))
        done #(when (zero? (swap! (.-alive ps) dec))
                ((.-terminator ps)))]
    (loop [index 0 flow f]
      (if (.hasNext it)
        (do (aset inputs index (flow #(dirty ps index) done))
            (when (nil? (aget args index)) (set! (.-combinator ps) nil))
            (recur (inc index) (.next it)))
        (aset inputs index
          (flow #(when-some [cb (ready ps)] (cb))
            #(do (set! (.-done ps) true)
                 (when-some [cb (ready ps)]
                   (cb)))))))
    (when-some [cb (when (arb/release! (.-state ps)) (rounds ps))] (cb))
    ps))
