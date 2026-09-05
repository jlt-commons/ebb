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

(deftype Failed [e])

(deftype Process [^:unsynchronized-mutable combinator
                  ^:unsynchronized-mutable notifier
                  terminator args owed busy gates inputs state
                  ^:unsynchronized-mutable done
                  alive]
  clojure.lang.IFn
  (invoke [_] (dotimes [i (alength inputs)] ((aget inputs i))) nil)
  clojure.lang.IDeref
  (deref [p] (transfer p)))

(defn- take-slot!
  "Take slot i's deref rights. Answers [n nil] when we now hold it and it owes n
  DISCARDS, or [nil gate] when somebody else holds it -- gate opens when they
  are done. The gate is chosen under the same lock as the claim it lost, so it
  can never be one the holder has already opened."
  [^Process p i]
  (locking p
    (let [busy (.-busy p)]
      (if (zero? (aget busy i))
        (let [owed (.-owed p) n (aget owed i)]
          (aset busy i 1)
          (aset owed i 0)
          [n nil])
        (let [gates (.-gates p)]
          [nil (or (aget gates i) (aset gates i (promise)))])))))

(defn- release-slot!
  "Give slot i back. Answers the discards that came due while we held it --
  nonzero means keep going, and the claim stays ours."
  [^Process p i]
  (locking p
    (let [owed (.-owed p) n (aget owed i)]
      (if (pos? n)
        (do (aset owed i 0) n)
        (let [gates (.-gates p) g (aget gates i)]
          (aset (.-busy p) i 0)
          (when g (aset gates i nil) (deliver g true))
          0)))))

(defn- discard!
  "Pull and drop n values from input i, then whatever else came due. The caller
  holds the slot. Swallows, as the Java's Util.discard does."
  [^Process p i n]
  (let [input (aget (.-inputs p) i)]
    (loop [n n]
      (dotimes [_ n] (try @input (catch Throwable _ nil)))
      (let [m (release-slot! p i)]
        (when (pos? m) (recur m))))))

(defn dirty
  "Input i notified. One notification is left for the combinator to sample; any
  FURTHER one before that happens is a discard the input is owed, and draining
  it eagerly is what keeps a sampled input flowing between samples -- defer it
  and `(m/sample f (m/reductions ...) sampler)` stalls, which core_test's
  sample-async catches.

  What this must not do is deref alongside a transfer of the same slot. It used
  to: `mark!` answered `already dirty, go drain` WITHOUT clearing the flag, so
  `resample!` could take the still-dirty slot and deref concurrently -- 2630
  overlaps in 20 runs once the window was widened enough to see (ebb-8nq.33).
  The slot is claimed now, and a `dirty` that loses simply leaves the count
  behind for whoever holds it. It never waits: the holder's deref never depends
  on us."
  [^Process p i]
  (let [args (.-args p)
        owed (.-owed p)
        surplus? (locking p
                   (if (identical? (aget args i) args)
                     (do (aset owed i (inc (aget owed i))) true)
                     (do (aset args i args) false)))]
    (when surplus?
      (let [[n _] (take-slot! p i)]
        (when n (discard! p i n)))))
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
              ;; cancel first, then take every transfer it owes us -- both
              ;; outside the lock, both able to park
              ((aget inputs i))
              (dirty ps i))
            (when (zero? (swap! (.-alive ps) dec)) (.-terminator ps)))
        (if (identical? (aget args sampled) args)
          (do (try @(aget inputs sampled) (catch Throwable _ nil))
              (when (arb/release! (.-state ps)) (recur)))
          (.-notifier ps))))))

(defn ready [^Process ps]
  (when (arb/claim! (.-state ps)) (rounds ps)))

(defn- resample!
  "The current value of slot i. Unlike `dirty` this one NEEDS the answer, so
  when it loses the claim it waits for the holder to finish rather than
  skipping. That wait cannot deadlock: the holder is either a `dirty` drain,
  which never waits for anything, or this same transfer, which releases before
  it moves on. A plain mutex would not do -- a deref that notifies its own slot
  arrives on the input's owner fiber for an ap, so it is not the same executor
  and could not be let back in."
  [^Process ps i]
  (let [args  (.-args ps)
        input (aget (.-inputs ps) i)]
    (loop []
      (let [[n gate] (take-slot! ps i)]
        (if (nil? n)
          (do (deref gate 1000 nil) (recur))
          (try
            ;; the discards this slot is owed come first, and are dropped
            (dotimes [_ n] (try @input (catch Throwable _ nil)))
            ;; then the notification the combinator is actually here for
            (loop []
              ;; ONE region for both questions. Asking `is it dirty?` and
              ;; reading the cached value in two regions leaves a window for a
              ;; `dirty` in between, and the read then answers with the SENTINEL
              ;; -- the args array itself -- which goes straight into the
              ;; combinator's arguments.
              (let [r (locking ps
                        (if (identical? (aget args i) args)
                          (do (aset args i nil) nil)
                          [(aget args i)]))]
                (if (nil? r)
                  (let [x @input]
                    ;; the deref may have notified this slot again -- the Java's
                    ;; do/while -- in which case that notification is ours too
                    (if (locking ps (if (identical? (aget args i) args)
                                      true
                                      (do (aset args i x) false)))
                      (recur)
                      x))
                  (nth r 0))))
            (finally
              ;; hand the slot back however we leave, exception included, or a
              ;; failed transfer strands every later notification behind a claim
              ;; nobody holds
              (loop []
                (let [m (release-slot! ps i)]
                  (when (pos? m)
                    (dotimes [_ m] (try @input (catch Throwable _ nil)))
                    (recur)))))))))))

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
        owed (int-array arity)
        busy (int-array arity)
        gates (object-array arity)
        inputs (object-array arity)
        ;; the claim starts held: Sample.java holds the monitor across the whole
        ;; of this construction, and the sampler's notifier must not run a round
        ;; against an `inputs` array that is not filled in yet.
        ps (->Process c n t args owed busy gates inputs (arb/arbiter) false (atom arity))
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
