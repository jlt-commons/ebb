;; Derived from missionary (EPL 2.0):
;;   logic           <- missionary/impl/Zip.cljs
;;   synchronisation <- missionary/impl/Zip.java  (a ReentrantLock on `pending`)
;;
;; ADR-001 rule 1. `pending` counts how many inputs still owe a notification
;; before the zip can transfer: `run` sets it to the arity, every notification
;; decrements, and whoever brings it to ZERO owns the round. Zip.java holds a
;; ReentrantLock across every one of those updates and across both deref loops.
;;
;; The header this file used to carry said Zip.java takes no lock and that the
;; counter serialises everything by itself. It was wrong on both counts, and
;; that is why the file was not on ebb-8nq.32's list -- that audit went looking
;; for a `busy` field and this one counts. `(set! (.-pending ps) (dec (.-pending
;; ps)))` is a read and a write, so two inputs notifying at once lose a
;; decrement, `pending` never reaches zero, nobody is ever handed the round, and
;; the zip goes idle with both inputs notified. Measured at 33 runs in 200 of
;;
;;   (m/zip vector (m/ap (m/? (m/sleep 2 1))) (m/ap (m/? (m/sleep 1 2))))
;;
;; and 0 in 200 when both inputs are seeds, which is the tell: it takes two
;; inputs whose notifications come from different executors.
;;
;; The lock itself is not portable -- both loops deref an input, and for an ap
;; or cp input that parks, which is what ADR-001 rule 5 forbids. An atomic
;; counter is, and it is faithful. Addition commutes, so the order the updates
;; land in cannot change the total; and the intermediate values cannot hand the
;; round to two owners either, because inside a deref loop `pending` only ever
;; DECREASES -- a deref can make its own input notify again, and nothing adds
;; until the loop is over -- so it cannot pass through zero a second time. That
;; is the same argument Eduction.java's atomic PRESSURE counter runs on, and
;; ebb.impl.eduction ports that one directly.
(ns ^:no-doc ebb.impl.zip)

(declare cancel transfer ready)

(deftype Process [combine
                  ^:unsynchronized-mutable step
                  done
                  inputs
                  pending]
  clojure.lang.IFn
  (invoke [z] (cancel z))
  clojure.lang.IDeref
  (deref [z] (transfer z)))

(defn- ready [^Process ps]
  (if-some [s (.-step ps)]
    (s)
    (let [inputs (.-inputs ps)
          arity  (alength inputs)]
      (loop [i 0 c 0]
        (if (< i arity)
          (let [input (aget inputs i)]
            (if (identical? input ps)
              (recur (inc i) c)
              (do (try @input (catch Throwable _ nil))
                  (recur (inc i) (inc c)))))
          (let [p (swap! (.-pending ps) + c)]
            (if (zero? c) ((.-done ps)) (when (zero? p) (ready ps)))))))))

(defn- cancel [^Process ps]
  (let [inputs (.-inputs ps)]
    (dotimes [i (alength inputs)]
      (let [input (aget inputs i)]
        (when-not (identical? input ps) (input)))))
  nil)

(defn- transfer [^Process ps]
  (let [c      (volatile! 0)
        inputs (.-inputs ps)
        arity  (alength inputs)
        buffer (object-array arity)]
    (try (dotimes [i arity]
           (vswap! c inc)
           (aset buffer i @(aget inputs i)))
         (apply (.-combine ps) (vec buffer))
         (catch Throwable e
           (set! (.-step ps) nil)
           (throw e))
         (finally
           (let [p (swap! (.-pending ps) + @c)]
             (when (nil? (.-step ps)) (cancel ps))
             (when (zero? p) (ready ps)))))))

(defn run [f fs s d]
  (let [arity  (count fs)
        inputs (object-array arity)
        ps     (->Process f s d inputs (atom 0))]
    (loop [i 0 fs (seq fs)]
      (when fs
        (let [input ((first fs)
                     (fn [] (let [p (swap! (.-pending ps) dec)]
                              (when (zero? p) (ready ps))))
                     (fn [] (aset (.-inputs ps) i ps)
                       (set! (.-step ps) nil)
                       (let [p (swap! (.-pending ps) dec)]
                         (when-not (neg? p) (cancel ps))
                         (when (zero? p) (ready ps)))))]
          (when (nil? (aget inputs i)) (aset inputs i input)))
        (recur (inc i) (next fs))))
    (let [p (swap! (.-pending ps) + arity)]
      (when (nil? (.-step ps)) (cancel ps))
      (when (zero? p) (ready ps))
      ps)))
