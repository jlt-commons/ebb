;; Derived from missionary/impl/Zip.cljs (missionary, EPL 2.0).
;; Zip.java takes no lock: the pending counter serialises everything, and a
;; transfer only runs when it reaches zero.
(ns ^:no-doc ebb.impl.zip)

(declare cancel transfer ready)

(deftype Process [combine
                  ^:unsynchronized-mutable step
                  done
                  inputs
                  ^:unsynchronized-mutable pending]
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
          (let [p (set! (.-pending ps) (+ (.-pending ps) c))]
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
           (let [p (set! (.-pending ps) (+ (.-pending ps) @c))]
             (when (nil? (.-step ps)) (cancel ps))
             (when (zero? p) (ready ps)))))))

(defn run [f fs s d]
  (let [arity  (count fs)
        inputs (object-array arity)
        ps     (->Process f s d inputs 0)]
    (loop [i 0 fs (seq fs)]
      (when fs
        (let [input ((first fs)
                     (fn [] (let [p (set! (.-pending ps) (dec (.-pending ps)))]
                              (when (zero? p) (ready ps))))
                     (fn [] (aset (.-inputs ps) i ps)
                       (set! (.-step ps) nil)
                       (let [p (set! (.-pending ps) (dec (.-pending ps)))]
                         (when-not (neg? p) (cancel ps))
                         (when (zero? p) (ready ps)))))]
          (when (nil? (aget inputs i)) (aset inputs i input)))
        (recur (inc i) (next fs))))
    (let [p (set! (.-pending ps) (+ (.-pending ps) arity))]
      (when (nil? (.-step ps)) (cancel ps))
      (when (zero? p) (ready ps))
      ps)))
