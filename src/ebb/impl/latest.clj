;; Derived from missionary/impl/Latest.cljs (missionary, EPL 2.0).
;; Latest.java drives one volatile state word by CAS; the cljs logic below is
;; already serialised by its own owner/sync protocol, which is what ebb keeps.
(ns ^:no-doc ebb.impl.latest
  (:require [ebb.impl.pairing-heap :as ph]))

(declare cancel transfer)

(deftype Process [^:unsynchronized-mutable step
                  done combinator
                  ^:unsynchronized-mutable value
                  ^:unsynchronized-mutable owner
                  args inputs children siblings
                  ^:unsynchronized-mutable pending
                  ^:unsynchronized-mutable head
                  ^:unsynchronized-mutable sync]
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

(defn ready [^Process ps]
  (let [step (.-step ps)
        done (.-done ps)]
    (loop []
      (if (nil? (.-head ps))
        (if (zero? (.-pending ps))
          (done)
          (if-some [s (.-sync ps)]
            (do (set! (.-sync ps) nil)
                (consume ps s)
                (recur))
            (set! (.-sync ps) idle)))
        (if (nil? step)
          (do (set! (.-owner ps) true)
              (try @(aget (.-inputs ps) (dequeue ps))
                   (catch Throwable _ nil))
              (set! (.-owner ps) false)
              (recur))
          (step))))))

(defn transfer [^Process ps]
  (when (nil? (.-step ps))
    (ready ps)
    (throw (ex-info "Uninitialized continuous flow." {:type :ebb.impl.util/undefined})))
  (set! (.-owner ps) true)
  (try
    (when (pos? (.-pending ps))
      (when-some [s (.-sync ps)]
        (set! (.-sync ps) nil)
        (consume ps s)))
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
      (set! (.-owner ps) false)
      (ready ps))))

(defn step [^Process ps i]
  (if (.-owner ps)
    (event ps i)
    (let [s (.-sync ps)]
      (if (identical? s idle)
        (do (set! (.-sync ps) nil)
            (event ps i)
            (ready ps))
        (do (aset (.-siblings ps) i s)
            (set! (.-sync ps) i))))))

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
        ps (->Process s d c nil true
             (object-array arity)
             (object-array arity)
             (object-array arity)
             (object-array arity)
             arity nil nil)]
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
    (set! (.-owner ps) false)
    (s) ps))