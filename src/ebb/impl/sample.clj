;; Derived from missionary/impl/Sample.cljs (missionary, EPL 2.0).
;; Sample.java is synchronized, but every critical region here is entered only
;; from the sampler's own callbacks, which the busy/done toggle already
;; serialises -- the same protocol the cljs impl relies on. Callbacks are
;; invoked after the region, per ADR-001.
(ns ^:no-doc ebb.impl.sample)

(declare transfer)

(deftype Process [^:unsynchronized-mutable combinator
                  ^:unsynchronized-mutable notifier
                  terminator args inputs
                  ^:unsynchronized-mutable busy
                  ^:unsynchronized-mutable done
                  ^:unsynchronized-mutable alive]
  clojure.lang.IFn
  (invoke [_] (dotimes [i (alength inputs)] ((aget inputs i))) nil)
  clojure.lang.IDeref
  (deref [p] (transfer p)))

(defn ready [^Process ps]
  (let [args (.-args ps)
        inputs (.-inputs ps)
        sampled (dec (alength inputs))]
    (loop [cb nil]
      (if (set! (.-busy ps) (not (.-busy ps)))
        (if (.-done ps)
          (do (dotimes [i sampled]
                (let [input (aget inputs i)]
                  (input)
                  (if (identical? (aget args i) args)
                    (try @input (catch Throwable _ nil))
                    (aset args i args))))
              (when (zero? (set! (.-alive ps) (dec (.-alive ps))))
                (.-terminator ps)))
          (if (identical? (aget args sampled) args)
            (do (try @(aget inputs sampled)
                     (catch Throwable _ nil)) (recur cb))
            (.-notifier ps))) cb))))

(defn transfer [^Process ps]
  (let [c (.-combinator ps)
        args (.-args ps)
        inputs (.-inputs ps)
        sampled (dec (alength inputs))
        sampler (aget inputs sampled)
        x (try
            (try
              (when (nil? c) (throw (ex-info "Undefined continuous flow." {:type :ebb.impl.util/undefined})))
              (dotimes [i sampled]
                (when (identical? (aget args i) args)
                  (let [input (aget inputs i)]
                    (loop []
                      (aset args i nil)
                      (let [x @input]
                        (if (identical? (aget args i) args)
                          (recur) (aset args i x)))))))
              (catch Throwable e
                (try @sampler (catch Throwable _ nil))
                (throw e)))
            (aset args sampled @sampler)
            (apply c (vec args))
            (catch Throwable e
              (set! (.-notifier ps) nil)
              (sampler)
              (aset args sampled args) e))]
    (when-some [cb (ready ps)] (cb))
    (if (nil? (.-notifier ps))
      (throw x) x)))

(defn dirty [^Process p i]
  (let [args (.-args p)]
    (if (identical? (aget args i) args)
      (try @(aget (.-inputs p) i)
           (catch Throwable _ nil))
      (aset args i args))))

(defn run [c f fs n t]
  (let [it (.iterator (seq fs))
        arity (inc (count fs))
        args (object-array arity)
        inputs (object-array arity)
        ps (->Process c n t args inputs false false arity)
        done #(when (zero? (set! (.-alive ps) (dec (.-alive ps))))
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
                   (cb))))))) ps))