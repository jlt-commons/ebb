;; Derived from missionary/impl/Eduction.cljs (missionary, EPL 2.0).
;; See Eduction.java for the synchronisation strategy (ADR-001 rule 1).
(ns ^:no-doc ebb.impl.eduction)

(declare cancel transfer)
(deftype Process
  [^:unsynchronized-mutable reducer ^:unsynchronized-mutable iterator notifier terminator ^:unsynchronized-mutable buffer ^:unsynchronized-mutable offset ^:unsynchronized-mutable length ^:unsynchronized-mutable error ^:unsynchronized-mutable busy ^:unsynchronized-mutable done]
  clojure.lang.IFn
  (invoke [t] (cancel t))
  clojure.lang.IDeref
  (deref [t] (transfer t)))


;; The cljs impl keeps a growable JS array and calls .push. A jolt array has a
;; fixed length, so the buffer grows by doubling instead -- a transducer such as
;; `cat` emits more values than it consumes, which is exactly what overruns it.
(defn feed
  ([^Process t] t)
  ([^Process t x]
   (let [b (.-buffer t)
         n (.-length t)]
     (if (= n (alength b))
       (let [b2 (object-array (max 8 (* 2 (alength b))))]
         (dotimes [i n] (aset b2 i (aget b i)))
         (aset b2 n x)
         (set! (.-buffer t) b2))
       (aset b n x)))
   (set! (.-length t) (inc (.-length t))) t))

(defn pull [^Process t]
  (loop []
    (if (.-done t)
      (if-some [rf (.-reducer t)]
        (do (set! (.-offset t) 0)
            (set! (.-length t) 0)
            (try (rf t)
                 (catch Throwable e
                   (set! (.-error t) (.-length t))
                   (feed t e)))
            (set! (.-reducer t) nil)
            (if (zero? (.-length t))
              (recur)
              (do ((.-notifier t))
                  (when (set! (.-busy t) (not (.-busy t))) (recur)))))
        ((.-terminator t)))
      (if-some [rf (.-reducer t)]
        (do (set! (.-offset t) 0)
            (set! (.-length t) 0)
            (try (when (reduced? (rf t @(.-iterator t)))
                   (rf t)
                   (set! (.-reducer t) nil)
                   (cancel t))
                 (catch Throwable e
                   (set! (.-error t) (.-length t))
                   (feed t e)
                   (set! (.-reducer t) nil)
                   (cancel t)))
            (if (pos? (.-length t))
              ((.-notifier t))
              (when (set! (.-busy t) (not (.-busy t))) (recur))))
        (do (try @(.-iterator t) (catch Throwable _ nil))
            (when (set! (.-busy t) (not (.-busy t))) (recur)))))))

(defn cancel [^Process t]
  ((.-iterator t)))

(defn transfer [^Process t]
  (let [o (.-offset t)
        x (aget (.-buffer t) o)]
    (aset (.-buffer t) o nil)
    (set! (.-offset t) (inc o))
    (if (= (.-offset t) (.-length t))
      (when (set! (.-busy t) (not (.-busy t)))
        (pull t)) ((.-notifier t)))
    (if (= o (.-error t)) (throw x) x)))

(defn run [xf flow n t]
  (let [t (->Process (xf feed) nil n t (object-array 1) 0 0 -1 true false)
        n #(when (set! (.-busy t) (not (.-busy t))) (pull t))]
    (set! (.-iterator t) (flow n #(do (set! (.-done t) true) (n))))
    (n) t))