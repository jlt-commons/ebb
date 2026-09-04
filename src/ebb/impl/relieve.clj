;; Derived from missionary/impl/Relieve.cljs (missionary, EPL 2.0).
;; See Relieve.java for the synchronisation strategy (ADR-001 rule 1).
(ns ^:no-doc ebb.impl.relieve)

(declare transfer)
(deftype Process [^:unsynchronized-mutable reducer ^:unsynchronized-mutable notifier terminator ^:unsynchronized-mutable iterator ^:unsynchronized-mutable current ^:unsynchronized-mutable busy ^:unsynchronized-mutable done]
  clojure.lang.IFn
  (invoke [_] (iterator))
  clojure.lang.IDeref
  (deref [p] (transfer p)))

(defn transfer [^Process ps]
  (let [x (.-current ps)]
    (set! (.-current ps) ps)
    (when (nil? (.-reducer ps))
      ((.-terminator ps)))
    (if (nil? (.-notifier ps))
      (throw x) x)))

(defn ready [^Process ps]
  (loop [cb nil]
    (if (set! (.-busy ps) (not (.-busy ps)))
      (recur (if (.-done ps)
               (do (set! (.-reducer ps) nil)
                   (if (identical? ps (.-current ps))
                     (.-terminator ps) cb))
               (if-some [n (.-notifier ps)]
                 (let [r (.-current ps)]
                   (try (let [x @(.-iterator ps)]
                          (set! (.-current ps)
                            (if (identical? r ps)
                              x ((.-reducer ps) r x))))
                        (catch Throwable e
                          (set! (.-current ps) e)
                          (set! (.-notifier ps) nil)
                          ((.-iterator ps))))
                   (if (identical? r ps) n cb))
                 (do (try @(.-iterator ps) (catch Throwable _ nil)) cb)))) cb)))

(defn run [rf f n t]
  (let [ps (->Process rf n t nil nil true false)]
    (set! (.-current ps) ps)
    (set! (.-iterator ps)
      (f #(when-some [cb (ready ps)] (cb))
        #(do (set! (.-done ps) true)
             (when-some [cb (ready ps)] (cb)))))
    (when-some [cb (ready ps)] (cb)) ps))