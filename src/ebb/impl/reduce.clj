;; Derived from missionary/impl/Reduce.cljs (missionary, EPL 2.0).
;; Ported to jolt: cljs protocol/field syntax -> clojure.lang, and
;; missionary.Cancelled -> an ex-info carrying ::ebb.impl.util/cancelled.

(ns ^:no-doc ebb.impl.reduce)

(declare transfer)

(deftype Process [^:unsynchronized-mutable reducer
                  ^:unsynchronized-mutable status
                  failure
                  ^:unsynchronized-mutable result
                  ^:unsynchronized-mutable input
                  ^:unsynchronized-mutable busy
                  ^:unsynchronized-mutable done]
  clojure.lang.IFn
  (invoke [_] (input)))

(defn- transfer [^Process p]
  (set! (.-result p)
    (try
      (let [f (.-reducer p)
            r (.-result p)
            r (if (identical? r p) (f) (f r @(.-input p)))]
        (if (reduced? r)
          (do ((.-input p)) (set! (.-reducer p) nil) @r) r))
      (catch Throwable e
        ((.-input p)) (set! (.-reducer p) nil)
        (set! (.-status p) (.-failure p)) e))))

(defn- ready [^Process p]
  (loop []
    (when (set! (.-busy p) (not (.-busy p)))
      (if (.-done p)
        ((.-status p) (.-result p))
        (do (if (nil? (.-reducer p))
              (try @(.-input p) (catch Throwable _ nil))
              (transfer p))
            (recur))))))

(defn run [rf flow success failure]
  (let [p (->Process rf success failure nil nil true false)]
    (set! (.-result p) p)
    (set! (.-input p)
      (flow (fn [] (ready p))
            (fn [] (set! (.-done p) true) (ready p))))
    (transfer p)
    (ready p) p))
