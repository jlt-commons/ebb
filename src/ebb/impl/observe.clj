;; Derived from missionary/impl/Observe.cljs (missionary, EPL 2.0).
;; See Observe.java for the synchronisation strategy (ADR-001 rule 1).
(ns ^:no-doc ebb.impl.observe
  (:require [ebb.impl.util :refer [cancelled]]))

(declare kill transfer)

(deftype Process [^:unsynchronized-mutable notifier terminator ^:unsynchronized-mutable unsub ^:unsynchronized-mutable value]
  clojure.lang.IFn
  (invoke [this] (kill this) nil)
  clojure.lang.IDeref
  (deref [this] (transfer this)))

(defn kill [^Process ps]
  (when-some [cb (.-notifier ps)]
    (set! (.-notifier ps) nil)
    (try ((.-unsub ps))
         (set! (.-unsub ps) (cancelled "Observe cancelled."))
         (catch Throwable e
           (set! (.-unsub ps) e)))
    (let [x (.-value ps)]
      (set! (.-value ps) nil)
      (when (identical? x ps) (cb)))))

(defn transfer [^Process ps]
  (if (nil? (.-notifier ps))
    (do ((.-terminator ps))
        (throw (.-unsub ps)))
    (let [x (.-value ps)]
      (set! (.-value ps) ps) x)))

(defn run [s n t]
  (let [ps (->Process n t nil nil)]
    (set! (.-value ps) ps)
    (try (set! (.-unsub ps)
           (s (fn [x]
                (when-some [cb (.-notifier ps)]
                  (if (identical? ps (.-value ps))
                    (do (set! (.-value ps) x) (cb))
                    (throw (ex-info "Can't process event - consumer is not ready." {})))))))
         (catch Throwable e
           (set! (.-unsub ps) e)
           (set! (.-notifier ps) nil)
           (if (identical? ps (.-value ps))
             (n) (set! (.-value ps) ps)))) ps))