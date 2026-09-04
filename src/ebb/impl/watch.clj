;; Derived from missionary/impl/Watch.cljs (missionary, EPL 2.0).
;; See Watch.java for the synchronisation strategy (ADR-001 rule 1).
(ns ^:no-doc ebb.impl.watch
  (:require [ebb.impl.util :refer [cancelled]]))

(declare kill transfer)
(deftype Process [^:unsynchronized-mutable notifier terminator reference ^:unsynchronized-mutable value]
  clojure.lang.IFn
  (invoke [this] (kill this) nil)
  clojure.lang.IDeref
  (deref [this] (transfer this)))

(defn watch [^Process ps _ _ curr]
  (when-some [cb (.-notifier ps)]
    (let [x (.-value ps)]
      (set! (.-value ps) curr)
      (when (identical? x ps) (cb)))))

(defn kill [^Process ps]
  (when-some [cb (.-notifier ps)]
    (set! (.-notifier ps) nil)
    (let [x (.-value ps)]
      (set! (.-value ps) nil)
      (when (identical? x ps) (cb)))))

(defn transfer [^Process ps]
  (if (nil? (.-notifier ps))
    (do ((.-terminator ps))
        (remove-watch (.-reference ps) ps)
        (throw (cancelled "Watch cancelled.")))
    (let [x (.-value ps)]
      (set! (.-value ps) ps) x)))

(defn run [r n t]
  (let [ps (->Process n t r @r)]
    (add-watch r ps watch)
    (n) ps))
