;; Derived from missionary/impl/Seed.cljs (missionary, EPL 2.0).
;; Ported to jolt: cljs protocol/field syntax -> clojure.lang, and
;; missionary.Cancelled -> an ex-info carrying ::ebb.impl.util/cancelled.

(ns ^:no-doc ebb.impl.seed
  (:require [ebb.impl.util :refer [cancelled]]))

(declare more)

(deftype Process [^:unsynchronized-mutable iterator notifier terminator]
  clojure.lang.IFn
  (invoke [_] (set! iterator nil) nil)
  clojure.lang.IDeref
  (deref [this]
    (if-some [i iterator]
      (let [x (.next i)] (more this i) x)
      (do (terminator)
          (throw (cancelled "Seed cancelled."))))))

(defn- more [^Process ps i]
  (if (.hasNext i)
    ((.-notifier ps))
    (do (set! (.-iterator ps) nil)
        ((.-terminator ps)))))

(defn run [coll n t]
  (if-some [xs (seq coll)]
    (let [i  (.iterator xs)
          ps (->Process i n t)]
      (more ps i) ps)
    ;; an empty seed terminates at once and never notifies
    (let [ps (->Process nil n t)]
      (t) ps)))
