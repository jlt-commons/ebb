;; Derived from missionary/impl/Sleep.cljs (missionary, EPL 2.0).
;; Ported to jolt: cljs protocol/field syntax -> clojure.lang, and
;; missionary.Cancelled -> an ex-info carrying ::ebb.impl.util/cancelled.

(ns ^:no-doc ebb.impl.sleep
  (:require [ebb.impl.util :as u]))

(deftype Process [failure ^:unsynchronized-mutable pending]
  clojure.lang.IFn
  (invoke [this]
    (when pending
      (set! pending false)
      (failure (u/cancelled "Sleep cancelled.")))
    nil))

(defn run [d x s f]
  (let [slp (->Process f true)]
    (u/schedule! d (fn []
                     (when (.-pending slp)
                       (set! (.-pending slp) false)
                       (s x))))
    slp))
