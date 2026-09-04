;; Derived from missionary (EPL 2.0):
;;   logic           <- missionary/impl/Watch.cljs
;;   synchronisation <- missionary/impl/Watch.java  (the process monitor)
;;
;; ADR-001 rule 1. Watch has no pull loop and therefore no claim to arbitrate --
;; what it has is one cell, `value`, doubling as the readiness flag: `value`
;; identical to the process means "the consumer has taken the last one, the next
;; change should notify it". The watcher reads that and writes the new value,
;; and `transfer` reads it and writes the flag back. Both are read-modify-writes
;; on the same field, and the watcher runs on whichever thread swapped the
;; reference -- so two changes landing at once both read "ready" and both
;; notify, which is one notification too many for the flow protocol.
;;
;; The monitor ports directly here, because none of these regions derefs an
;; input or invokes a callback: the notifier fires after the region, exactly as
;; the Java arranges it. Nothing can park inside, so rule 5 is satisfied.
(ns ^:no-doc ebb.impl.watch
  (:require [ebb.impl.util :refer [cancelled]]))

(declare kill transfer)
(deftype Process [^:unsynchronized-mutable notifier terminator reference ^:unsynchronized-mutable value]
  clojure.lang.IFn
  (invoke [this] (kill this) nil)
  clojure.lang.IDeref
  (deref [this] (transfer this)))

(defn watch [^Process ps _ _ curr]
  (when-some [cb (locking ps
                   (when-some [cb (.-notifier ps)]
                     (let [x (.-value ps)]
                       (set! (.-value ps) curr)
                       (when (identical? x ps) cb))))]
    (cb)))

(defn kill [^Process ps]
  (when-some [cb (locking ps
                   (when-some [cb (.-notifier ps)]
                     (set! (.-notifier ps) nil)
                     (let [x (.-value ps)]
                       (set! (.-value ps) nil)
                       (when (identical? x ps) cb))))]
    (cb)))

(defn transfer [^Process ps]
  (if (nil? (.-notifier ps))
    (do ((.-terminator ps))
        (remove-watch (.-reference ps) ps)
        (throw (cancelled "Watch cancelled.")))
    (locking ps
      (let [x (.-value ps)]
        (set! (.-value ps) ps) x))))

(defn run [r n t]
  (let [ps (->Process n t r @r)]
    (add-watch r ps watch)
    (n) ps))
