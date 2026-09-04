;; Derived from missionary (EPL 2.0):
;;   logic           <- missionary/impl/Observe.cljs
;;   synchronisation <- missionary/impl/Observe.java  (the process monitor)
;;
;; ADR-001 rule 1. Like Watch, Observe has no pull loop -- it has `value`, which
;; is both the pending event and the readiness flag, read and written by the
;; producer's callback, by `transfer` and by `kill`. The producer is whatever
;; the user's `subscribe` hands the emit function to, so those three can be on
;; three different executors, and `(set! (.-value ps) x)` after a separate read
;; of it loses events between them. `locking` makes each pair one step; the
;; notifier and the terminator fire outside the region, so nothing parks in it
;; (rule 5).
;;
;; ONE region of the Java does not port: `kill` holds the monitor across the
;; user's unsubscribe function. That is arbitrary user code and may park, so it
;; runs outside -- and to keep `transfer` from seeing a half-finished kill, the
;; unsubscribe function is TAKEN out of the field under the lock and the
;; Cancelled it will be replaced by is published in the same step. A transfer
;; racing the kill then reads the Cancelled, never the raw function.
;;
;; The producer's backpressure is missionary's own cljs divergence, not ebb's:
;; Observe.java makes an over-eager producer WAIT on the monitor until the
;; consumer has transferred, while the .cljs impl -- and so this one -- throws
;; "Can't process event - consumer is not ready." See doc/conformance.md.
(ns ^:no-doc ebb.impl.observe
  (:require [ebb.impl.util :refer [cancelled]]))

(declare kill transfer)

(deftype Process [^:unsynchronized-mutable notifier terminator ^:unsynchronized-mutable unsub ^:unsynchronized-mutable value]
  clojure.lang.IFn
  (invoke [this] (kill this) nil)
  clojure.lang.IDeref
  (deref [this] (transfer this)))

(defn kill [^Process ps]
  (when-some [taken (locking ps
                      (when-some [cb (.-notifier ps)]
                        (set! (.-notifier ps) nil)
                        (let [u (.-unsub ps)]
                          (set! (.-unsub ps) (cancelled "Observe cancelled."))
                          [cb u])))]
    (let [[cb u] taken]
      (try (u) (catch Throwable e (locking ps (set! (.-unsub ps) e))))
      (when (locking ps
              (let [x (.-value ps)]
                (set! (.-value ps) nil)
                (identical? x ps)))
        (cb)))))

(defn transfer [^Process ps]
  (if (nil? (.-notifier ps))
    (do ((.-terminator ps))
        (throw (.-unsub ps)))
    (locking ps
      (let [x (.-value ps)]
        (set! (.-value ps) ps) x))))

(defn run [s n t]
  (let [ps (->Process n t nil nil)]
    (set! (.-value ps) ps)
    (try (set! (.-unsub ps)
           (s (fn [x]
                (when-some [cb (locking ps
                                 (when-some [cb (.-notifier ps)]
                                   (if (identical? ps (.-value ps))
                                     (do (set! (.-value ps) x) cb)
                                     (throw (ex-info "Can't process event - consumer is not ready." {})))))]
                  (cb)))))
         (catch Throwable e
           (set! (.-unsub ps) e)
           (set! (.-notifier ps) nil)
           (if (identical? ps (.-value ps))
             (n) (set! (.-value ps) ps)))) ps))
