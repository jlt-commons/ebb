;; Derived from missionary (EPL 2.0):
;;   logic           <- missionary/impl/Watch.cljs
;;   synchronisation <- missionary/impl/Watch.java  (the process monitor)
;;
;; ADR-001 rule 1. Watch has no pull loop and therefore no claim to arbitrate --
;; what it has is one cell, `value`, which is READINESS AND NOTHING ELSE:
;; identical to the process means "the consumer has taken the last one, so the
;; next change should notify it". The watcher reads that and writes to it, and
;; `transfer` reads it and writes the flag back. Both are read-modify-writes on
;; the same field, and the watcher runs on whichever thread swapped the
;; reference -- so two changes landing at once both read "ready" and both
;; notify, which is one notification too many for the flow protocol.
;;
;; ---------------------------------------------------------------------------
;; THE VALUE IS NOT TAKEN FROM THE CALLBACK -- missionary #89, ebb-8nq.39
;;
;; Both of missionary's impls store the watcher's fourth argument and hand that
;; to `transfer`. References do not order their watches: if two threads swap a
;; reference from x to (f1 x) to (f2 (f1 x)), the two callbacks can reach the
;; process in either order, and the one that arrives second wins. The later
;; state is then lost and the flow reports a value the reference no longer
;; holds. Measured on the issue's own repro at 2 wrong in 40 here and 1 in 300
;; against missionary b.47 -- the same defect, more exposed on a fiber pool,
;; which spaces the two `via cpu` callbacks further apart than a thread pool.
;;
;; The issue states the fix and ebb takes it: "A correct implementation of
;; `watch` should ignore the value passed to the callback and deref on
;; transfer." So the callback's argument is discarded and `transfer` reads the
;; reference itself. THIS IS A DELIBERATE DIVERGENCE from missionary as it
;; stands, and doc/conformance.md carries it.
;;
;; The order matters and is the whole of the correctness argument: `transfer`
;; marks itself ready BEFORE it derefs, never after. A change landing in
;; between then still finds the flag set and notifies, so the consumer comes
;; back for a value it may already have seen -- a duplicate, which the flow
;; protocol allows and which `latest` and `cp` collapse anyway (#69). Derefing
;; first and marking after would let that change find the flag clear, and a
;; change nobody is notified of is exactly the bug being fixed.
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

(defn watch [^Process ps _ _ _]
  (when-some [cb (locking ps
                   (when-some [cb (.-notifier ps)]
                     (let [x (.-value ps)]
                       ;; record only THAT it changed, never what to
                       (set! (.-value ps) nil)
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
    (do (locking ps (set! (.-value ps) ps))
        @(.-reference ps))))

(defn run [r n t]
  ;; `value` starts NOT ready: the first notification below is the one that
  ;; hands the initial state over.
  (let [ps (->Process n t r nil)]
    (add-watch r ps watch)
    (n) ps))
