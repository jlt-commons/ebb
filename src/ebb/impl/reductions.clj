;; Derived from missionary (EPL 2.0):
;;   logic           <- missionary/impl/Reductions.cljs
;;   synchronisation <- missionary/impl/Reductions.java  (the process monitor)
;;
;; ADR-001 rule 1 via `ebb.impl.arbiter`; see that header for the protocol.
;;
;; `ready` BREAKS out of its loop whenever it hands a callback out, so the claim
;; is still held while the consumer runs `transfer`, and `transfer` gives it
;; back at the end. That interlock is why the process's fields need no lock of
;; their own: nothing else can be in the loop while a transfer is in flight,
;; and a notification arriving meanwhile only records `:more`.
(ns ^:no-doc ebb.impl.reductions
  (:require [ebb.impl.arbiter :as arb]))

(declare transfer)
(deftype Process [^:unsynchronized-mutable reducer ^:unsynchronized-mutable notifier terminator ^:unsynchronized-mutable result ^:unsynchronized-mutable input state ^:unsynchronized-mutable done]
  clojure.lang.IFn
  (invoke [_] (input))
  clojure.lang.IDeref
  (deref [p] (transfer p)))

(defn- rounds
  "Run rounds while the claim is held; answer the callback to fire outside, or
  nil. Returning a callback keeps the claim -- the Java's `break`, which leaves
  busy true for the transfer that follows."
  [^Process p]
  (loop []
    (cond
      (.-done p)            (.-terminator p)
      (nil? (.-reducer p))  (do (try @(.-input p) (catch Throwable _ nil))
                                (when (arb/release! (.-state p)) (recur)))
      :else                 (.-notifier p))))

(defn ready [^Process p]
  (when (arb/claim! (.-state p)) (rounds p)))

(defn transfer [^Process ps]
  (try
    (let [f (.-reducer ps)
          r (.-result ps)
          r (if (identical? r ps)
              (f) (f r @(.-input ps)))]
      (set! (.-result ps)
        (if (reduced? r)
          (do ((.-input ps))
              (set! (.-reducer ps) nil)
              @r) r)))
    (catch Throwable e
      ((.-input ps))
      (set! (.-notifier ps) nil)
      (set! (.-reducer ps) nil)
      (set! (.-result ps) e)))
  ;; the claim came in with the notification this transfer answers, so this is
  ;; a release, not a claim.
  (when-some [cb (when (arb/release! (.-state ps)) (rounds ps))] (cb))
  (if (nil? (.-notifier ps))
    (throw (.-result ps))
    (.-result ps)))

(defn run [rf f n t]
  (let [ps (->Process rf n t nil nil (arb/arbiter) false)]
    (set! (.-result ps) ps)
    (set! (.-input ps)
      (f #(when-some [cb (ready ps)] (cb))
        #(do (set! (.-done ps) true)
             (when-some [cb (ready ps)]
               (cb))))) (n) ps))
