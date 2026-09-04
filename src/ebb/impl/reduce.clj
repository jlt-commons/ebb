;; Derived from missionary (EPL 2.0):
;;   logic           <- missionary/impl/Reduce.cljs
;;   synchronisation <- missionary/impl/Reduce.java  (the process monitor)
;;
;; ADR-001 rule 1, via `ebb.impl.arbiter` -- read that namespace's header for
;; why the monitor is not portable and what the claim protocol replaces it with.
;;
;; This was the operator that proved the point: `(set! busy (not busy))` lost a
;; flip between the thread that called `run` and the input's own executor, and
;; missionary #108's shape -- `(?> ##Inf (seed (repeat 400 (via cpu :hi))))` --
;; stalled with `result` still at the initial accumulator (ebb-8nq.31).
;;
;; `m/reduce` is a TASK, not a flow, so there is no `transfer` to race with the
;; loop: the claim protocol is the whole of the arbitration here.
;;
;; missionary.Cancelled is an ex-info carrying ::ebb.impl.util/cancelled.

(ns ^:no-doc ebb.impl.reduce
  (:require [ebb.impl.arbiter :as arb]))

(declare transfer)

(deftype Process [^:unsynchronized-mutable reducer
                  ^:unsynchronized-mutable status
                  failure
                  ^:unsynchronized-mutable result
                  ^:unsynchronized-mutable input
                  state
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

(defn- pass!
  "Run rounds until the state goes idle. The caller must hold the claim."
  [^Process p]
  (loop []
    (if (.-done p)
      ;; terminal, so the claim is never handed back -- the Java's `break`,
      ;; which leaves busy true and stops every later notification dead.
      ((.-status p) (.-result p))
      (do (if (nil? (.-reducer p))
            (try @(.-input p) (catch Throwable _ nil))
            (transfer p))
          (when (arb/release! (.-state p)) (recur))))))

(defn- ready [^Process p]
  (when (arb/claim! (.-state p)) (pass! p)))

(defn run [rf flow success failure]
  ;; `run` starts out holding the claim, as the Java's busy=true does: a
  ;; notification raised while the flow is still being built is recorded and
  ;; replayed below, rather than running a round against a process whose input
  ;; is not assigned yet.
  (let [p (->Process rf success failure nil nil (arb/arbiter) false)]
    (set! (.-result p) p)
    (set! (.-input p)
      (flow (fn [] (ready p))
            (fn [] (set! (.-done p) true) (ready p))))
    (transfer p)
    (when (arb/release! (.-state p)) (pass! p))
    p))
