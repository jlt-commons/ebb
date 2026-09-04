;; Derived from missionary (EPL 2.0):
;;   logic           <- missionary/impl/Semaphore.cljs
;;   synchronisation <- missionary/impl/Semaphore.java  (AtomicReferenceFieldUpdater)
;;
;; Per ADR-001: one atom, one CAS, callbacks only after it succeeds.
;;
;; state is one of:
;;   nil           no permits available, nobody waiting
;;   n (pos int)   n permits available
;;   #{Event ...}  no permits, these are waiting
(ns ^:no-doc ebb.impl.semaphore
  (:require [ebb.impl.util :refer [nop cancelled]]
            [ebb.impl.event :as ev]))

(declare cancel-acquire)

(deftype Port [state]
  ev/Emitter
  (-cancel [this e] (cancel-acquire this e))

  clojure.lang.IFn
  ;; (port) -- release a permit, handing it straight to a waiter if there is one.
  (invoke [_]
    (loop []
      (let [s @state]
        (if (set? s)
          (let [e (first s)]
            (if (compare-and-set! state s (if (= 1 (count s)) nil (disj s e)))
              ((.-success ^ebb.impl.event.Event e) nil)
              (recur)))
          (when-not (compare-and-set! state s (if (nil? s) 1 (inc s)))
            (recur)))))
    nil)

  ;; (port success failure) -- acquire, as a task.
  (invoke [this s! f!]
    (loop []
      (let [s @state]
        (if (integer? s)
          (if (compare-and-set! state s (if (= 1 s) nil (dec s)))
            (do (s! nil) nop)
            (recur))
          (let [e (ev/event this s! f!)]
            (if (compare-and-set! state s (conj (or s #{}) e))
              e
              (recur))))))))

(defn- cancel-acquire [^Port port e]
  (loop []
    (let [s @(.-state port)]
      (when (and (set? s) (contains? s e))
        (if (compare-and-set! (.-state port) s (if (= 1 (count s)) nil (disj s e)))
          ((.-failure ^ebb.impl.event.Event e) (cancelled "Semaphore acquire cancelled."))
          (recur))))))

(defn make [n] (->Port (atom (when (pos? n) n))))
