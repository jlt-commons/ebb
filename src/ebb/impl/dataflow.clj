;; Derived from missionary (EPL 2.0):
;;   logic          <- missionary/impl/Dataflow.cljs
;;   synchronisation<- missionary/impl/Dataflow.java  (AtomicReferenceFieldUpdater)
;;
;; Per ADR-001, the whole port state lives in ONE atom and moves in one CAS, and
;; callbacks fire only after the CAS has succeeded -- never while the state is
;; half-updated, because a success callback may re-enter this port.
;;
;; state is one of:
;;   nil            unbound, nobody waiting
;;   #{Event ...}   unbound, these are waiting
;;   (reduced x)    bound to x
(ns ^:no-doc ebb.impl.dataflow
  (:require [ebb.impl.util :refer [nop cancelled]]
            [ebb.impl.event :as ev]))

(declare cancel-deref)

(deftype Port [state]
  ev/Emitter
  (-cancel [this e] (cancel-deref this e))

  clojure.lang.IFn
  ;; (port x) -- assign, idempotent: the first assignment wins and is returned.
  (invoke [_ x]
    (loop []
      (let [s @state]
        (if (reduced? s)
          @s
          (if (compare-and-set! state s (reduced x))
            (do (when (some? s) (doseq [^ebb.impl.event.Event e s] ((.-success e) x)))
                x)
            (recur))))))

  ;; (port success failure) -- deref, as a task.
  (invoke [this s! f!]
    (loop []
      (let [s @state]
        (if (reduced? s)
          (do (s! @s) nop)
          (let [e (ev/event this s! f!)]
            (if (compare-and-set! state s (conj (or s #{}) e))
              e
              (recur))))))))

(defn- cancel-deref [^Port port e]
  (loop []
    (let [s @(.-state port)]
      (when (and (set? s) (contains? s e))
        (if (compare-and-set! (.-state port) s (if (= 1 (count s)) nil (disj s e)))
          ((.-failure ^ebb.impl.event.Event e)
           (cancelled "Dataflow variable dereference cancelled."))
          (recur))))))

(defn make [] (->Port (atom nil)))
