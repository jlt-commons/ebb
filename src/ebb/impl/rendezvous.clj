;; Derived from missionary (EPL 2.0):
;;   logic           <- missionary/impl/Rendezvous.cljs
;;   synchronisation <- missionary/impl/Rendezvous.java  (AtomicReferenceFieldUpdater)
;;
;; Per ADR-001: one atom, one CAS, callbacks only after it succeeds.
;;
;; state is one of:
;;   nil                 idle
;;   #{Event ...}        takers waiting
;;   {Event value, ...}  givers waiting, each with the value it is offering
;;
;; A taker's Event is emitted by the Port; a giver's by its Give, so cancelling
;; each reaches the right half. That is why Give is its own type rather than a
;; closure -- it has to be an Emitter.
(ns ^:no-doc ebb.impl.rendezvous
  (:require [ebb.impl.util :refer [nop cancelled]]
            [ebb.impl.event :as ev]))

(declare cancel-take cancel-give give)

(deftype Give [port value]
  ev/Emitter
  (-cancel [this e] (cancel-give this e))
  clojure.lang.IFn
  (invoke [this s! f!] (give this s! f!)))

(deftype Port [state]
  ev/Emitter
  (-cancel [this e] (cancel-take this e))

  clojure.lang.IFn
  ;; (port x) -- a task that gives x.
  (invoke [this x] (->Give this x))

  ;; (port success failure) -- a task that takes.
  (invoke [this s! f!]
    (loop []
      (let [s @state]
        (if (map? s)
          (let [[e v] (first s)]
            (if (compare-and-set! state s (if (= 1 (count s)) nil (dissoc s e)))
              (do ((.-success ^ebb.impl.event.Event e) nil) (s! v) nop)
              (recur)))
          (let [e (ev/event this s! f!)]
            (if (compare-and-set! state s (conj (or s #{}) e))
              e
              (recur))))))))

(defn- give [^Give g s! f!]
  (let [^Port port (.-port g)
        value      (.-value g)
        state      (.-state port)]
    (loop []
      (let [s @state]
        (if (set? s)
          (let [e (first s)]
            (if (compare-and-set! state s (if (= 1 (count s)) nil (disj s e)))
              (do ((.-success ^ebb.impl.event.Event e) value) (s! nil) nop)
              (recur)))
          (let [e (ev/event g s! f!)]
            (if (compare-and-set! state s (assoc (or s {}) e value))
              e
              (recur))))))))

(defn- cancel-take [^Port port e]
  (loop []
    (let [s @(.-state port)]
      (when (and (set? s) (contains? s e))
        (if (compare-and-set! (.-state port) s (if (= 1 (count s)) nil (disj s e)))
          ((.-failure ^ebb.impl.event.Event e) (cancelled "Rendez-vous take cancelled."))
          (recur))))))

(defn- cancel-give [^Give g e]
  (let [state (.-state ^Port (.-port g))]
    (loop []
      (let [s @state]
        (when (and (map? s) (contains? s e))
          (if (compare-and-set! state s (if (= 1 (count s)) nil (dissoc s e)))
            ((.-failure ^ebb.impl.event.Event e) (cancelled "Rendez-vous give cancelled."))
            (recur)))))))

(defn make [] (->Port (atom nil)))
