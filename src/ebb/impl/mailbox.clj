;; Derived from missionary (EPL 2.0):
;;   logic           <- missionary/impl/Mailbox.cljs
;;   synchronisation <- missionary/impl/Mailbox.java  (AtomicReferenceFieldUpdater)
;;
;; Per ADR-001: one atom, one CAS, callbacks only after it succeeds.
;;
;; The .cljs impl keeps two JS arrays and flips them (.push/.pop/.reverse). That
;; is a single-threaded trick and cannot survive a CAS retry, so ebb follows the
;; Java shape: one persistent vector, dropping the head with subvec.
;;
;; state is one of:
;;   nil            empty, nobody waiting
;;   [x ...]        queued messages, oldest first
;;   #{Event ...}   empty, these are waiting
(ns ^:no-doc ebb.impl.mailbox
  (:require [ebb.impl.util :refer [nop cancelled]]
            [ebb.impl.event :as ev]))

(declare cancel-fetch)

(deftype Port [state]
  ev/Emitter
  (-cancel [this e] (cancel-fetch this e))

  clojure.lang.IFn
  ;; (port x) -- post, handing straight to a waiter if there is one.
  (invoke [_ x]
    (loop []
      (let [s @state]
        (if (set? s)
          (let [e (first s)]
            (if (compare-and-set! state s (if (= 1 (count s)) nil (disj s e)))
              ((.-success ^ebb.impl.event.Event e) x)
              (recur)))
          (when-not (compare-and-set! state s (conj (or s []) x))
            (recur)))))
    nil)

  ;; (port success failure) -- fetch, as a task.
  (invoke [this s! f!]
    (loop []
      (let [s @state]
        (if (vector? s)
          (let [n (count s)]
            (if (compare-and-set! state s (if (= 1 n) nil (subvec s 1 n)))
              (do (s! (nth s 0)) nop)
              (recur)))
          (let [e (ev/event this s! f!)]
            (if (compare-and-set! state s (conj (or s #{}) e))
              e
              (recur))))))))

(defn- cancel-fetch [^Port port e]
  (loop []
    (let [s @(.-state port)]
      (when (and (set? s) (contains? s e))
        (if (compare-and-set! (.-state port) s (if (= 1 (count s)) nil (disj s e)))
          ((.-failure ^ebb.impl.event.Event e) (cancelled "Mailbox fetch cancelled."))
          (recur))))))

(defn make [] (->Port (atom nil)))
