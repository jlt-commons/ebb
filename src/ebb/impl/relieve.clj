;; Derived from missionary (EPL 2.0):
;;   logic           <- missionary/impl/Relieve.cljs
;;   synchronisation <- missionary/impl/Relieve.java  (the process monitor)
;;
;; ADR-001 rule 1 via `ebb.impl.arbiter`; see that header for the protocol.
;;
;; Relieve is the case the claim protocol does NOT finish on its own. `ready`
;; never breaks -- it hands the notifier back as a return value and keeps
;; looping until the state goes idle -- so by the time the consumer is notified
;; the claim is already released, and the next round can run WHILE the consumer
;; is inside `transfer`. Both touch `current`, which is the whole point of the
;; operator: the round accumulates into it, the transfer empties it.
;;
;; So `current` gets `locking` on top of the claim, held for the field updates
;; only. The input deref stays outside it (rule 5: for an ap or cp input that
;; deref parks), which moves the read of `current` from BEFORE the deref, where
;; the Java has it, to after. That is still one of the two serialisations the
;; monitor allows -- if the consumer got there first it finds `current` empty
;; and notifies again for the new value, instead of folding the new value into
;; a batch the consumer has already taken. One notification per value either
;; way, which is what the flow protocol asks for.
;;
;; The relieving function runs under the lock, as it does under the monitor.
(ns ^:no-doc ebb.impl.relieve
  (:require [ebb.impl.arbiter :as arb]))

(declare transfer)
(deftype Process [^:unsynchronized-mutable reducer ^:unsynchronized-mutable notifier terminator ^:unsynchronized-mutable iterator ^:unsynchronized-mutable current state ^:unsynchronized-mutable done]
  clojure.lang.IFn
  (invoke [_] (iterator))
  clojure.lang.IDeref
  (deref [p] (transfer p)))

(defn transfer [^Process ps]
  (let [snap (locking ps
               (let [s [(.-notifier ps) (.-reducer ps) (.-current ps)]]
                 (set! (.-current ps) ps)
                 s))
        [n r x] snap]
    (when (nil? r) ((.-terminator ps)))
    (if (nil? n) (throw x) x)))

(defn- commit!
  "Fold res into `current` and answer [cb kill?]: the callback to fire and
  whether the input must be cancelled, both of them outside the lock."
  [^Process ps n cb res]
  (locking ps
    (let [r     (.-current ps)
          kill? (if (identical? ::ok (nth res 0))
                  (try (set! (.-current ps)
                             (if (identical? r ps)
                               (nth res 1)
                               ((.-reducer ps) r (nth res 1))))
                       false
                       (catch Throwable e
                         (set! (.-current ps) e)
                         (set! (.-notifier ps) nil)
                         true))
                  (do (set! (.-current ps) (nth res 1))
                      (set! (.-notifier ps) nil)
                      true))]
      [(if (identical? r ps) n cb) kill?])))

(defn- rounds
  "Run rounds while the claim is held. Unlike the other operators this loop
  never breaks, so it always ends idle and never leaves the claim behind."
  [^Process ps]
  (loop [cb nil]
    (let [cb (if (.-done ps)
               (locking ps
                 (set! (.-reducer ps) nil)
                 (if (identical? ps (.-current ps)) (.-terminator ps) cb))
               (if-some [n (.-notifier ps)]
                 (let [res  (try [::ok @(.-iterator ps)]
                                 (catch Throwable e [::err e]))
                       [cb kill?] (commit! ps n cb res)]
                   (when kill? ((.-iterator ps)))
                   cb)
                 (do (try @(.-iterator ps) (catch Throwable _ nil)) cb)))]
      (if (arb/release! (.-state ps)) (recur cb) cb))))

(defn ready [^Process ps]
  (when (arb/claim! (.-state ps)) (rounds ps)))

(defn run [rf f n t]
  (let [ps (->Process rf n t nil nil (arb/arbiter) false)]
    (set! (.-current ps) ps)
    (set! (.-iterator ps)
      (f #(when-some [cb (ready ps)] (cb))
        #(do (set! (.-done ps) true)
             (when-some [cb (ready ps)] (cb)))))
    ;; `run` came in holding the claim, so this is a release.
    (when-some [cb (when (arb/release! (.-state ps)) (rounds ps))] (cb))
    ps))
