;; Derived from missionary (EPL 2.0):
;;   logic           <- missionary/impl/RaceJoin.cljs
;;   synchronisation <- missionary/impl/RaceJoin.java  (two atomic int fields)
;;
;; One process serves both `join` and `race`; `r` picks which way round the
;; success/failure callbacks go. Under join, the first failure wins and cancels
;; the rest; under race, the first success does.
;;
;; Per ADR-001 this is the CAS family, but unlike the ports it has two
;; independent atomic counters rather than one state word, so it keeps them as
;; two atoms exactly as the Java does:
;;
;;   race  -2 = still spawning children, -1 = spawned and nobody has won,
;;         >= 0 = index of the winner
;;   join  how many children have terminated
;;
;; The -2 -> -1 CAS at the end of `run` is what makes cancellation safe when a
;; child completes synchronously during spawning: the winner sees -2, cannot
;; cancel siblings that do not exist yet, and `run` cancels on its behalf.
(ns ^:no-doc ebb.impl.race-join
  (:require [ebb.impl.util :as u]))

(deftype Process [combinator joincb racecb children result join race]
  clojure.lang.IFn
  (invoke [this] (.cancel this) nil)
  Object
  (cancel [_]
    (dotimes [i (alength children)]
      (when-some [c (aget children i)] (c)))))

(defn- cancel [^Process ps] (.cancel ps))

(defn- terminated [^Process ps]
  (when (= (alength (.-result ps)) (swap! (.-join ps) inc))
    (let [w @(.-race ps)]
      (if (neg? w)
        (try ((.-joincb ps) (apply (.-combinator ps) (vec (.-result ps))))
             (catch Throwable e ((.-racecb ps) e)))
        ((.-racecb ps) (aget (.-result ps) w))))))

(defn run [r c ts s f]
  (let [n  (count ts)
        ps (->Process c (if r f s) (if r s f)
                      (object-array n) (object-array n) (atom 0) (atom -2))]
    ;; An explicit loop, NOT (dorun (map-indexed ...)). Realizing a lazy seq
    ;; holds a counted lock, and spawning a child can park -- and jolt forbids a
    ;; fiber leaving the CPU while its carrier holds a counted lock
    ;; (host/chez/locks.ss). Missionary loops over an iterator here for its own
    ;; reasons; on jolt it is load-bearing.
    ;; Children are started inside one batch, so joining N processes costs the
    ;; slowest first park rather than the sum of all of them (see
    ;; ebb.impl.util/batch-starts). The wait happens INSIDE the race = -2
    ;; window, which is exactly what that window is for: a child completing
    ;; while its siblings do not yet exist cannot cancel them, and `run`
    ;; cancels on its behalf below.
    (u/batch-starts
     (fn []
      (loop [index 0 ts (seq ts)]
       (when ts
        (let [join-cb (fn [x]
                        (aset (.-result ps) index x)
                        (terminated ps)
                        nil)
              race-cb (fn [x]
                        (loop []
                          (let [w @(.-race ps)]
                            (when (neg? w)
                              (if (compare-and-set! (.-race ps) w index)
                                (when (= -1 w) (cancel ps))
                                (recur)))))
                        (join-cb x))]
          (aset (.-children ps) index
                ((first ts) (if r race-cb join-cb)
                            (if r join-cb race-cb))))
        (recur (inc index) (next ts)))))) 
    (when-not (compare-and-set! (.-race ps) -2 -1) (cancel ps))
    ps))
