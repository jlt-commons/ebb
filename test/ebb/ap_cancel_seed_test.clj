;; A cancelled ap must stop drawing from the flow it forked on. Found in
;; samizdat (karamazov-3cll.9): a supervisor stream is a reduce over
;;   (ap (let [_ (?> (seed (repeat nil)))] (? (sleep poll-ms)) (drain)))
;; and stopping it cancelled the reduce, which cancelled the ap -- whose
;; parks were then cancelled on arrival while the fork kept pulling the next
;; value from an infinite seed. Measured: 88,641 iterations in the two
;; seconds after the cancel, and the reduce never settled. Missionary's
;; Ambiguous cancels the forked flow along with the parks; ebb's did not.
(ns ebb.ap-cancel-seed-test
  (:require [clojure.test :as t]
            [ebb.core :as m]))

(defn- start [task]
  (let [done (promise)
        cancel (task (fn [v] (deliver done [:ok v])) (fn [e] (deliver done [:err e])))]
    {:done done :cancel cancel}))

(defn- observed
  "A flow that counts the cancels reaching it."
  [flow counter]
  (fn [n t]
    (let [ps (flow n t)]
      (reify clojure.lang.IFn
        (invoke [_] (swap! counter inc) (ps))
        clojure.lang.IDeref
        (deref [_] @ps)))))

(t/deftest cancelling-an-ap-cancels-the-flow-it-forked-on
  (let [seed-cancels (atom 0)
        runs (atom 0)
        flow (m/ap (let [x (m/?> (observed (m/seed (repeat 1)) seed-cancels))]
                     (swap! runs inc)
                     (m/? (m/sleep 5))
                     x))
        {:keys [done cancel]} (start (m/reduce (fn [_ _] nil) nil flow))]
    (Thread/sleep 60)
    (t/is (pos? @runs) "the flow was running")
    (let [before @runs]
      (cancel)
      (Thread/sleep 100)
      (t/is (= 1 @seed-cancels) "the cancel reached the seed the fork draws from")
      (let [after @runs]
        (Thread/sleep 300)
        (t/is (= after @runs) "no further forks once cancelled")
        (t/is (< (- after before) 3) "and at most the branch already in flight ran"))
      (let [[tag e] (deref done 1000 [:unsettled nil])]
        (t/is (= :err tag) "the reduce settled")
        (t/is (m/cancelled? e) "with Cancelled")))))

(t/deftest cancelling-an-ap-parked-in-a-fork-terminates-it
  ;; The same shape without a park in the body: a finite seed the consumer
  ;; cancels part-way through must also end.
  (let [flow (m/ap (let [x (m/?> (m/seed (range 100000)))]
                     (m/? (m/sleep 1))
                     x))
        {:keys [done cancel]} (start (m/reduce (fn [acc x] (conj acc x)) [] flow))]
    (Thread/sleep 30)
    (cancel)
    (let [[tag e] (deref done 1000 [:unsettled nil])]
      (t/is (= :err tag))
      (t/is (m/cancelled? e)))))
