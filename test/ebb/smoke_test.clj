(ns ebb.smoke-test
  "The feasibility spike's checks, kept green as the port proceeds. These are
  ebb's own tests, not ports of missionary's -- the ported suite replaces the
  overlapping ones as each impl bead lands."
  (:require [clojure.test :refer [deftest is testing]]
            [ebb.core :as m]))

;; parks inside an ordinary fn -- the capability cloroutine's lexical transform
;; cannot give missionary
(defn- nested [ms v] (m/? (m/sleep ms v)))
(defn- deeper [v] (inc (nested 10 v)))

(deftest tasks
  (is (= 42 (m/? (m/sleep 20 42))) "sleep completes with its value")
  (testing "join runs its tasks concurrently"
    (let [t0 (System/currentTimeMillis)
          r  (m/? (m/join vector (m/sleep 60 1) (m/sleep 60 2) (m/sleep 60 3)))]
      (is (= [1 2 3] r))
      (is (< (- (System/currentTimeMillis) t0) 150))))
  (is (= :fast (m/? (m/race (m/sleep 150 :slow) (m/sleep 20 :fast))))))

(deftest sp-parks-at-any-depth
  (is (= 6 (m/? (m/sp (deeper 5)))) "? works inside a plain helper fn")
  (is (= :done (m/? (m/sp (try (m/? (m/sleep 10)) :done (finally nil)))))
      "? works inside try/finally")
  (is (= [10 20] (m/? (m/join vector (m/sp (nested 20 10)) (m/sp (nested 20 20)))))
      "sp composes with join"))

(deftest flows
  (is (= [1 2 3] (m/? (m/reduce conj [] (m/seed [1 2 3])))))
  (is (= 15 (m/? (m/reduce + 0 (m/seed (range 1 6)))))))

(deftest ports
  (testing "dfv"
    (let [v (m/dfv)] (v :bound) (is (= :bound (m/? v)))))
  (testing "dfv hands off across fibers"
    (let [v (m/dfv)]
      (is (= [:sent :got]
             (m/? (m/join vector
                          (m/sp (m/? (m/sleep 20)) (v :got) :sent)
                          (m/sp (m/? v))))))))
  (testing "rdv"
    (let [r (m/rdv)]
      (is (= [nil :msg]
             (m/? (m/join vector (m/sp (m/? (r :msg))) (m/sp (m/? r)))))))))

(deftest cancellation
  (let [out    (promise)
        cancel ((m/sleep 5000 :never)
                (fn [_] (deliver out :completed))
                (fn [e] (deliver out (m/cancelled? e))))]
    (cancel)
    (is (true? (deref out)) "cancelling a sleep fails it with Cancelled")))
