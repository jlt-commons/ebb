;; End-to-end ap semantics. missionary's ap_test drives the flow protocol event
;; by event through lolcat; these check that the values actually come out right,
;; which is what pins multi-shot branch independence -- the property the whole
;; prompt design exists for.
(ns ebb.ap-semantics-test
  (:require [clojure.test :refer [deftest is testing]]
            [ebb.core :as m]))

(defn- values [flow] (m/? (m/reduce conj [] flow)))

(deftest fork-runs-the-continuation-once-per-value
  (is (= [1 4 9] (values (m/ap (let [x (m/?> (m/seed [1 2 3]))] (* x x)))))))

(deftest branches-are-independent
  (testing "each branch gets its own copy of everything after the fork"
    (is (= [[1 :a] [2 :a] [3 :a]]
           (values (m/ap (let [x (m/?> (m/seed [1 2 3]))
                               ;; bound per branch, must not leak between them
                               tag :a]
                           [x tag])))))))

(deftest a-branch-can-park
  (testing "? inside a branch, after the fork"
    (is (= [10 20 30]
           (values (m/ap (let [x (m/?> (m/seed [1 2 3]))]
                           (m/? (m/sleep 2 (* x 10))))))))))

(deftest nested-forks-multiply
  (is (= [11 21 12 22]
         (values (m/ap (+ (m/?> (m/seed [1 2]))
                          (m/?> (m/seed [10 20]))))))))

(deftest amb-produces-each-form-in-turn
  (is (= [:a :b :c] (values (m/ap (m/amb :a :b :c)))))
  (is (= [1 2 3 4]  (values (m/ap (m/amb 1 2 (m/amb 3 4)))))))

(deftest empty-fork-produces-nothing
  (is (= [] (values (m/ap (m/?> (m/seed []))))))
  (is (= [] (values (m/ap (m/amb))))))

(deftest an-error-in-a-branch-surfaces-downstream
  (let [boom (ex-info "boom" {})]
    (is (= "boom"
           (try (values (m/ap (let [x (m/?> (m/seed [1 2 3]))]
                                (if (= 2 x) (throw boom) x))))
                (catch Throwable e (ex-message e)))))))

(deftest parallelism-does-not-change-the-value-set
  (testing "par only governs how many branches are in flight"
    (is (= #{1 4 9}
           (set (values (m/ap (let [x (m/?> 3 (m/seed [1 2 3]))] (* x x)))))))))
