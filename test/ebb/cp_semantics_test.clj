;; End-to-end cp semantics. missionary's cp_test drives the protocol event by
;; event; these check the incremental behaviour that makes cp worth having.
(ns ebb.cp-semantics-test
  (:require [clojure.test :refer [deftest is testing]]
            [ebb.core :as m]))

(defn- sample-once
  "Spawn the continuous flow, take one sample, dispose."
  [flow]
  (let [out (promise)
        it  (flow (fn [] (deliver out :ready)) (fn [] nil))]
    (deref out 2000 nil)
    ;; dispose even when the sample throws, or the process (and its owner
    ;; fiber) outlives the test
    (try @it (finally (it)))))

(deftest a-constant-is-a-continuous-flow
  (is (= 1 (sample-once (m/cp 1))))
  (is (= 3 (sample-once (m/cp (+ 1 2))))))

(deftest a-throw-surfaces-on-sample
  (is (= "boom" (try (sample-once (m/cp (throw (ex-info "boom" {})))) :no-throw
                     (catch Throwable e (ex-message e))))))

(deftest switch-reads-the-current-value
  (let [!x (atom 41)]
    (is (= 42 (sample-once (m/cp (inc (m/?< (m/watch !x)))))))))

(deftest recomputes-only-below-the-change
  (testing "a change re-runs the continuation from that switch, not the whole body"
    (let [!x    (atom 1)
          above (atom 0)
          below (atom 0)
          out   (atom [])
          done  (promise)
          it    ((m/cp (swap! above inc)
                       (let [x (m/?< (m/watch !x))]
                         (swap! below inc)
                         (* x 10)))
                 (fn [] (deliver done :ready)) (fn [] nil))]
      (deref done 2000 nil)
      (swap! out conj @it)
      (reset! !x 2) (swap! out conj @it)
      (reset! !x 3) (swap! out conj @it)
      (it)
      (is (= [10 20 30] @out))
      (is (= 1 @above) "everything above the switch runs once")
      (is (= 3 @below) "everything below it runs per change"))))

(deftest a-duplicate-value-does-not-recompute
  (testing "re-emitting the same value leaves the continuation valid"
    (let [!x   (atom 1)
          runs (atom 0)
          done (promise)
          it   ((m/cp (let [x (m/?< (m/watch !x))] (swap! runs inc) x))
                (fn [] (deliver done :ready)) (fn [] nil))]
      (deref done 2000 nil)
      (is (= 1 @it))
      (reset! !x 1)                       ; same value
      (is (= 1 @it))
      (is (= 1 @runs) "no recompute for an unchanged input")
      (reset! !x 2)
      (is (= 2 @it))
      (is (= 2 @runs))
      (it))))

(deftest nested-switch
  (let [!outer (atom 10)
        !inner (atom 1)]
    (is (= 11 (sample-once (m/cp (+ (m/?< (m/watch !outer))
                                    (m/?< (m/watch !inner)))))))))
