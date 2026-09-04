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

;; ------------------------------------------------- failure ends the output

(def ^:private boom (ex-info "boom" {}))

(defn- crash-flow
  "Notifies once; the transfer throws. The error reaches the body through the
  fork's deref rather than being raised by the body itself, and the flow never
  terminates -- which is the case that separates the two rules below."
  [n _]
  (n)
  (reify clojure.lang.IFn (invoke [_] nil)
         clojure.lang.IDeref (deref [_] (throw boom))))

(defn- drain
  "Consume flow WITHOUT cancelling it, logging every value, error and the
  terminator. `m/reduce` cancels its input the moment a transfer throws, which
  is why nothing in the suite noticed ebb-8nq.38 -- the ap was being killed from
  outside and the difference never showed."
  [flow]
  (let [log (atom []) pending (atom 0)
        ps  (flow #(swap! pending inc) #(swap! log conj :done))]
    (dotimes [_ 14]
      (when (pos? @pending)
        (swap! pending dec)
        (swap! log conj (try [:v (deref ps)] (catch Throwable e [:e (ex-message e)]))))
      (Thread/sleep 15))
    @log))

(deftest an-error-ends-the-output-and-discards-branches-in-flight
  ;; ebb-8nq.38. Handing an error downstream ends what the ap will deliver:
  ;; branches still in flight are discarded, not delivered after the failure.
  ;; Ebb used to deliver the error and carry on, so a consumer that did not
  ;; cancel saw values arrive AFTER :done.
  ;;
  ;; Every expectation here was taken from missionary b.47 -- java impls
  ;; compiled out of the checkout, the same programs run against them -- and
  ;; not reasoned about. The first attempt at this reasoned instead, concluded
  ;; "an uncaught branch error terminates the ap", and broke missionary's own
  ;; `?>-cancel-with-crash`.
  (testing "a branch still queued behind the failure is dropped"
    (is (= [[:v 1] :done [:e "boom"]] (drain (m/ap (m/amb 1 (throw boom) 3)))))
    (is (= [[:v 3] :done [:e "boom"]] (drain (m/ap (m/amb= 1 (throw boom) 3))))))
  (testing "a fork with values left to give is dropped too"
    (is (= [[:v 1] :done [:e "boom"]]
           (drain (m/ap (let [x (m/?> (m/seed [1 2 3]))] (if (= x 2) (throw boom) x))))))
    (is (= [[:v 1] :done [:e "boom"]]
           (drain (m/ap (let [x (m/?> (m/seed [1 2]))] (m/amb x (throw boom))))))))
  (testing "nothing left to discard: unchanged, and already correct"
    (is (= [[:v 1] :done [:e "boom"]] (drain (m/ap (m/amb 1 (throw boom))))))
    (is (= [:done [:e "boom"]]        (drain (m/ap (throw boom)))))
    (is (= [[:v 1] :done [:e "boom"]] (drain (m/ap (m/?> (m/ap (m/amb 1 (throw boom)))))))))
  (testing "failing does NOT terminate an ap whose input has not"
    ;; the part that is easy to get wrong, and what missionary's own
    ;; ?>-cancel-with-crash asserts: the error goes out and the ap then waits.
    (is (= [[:e "boom"]] (drain (m/ap (m/?> crash-flow)))))))
