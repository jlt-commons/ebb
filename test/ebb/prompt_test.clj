;; Delimited continuations (ebb.impl.prompt). No missionary counterpart -- the
;; JVM has no continuations, so this is ebb's own spec.
(ns ebb.prompt-test
  (:require [clojure.test :refer [deftest is testing]]
            [ebb.impl.prompt :as p]
            [jolt.fibers :as fib]
            [clojure.core.async :as a]))

(deftest suspend-and-resume-in-order
  (let [seen (atom [])
        pr   (p/prompt)]
    (loop [r (p/start pr (fn [] (doseq [x [:a :b :c]] (p/suspend pr x)) :done))]
      (if (p/value? r)
        (is (= :done (p/payload r)))
        (do (swap! seen conj (p/payload r))
            (recur (p/resume pr (p/continuation r) nil)))))
    (is (= [:a :b :c] @seen))))

(deftest continuation-is-delimited
  (testing "resuming must not re-run the driver -- the bug that motivates the design"
    (let [driver-runs (atom 0)
          pr          (p/prompt)
          r           (p/start pr (fn [] (let [x (p/suspend pr :fork)] [:branch x])))]
      (swap! driver-runs inc)
      (p/resume pr (p/continuation r) 1)
      (p/resume pr (p/continuation r) 2)
      (is (= 1 @driver-runs)))))

(deftest continuation-is-multi-shot
  (testing "each resume runs its own branch and answers its own value"
    (let [effects (atom [])
          pr      (p/prompt)
          r       (p/start pr (fn [] (let [x (p/suspend pr :fork)]
                                       (swap! effects conj (* x 10))
                                       [:branch x])))
          k       (p/continuation r)]
      (is (= [:branch 1] (p/payload (p/resume pr k 1))))
      (is (= [:branch 2] (p/payload (p/resume pr k 2))))
      (is (= [:branch 3] (p/payload (p/resume pr k 3))))
      (is (= [10 20 30] @effects)))))

(deftest capture-from-a-nested-call
  (testing "suspension works at any call depth, not only lexically in the body"
    (let [pr (p/prompt)]
      (letfn [(deep [x] (p/suspend pr x))]
        (let [r (p/start pr (fn [] (inc (deep :tag))))]
          (is (p/suspend? r))
          (is (= :tag (p/payload r)))
          (is (= 6 (p/payload (p/resume pr (p/continuation r) 5)))))))))

(deftest multi-shot-on-a-fiber-with-parks
  (testing "a park inside each resumed extent is fine"
    (let [outcome
          (fib/join
            (fib/spawn
              (fn []
                (try
                  (let [out (atom [])
                        pr  (p/prompt)
                        r   (p/start pr (fn [] (let [x (p/suspend pr :fork)]
                                                 (a/<!! (a/timeout 2))
                                                 (swap! out conj x) :ok)))
                        k   (p/continuation r)]
                    (p/resume pr k :one) (p/resume pr k :two) (p/resume pr k :three)
                    @out)
                  (catch Throwable e
                    [:threw (str (type e)) (ex-message e) (ex-data e)])))))]
      (is (= [:one :two :three] outcome)))))

(deftest resuming-on-another-fiber-throws
  (testing "a cross-fiber resume HANGS jolt with no error, so it must be refused"
    (let [pr (p/prompt)
          r  (p/start pr (fn [] (p/suspend pr :fork)))
          k  (p/continuation r)
          outcome (fib/join (fib/spawn (fn []
                                         (try (p/resume pr k 1) :no-throw
                                              (catch Throwable e (:type (ex-data e)))))))]
      (is (= :ebb.impl.prompt/wrong-fiber outcome)))))

(deftest forking-inside-a-finally-is-refused
  (testing "jolt compiles finally to a dynamic-wind after-thunk: N+1 runs for N branches"
    (let [pr (p/prompt)
          r  (p/start pr (fn [] (try (p/suspend pr :fork) (finally nil))))]
      ;; reported as an error result, not a throw: a throw inside the extent is
      ;; routed through the prompt so it reaches the party that entered it
      (is (p/error? r))
      (is (= ::p/fork-in-finally (:type (ex-data (p/payload r)))))))
  (testing "catch has no winder and is allowed"
    (let [pr (p/prompt)
          r  (p/start pr (fn [] (try (p/suspend pr :fork) (catch Throwable _ :caught))))]
      (is (p/suspend? r))
      (is (= :resumed (p/payload (p/resume pr (p/continuation r) :resumed))))))
  (testing "a finally OUTSIDE the prompt does not count"
    (let [pr (p/prompt)]
      (try (let [r (p/start pr (fn [] (p/suspend pr :fork)))]
             (is (p/suspend? r)))
           (finally nil)))))

(deftest affinity-is-thread-and-fiber-not-fiber-alone
  (testing "current-fiber is nil OFF a fiber, so a fiber-only check passes vacuously"
    (let [pr (p/prompt)
          r  (p/start pr (fn [] (p/suspend pr :fork)))
          k  (p/continuation r)
          ;; a plain OS thread, no fiber -- fiber identity is nil on both sides
          out (promise)
          th  (Thread. (fn [] (deliver out (try (p/resume pr k 1) :no-throw
                                                (catch Throwable e (:type (ex-data e)))))))]
      (.start th)
      (.join th)
      (is (= ::p/wrong-fiber (deref out 5000 :timeout))
          "resuming from another thread must be refused even when neither side is a fiber"))))
