(ns ebb.concurrency-test
  "Ebb-specific tests for ADR-001. Missionary has no analogue: on the JVM these
  impls are already CAS-driven, and on cljs there is one thread. Ebb takes the
  .cljs logic onto a multi-carrier fiber pool, which is where it can go wrong.

  Every case here is a deadlock hunt, so each runs under a deadline -- a broken
  impl must FAIL the suite, not hang it."
  (:require [clojure.test :refer [deftest is testing]]
            [ebb.core :as m]
            [jolt.fibers :as fib]))

(def ^:private timeout-ms 5000)

(defn- within
  "Run (f) on a fiber and return its value, or ::timeout if it outlives the
  deadline. A deadlocked impl parks its fibers forever, so this is the only way
  to report one as a failure."
  [f]
  (let [out (promise)]
    (fib/spawn (fn [] (try (deliver out [:ok (f)])
                           (catch Throwable e (deliver out [:err e])))))
    (let [r (deref out timeout-ms nil)]
      (cond (nil? r)          ::timeout
            (= :err (first r)) (throw (second r))
            :else              (second r)))))

(defn- dfv-handoff []
  (let [v (m/dfv)]
    (m/? (m/join vector
                 (m/sp (m/? (m/sleep 5)) (v :got) :sent)
                 (m/sp (m/? v))))))

(defn- rdv-handoff []
  (let [r (m/rdv)]
    (m/? (m/join vector (m/sp (m/? (r :msg))) (m/sp (m/? r))))))

(deftest dfv-survives-concurrent-fibers
  (is (= [:sent :got] (within dfv-handoff))))

(deftest rdv-survives-concurrent-fibers
  (is (= [nil :msg] (within rdv-handoff))))

(deftest ports-interleave-without-deadlocking
  (testing "the spike's failure: a dfv handoff followed by an rdv handoff"
    (is (= [:sent :got] (within dfv-handoff)))
    (is (= [nil :msg]   (within rdv-handoff)))))

(deftest ports-survive-repetition
  (testing "enough rounds to lose a CAS race on a 4-carrier pool"
    (dotimes [_ 10]
      (is (= [:sent :got] (within dfv-handoff)))
      (is (= [nil :msg]   (within rdv-handoff))))))

(deftest many-waiters-on-one-dfv
  (testing "concurrent derefs all resume when the variable is assigned"
    (is (= [:v :v :v :v :v :v]
           (within (fn []
                     (let [v (m/dfv)]
                       (m/? (m/join vector
                                    (m/sp (m/? (m/sleep 5)) (v :v) (m/? v))
                                    (m/sp (m/? v)) (m/sp (m/? v))
                                    (m/sp (m/? v)) (m/sp (m/? v))
                                    (m/sp (m/? v)))))))))))

;; --- ADR-001 rule 3: no task invocation inside a locked region --------------

(deftest no-park-under-a-lazy-seq-lock
  (testing "a combinator must not spawn its children inside a lazy seq"
    ;; Realizing a lazy seq holds a counted lock, and jolt refuses to park a
    ;; fiber whose carrier holds one. join spawns children that can park, so a
    ;; (dorun (map-indexed ...)) here fails with
    ;;   "a fiber cannot leave the CPU while its carrier holds a counted lock".
    ;; Six children, each parking, from a fiber -- the shape that caught it.
    (is (= [1 2 3 4 5 6]
           (within (fn []
                     (m/? (apply m/join vector
                                 (map (fn [n] (m/sp (m/? (m/sleep 2 n))))
                                      (range 1 7))))))))))

;; --- the readiness handshake (ebb.impl.affine) ------------------------------

(deftest sp-has-run-to-its-first-park-before-the-task-call-returns
  (testing "missionary's start ordering, preserved by the handshake"
    (let [log (atom [])
          t   (m/sp (swap! log conj :body-start)
                    (m/? (m/sleep 20))
                    (swap! log conj :after-park))]
      (t (fn [_] (swap! log conj :succeeded)) (fn [_] nil))
      (swap! log conj :task-call-returned)
      ;; the body reached its park before the call returned; it had not resumed
      (is (= [:body-start :task-call-returned] @log))
      (m/? (m/sleep 60))
      (is (= [:body-start :task-call-returned :after-park :succeeded] @log)))))

(deftest sp-that-never-parks-completes-before-the-task-call-returns
  (let [log (atom [])]
    ((m/sp (swap! log conj :body) :v)
     (fn [_] (swap! log conj :succeeded)) (fn [_] nil))
    (swap! log conj :returned)
    (is (= [:body :succeeded :returned] @log))))
