(ns ebb.missionary-issues-test
  "Ebb checked against missionary's issue tracker.

  Missionary's issues are a list of behaviours a port has to get right, and
  they come in three kinds. CLOSED issues are regressions: missionary fixed
  them, and ebb -- ported from a recent checkout -- should have the fix. OPEN
  issues are places where missionary is known to misbehave; ebb may inherit the
  bug, may be immune, or may have chosen differently. And some are open design
  questions upstream, where matching missionary is the point.

  Every test below names its issue and says which kind it is. A test that
  asserts ebb diverges from missionary says why, and that reason belongs in
  doc/conformance.md too."
  (:require [ebb.core :as m]
            [clojure.test :as t]))

(defn- settle
  "Run task to completion, returning [:ok v] or [:err e]. Never throws, so a
  test can assert on a failure without wrapping every call."
  ([task] (settle task 5000))
  ([task ms]
   (let [p (promise)]
     (task (fn [v] (deliver p [:ok v])) (fn [e] (deliver p [:err e])))
     (deref p ms [:err :TIMED-OUT]))))

;; ---------------------------------------------------------------- regressions
;; CLOSED upstream. These must keep working.

(t/deftest zip-terminates-on-the-shorter-input-74
  ;; #74: (m/zip vector (m/seed [1 2 3]) (m/seed (repeat nil))) looped forever
  ;; once 3 was consumed.
  (t/is (= [:ok [[1 nil] [2 nil] [3 nil]]]
          (settle (m/reduce conj [] (m/zip vector (m/seed [1 2 3])
                                           (m/seed (repeat nil))))))))

(t/deftest latest-supports-zero-inputs-126
  ;; #126: (m/latest f) with no inputs is a well defined continuous flow whose
  ;; state is always (f). It used to crash on transfer.
  (t/is (= [:ok [:nullary]]
          (settle (m/reduce conj [] (m/latest (fn [] :nullary)))))))

(t/deftest signal-reports-an-uninitialised-flow-125
  ;; #125: (m/signal (m/ap (m/? m/never))) leaked internal state and reported
  ;; nothing. It must fail instead.
  (let [[tag e] (settle (m/reduce (fn [_ x] x) nil (m/signal (m/ap (m/? m/never)))) 1000)]
    (t/is (= :err tag))
    (t/is (not= :TIMED-OUT e) "must report, not hang")
    (t/is (re-find #"(?i)uninitialized|uninitialised" (str (ex-message e))))))

(t/deftest sleep-spawned-from-a-sleep-termination-115
  ;; #115: a sleep process spawned in another sleep's termination callback ran
  ;; user code on a thread left in an interrupted state, and the reduce died
  ;; with InterruptedException.
  (let [[tag v] (settle (m/reduce (fn [n _] (Thread/sleep 0)
                                    (if (< n 20) (inc n) (reduced n)))
                          0 (m/ap (m/? (m/?> (m/seed (repeat (m/sleep 1))))))))]
    (t/is (= :ok tag))
    (t/is (= 20 v))))

;; ------------------------------------------------------ missionary bugs ebb lacks
;; OPEN upstream. Ebb is correct here; these pin that down.

(t/deftest ap-cancellation-reaches-child-branches-116
  ;; #116: cancelling this ap should terminate it. In missionary it does not.
  (let [events (atom [])
        ps ((m/ap (m/amb= 1 2 3)
              (try (m/? m/never)
                   (catch Throwable e (if (m/cancelled? e) (m/amb) (throw e)))))
            (fn [] (swap! events conj :step))
            (fn [] (swap! events conj :done)))]
    (Thread/sleep 50)
    (ps)
    (Thread/sleep 200)
    (t/is (= [:done] @events) "cancellation must propagate into every branch")))

(t/deftest mbx-post-returns-nil-85
  ;; #85: the docs say posting returns nil; missionary could return non-nil.
  (let [mbx (m/mbx)]
    (t/is (nil? (mbx :a)) "post with nobody waiting"))
  (let [mbx (m/mbx)]
    ((m/sp (m/? mbx)) (fn [_]) (fn [_]))
    (t/is (nil? (mbx :b)) "post with a waiter")))

(t/deftest ap-with-unbounded-parallelism-108
  ;; #108: (m/?> ##Inf ...) over many tasks. Ebb is correct and roughly linear
  ;; at this size; the reported knee is far above it. Kept small so the suite
  ;; stays fast -- ebb-8nq.25 tracks finding where it degrades.
  (let [n 400
        values (m/ap (let [task (m/?> ##Inf (m/seed (repeat n (m/via m/cpu :hi))))]
                       (m/? task)))]
    (t/is (= [:ok n] (settle (m/reduce (fn [acc v]
                                         (t/is (= :hi v))
                                         (inc acc)) 0 values)
                             20000)))))

;; --------------------------------------------------- deliberately as missionary
;; Open design questions upstream. Matching is the correct answer until they
;; are decided; if ebb ever diverges, doc/conformance.md gets a row.

(t/deftest sem-accepts-a-non-positive-count-111
  ;; #111 asks whether (m/sem 0) / (m/sem -1) should throw. Missionary allows
  ;; them and acquiring blocks. Ebb does the same.
  (doseq [n [0 -1]]
    (let [sem (m/sem n)
          p   (promise)]
      (sem (fn [_] (deliver p :acquired)) (fn [_] (deliver p :failed)))
      (t/is (= :blocked (deref p 200 :blocked))
            (str "(m/sem " n ") must construct, and acquiring must block")))))

(t/deftest cancelling-via-runs-the-bodys-handler-82
  ;; #82 reported that cancelling a `via` whose body catches Throwable still
  ;; SUCCEEDS, with the value the handler returned. Closed upstream, and it is
  ;; right: cancellation interrupts the thread, the body catches that and
  ;; returns normally, so the task succeeded. Cancellation is cooperative --
  ;; a body that swallows it is not cancelled. Pinned so it is a decision.
  (let [task (m/via m/blk (try (Thread/sleep 2000) :not-cancelled
                               (catch Throwable _ :handler-ran)))
        p    (promise)
        c    (task (fn [v] (deliver p [:success v])) (fn [e] (deliver p [:failure e])))]
    (Thread/sleep 50)
    (c)
    (t/is (= [:success :handler-ran] (deref p 3000 :TIMED-OUT)))))
