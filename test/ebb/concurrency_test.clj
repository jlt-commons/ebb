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

;; --------------------------------------------------- notifications off-thread

(defn- offloaded
  "A flow emitting xs, raising every notification on a FRESH THREAD.

  That is what an `ap` over `via` does to whatever consumes it, and it is the
  window ebb-8nq.31 lived in: the notifier fires from the flow's own executor
  while the thread that called the consumer's `run` is still between
  `(flow n t)` and its first round. The protocol is respected -- one
  notification, then one transfer, then the next -- so a consumer that loses a
  flip has lost it to concurrency, not to a malformed flow."
  [xs]
  (fn [n t]
    (let [left (atom (seq xs))
          ping (fn [] (doto (Thread. (fn [] (if (seq @left) (n) (t))))
                        (.setDaemon true)
                        (.start))
                 nil)]
      (ping)
      (reify
        clojure.lang.IFn
        (invoke [_] nil)
        clojure.lang.IDeref
        (deref [_] (let [x (first @left)] (swap! left rest) (ping) x))))))

(deftest reduce-does-not-lose-a-notification-raised-off-thread
  ;; ebb-8nq.31. `(set! busy (not busy))` is a read and a write, so a
  ;; notification landing from another thread while the starting thread runs
  ;; its own first round loses one flip. Nothing throws: the reduction simply
  ;; goes idle while its input sits notified, so this can only be asserted on a
  ;; deadline.
  (dotimes [_ 20]
    (let [n 40
          p (promise)]
      ((m/reduce conj [] (offloaded (range n)))
       (fn [v] (deliver p [:ok v]))
       (fn [e] (deliver p [:err e])))
      (is (= [:ok (vec (range n))] (deref p timeout-ms [:err ::timeout]))))))

;; ------------------------------------------------------- concurrent reactors

(defn- delayed-reactor
  "`reactor-delayed`'s shape, tagged so a reactor that saw another's context
  would collect the wrong values rather than merely crashing."
  [tag]
  (m/reactor
    (let [r  (atom [])
          i1 (m/stream! (m/ap (m/? (m/sleep 2 [tag 1]))))
          i2 (m/stream! (m/ap (m/? (m/sleep 1 [tag 2]))))]
      (m/stream! (m/zip (partial swap! r conj) i1 i2))
      r)))

(deftest concurrent-reactors-do-not-share-a-context
  ;; ebb-8nq.30. `current` and `delayed` were plain global atoms, so a reactor
  ;; propagating on one fiber was in scope for every other fiber in the process.
  ;; An event raised elsewhere then took `event`'s in-context branch and read
  ;; `(.-current ps)` -- which the propagation nils on its way out -- as a null
  ;; `.-ranks`. Reactor.java keeps both in a ThreadLocal and never has the
  ;; problem.
  ;;
  ;; Each reactor here has two sleeps landing a millisecond apart, so several
  ;; are always mid-propagation at once. Every one must come back with its own
  ;; two values and nobody else's.
  (dotimes [_ 5]
    (let [n    8
          outs (mapv (fn [tag]
                       (let [p (promise)]
                         ((delayed-reactor tag)
                          (fn [r] (deliver p [:ok @r]))
                          (fn [e] (deliver p [:err (ex-message e)])))
                         p))
                     (range n))]
      (dotimes [tag n]
        (is (= [:ok [[tag 1] [tag 2]]] (deref (nth outs tag) timeout-ms [:err ::timeout])))))))

;; ------------------------------------------------- callbacks that do not block

(deftest a-cancel-cast-at-an-owner-fiber-is-never-lost
  ;; ebb-8nq.29 made every callback an `ap` hands out a `server/cast!` --
  ;; enqueue and return, which is what "must not block the calling thread"
  ;; asks for. Two things that a blocking `call!` had been covering up came out
  ;; of it, and this shape found both:
  ;;
  ;;   * a cast enqueued after the owner fiber took its last drain vanished --
  ;;     a `call!` at least hangs its caller, a cast has nobody waiting. So
  ;;     `ebb.impl.server` flags `drained` before that final drain and the
  ;;     sender runs anything it enqueues afterwards.
  ;;   * `ebb.impl.propagator` kept ONE global Context where Propagator.java
  ;;     has a ThreadLocal, so a propagation on the consumer's thread and one
  ;;     on the ap's owner fiber shared `cursor`. The second was told it was
  ;;     nested, did not drain `delayed`, and the process waiting there was
  ;;     never ticked.
  ;;
  ;; Both are silent: the reduce simply never completes. 9 runs in 300 before,
  ;; and only under load, so the count here is what it takes to see it.
  (let [bad (atom 0)]
    (dotimes [_ 400]
      (let [p (promise)]
        ((m/reduce (fn [_ x] x) nil (m/signal (m/ap (m/? m/never))))
         (fn [v] (deliver p [:ok v])) (fn [e] (deliver p [:err (ex-message e)])))
        (let [[tag v] (deref p timeout-ms [:err ::timeout])]
          (when-not (and (= :err tag) (not= ::timeout v)) (swap! bad inc)))))
    (is (zero? @bad) "every run must report Uninitialized rather than stall")))
