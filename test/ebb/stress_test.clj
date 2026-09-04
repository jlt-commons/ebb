(ns ebb.stress-test
  "ebb-8nq.32. The flow operators ported from missionary's .cljs impls, driven
  at the parallelism their .java counterparts synchronise for.

  Missionary has no analogue of this namespace. On the JVM every operator here
  wraps its entry points in `synchronized (ps)`, and on cljs there is one
  thread, so neither host can reach the interleaving these tests aim at: the
  input's notifier firing from the input's own executor while the consumer is
  between `(flow n t)` and its first round, or inside `transfer`. Ebb takes the
  .cljs logic onto a multi-carrier fiber pool, where that interleaving is the
  normal case.

  Two drivers produce it. `offloaded` raises every notification on a FRESH
  THREAD -- the minimal shape, one notification then one transfer, so a
  consumer that loses one has lost it to concurrency and not to a malformed
  flow. `parallel` is the real thing: an `ap` forking ##Inf ways over the
  blocking pool, which is missionary #108's shape and the one that first
  stalled `m/reduce` (ebb-8nq.31).

  A lost notification does not throw and does not hang in a stack -- the
  operator simply goes idle with its input still notified -- so every case here
  runs under a deadline and a timeout is a FAILURE, not a hang."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :as a]
            [ebb.core :as m]
            [jolt.fibers :as fib]))

(def ^:private timeout-ms 15000)

(defn- settle
  "Run task to completion under the deadline, as [:ok v] or [:err e]."
  [task]
  (let [p (promise)]
    (task (fn [v] (deliver p [:ok v])) (fn [e] (deliver p [:err e])))
    (deref p timeout-ms [:err ::timeout])))

(defn- in-parallel
  "Run (f i) for i in 0..n on n fresh threads and wait for all of them."
  [n f]
  (let [ps (mapv (fn [i]
                   (let [p (promise)]
                     (doto (Thread. (fn [] (try (f i) (finally (deliver p true)))))
                       (.setDaemon true)
                       (.start))
                     p))
                 (range n))]
    (run! #(deref % timeout-ms nil) ps)))

(defn- offloaded
  "A flow emitting xs, raising every notification on a fresh thread."
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

(defn- racing
  "A flow whose next notification comes from an executor that is ALREADY
  running, released the instant the consumer takes a value -- so it lands while
  the consumer is still finishing the round it took that value in.

  This is the window `offloaded` cannot reach. Spawning a thread costs more
  than the round has left to run, so a notification raised that way always
  arrives after the round is over, and a lost `busy` flip needs the two to
  overlap. Nothing is cancelled: `invoke` just shuts the producer down."
  [xs]
  (fn [n t]
    (let [left (atom (seq xs))
          ch   (a/chan 1024)]
      (fib/spawn
        (fn []
          (loop []
            (when (some? (a/<!! ch))
              (if (seq @left) (do (n) (recur)) (t))))))
      (a/offer! ch true)
      (reify
        clojure.lang.IFn
        (invoke [_] (a/close! ch) nil)
        clojure.lang.IDeref
        (deref [_] (let [x (first @left)]
                     (swap! left rest)
                     ;; wake the producer BEFORE returning: its notification
                     ;; and the rest of this round now run at the same time
                     (a/offer! ch true)
                     x))))))

(defn- parallel
  "n values produced on the blocking pool, all at once. Order is whatever the
  pool decides, so assertions on this compare bags, not sequences."
  [n]
  (m/ap (let [i (m/?> ##Inf (m/seed (range n)))]
          (m/? (m/via m/blk i)))))

(def ^:private n 40)
(def ^:private rounds 10)

;; ------------------------------------------------------------------- the loops

(deftest reduce-under-offloaded-notifications
  ;; ebb-8nq.31 itself, kept here beside its siblings.
  (dotimes [_ rounds]
    (is (= [:ok (vec (range n))]
           (settle (m/reduce conj [] (offloaded (range n))))))))

(deftest reduce-under-a-high-parallelism-ap
  (dotimes [_ 5]
    (is (= [:ok (reduce + (range n))]
           (settle (m/reduce + 0 (parallel n)))))))

(deftest reductions-under-offloaded-notifications
  (dotimes [_ rounds]
    (is (= [:ok (vec (reductions + 0 (range n)))]
           (settle (m/reduce conj [] (m/reductions + 0 (offloaded (range n)))))))))

(deftest relieve-under-offloaded-notifications
  ;; relieve COMBINES what the consumer did not take, so nothing is dropped:
  ;; whatever it emits must still add up to the whole input.
  (dotimes [_ rounds]
    (let [[tag v] (settle (m/reduce conj [] (m/relieve + (offloaded (range n)))))]
      (is (= :ok tag))
      (is (= (reduce + (range n)) (reduce + v))))))

(deftest buffer-under-offloaded-notifications
  ;; the case with two parties in the ring at once: `more` hands the notifier
  ;; out as soon as the ring is non-empty and keeps filling behind it.
  (doseq [capacity [1 2 8]]
    (testing (str "capacity " capacity)
      (dotimes [_ 5]
        (is (= [:ok (vec (range n))]
               (settle (m/reduce conj [] (m/buffer capacity (offloaded (range n)))))))))))

(deftest eduction-under-offloaded-notifications
  (dotimes [_ rounds]
    (is (= [:ok (vec (eduction (map inc) (filter odd?) (range n)))]
           (settle (m/reduce conj [] (m/eduction (map inc) (filter odd?)
                                                 (offloaded (range n)))))))))

(deftest eduction-under-a-high-parallelism-ap
  ;; `cat` makes the transducer emit more than it consumes, which is what
  ;; exercises the buffer the pressure counter guards.
  (dotimes [_ 5]
    (let [[tag v] (settle (m/reduce conj [] (m/eduction (map (fn [i] [i i])) cat
                                                        (parallel n))))]
      (is (= :ok tag))
      (is (= (frequencies (mapcat (fn [i] [i i]) (range n))) (frequencies v))))))

(deftest sample-under-a-changing-reference
  ;; two entry points at once: the sampler's notifier runs the loop while the
  ;; continuous input's `dirty` marks slots from three other threads.
  (dotimes [_ 5]
    (let [a   (atom 0)
          out (promise)]
      ((m/reduce conj [] (m/sample vector (m/watch a) (offloaded (range n))))
       (fn [v] (deliver out [:ok v])) (fn [e] (deliver out [:err e])))
      (in-parallel 3 (fn [_] (dotimes [_ 50] (swap! a inc))))
      (let [[tag v] (deref out timeout-ms [:err ::timeout])]
        (is (= :ok tag) (str v))
        (is (= (vec (range n)) (mapv second v)))
        (is (every? integer? (map first v)))))))

(deftest group-by-under-offloaded-notifications
  ;; each group is a flow of its own, consumed by its own branch of an ap, so
  ;; the table is being probed by the loop while `group` and `cancel` mutate it
  ;; from the branches.
  (dotimes [_ 5]
    (is (= [:ok (set (map (fn [[k vs]] [k (vec vs)])
                          (group-by #(mod % 4) (range n))))]
           (settle (m/reduce conj #{}
                     (m/ap (let [[k gf] (m/?> ##Inf (m/group-by #(mod % 4)
                                                                (offloaded (range n))))]
                             [k (m/? (m/reduce conj [] gf))]))))))))

;; -------------------------------------------------------- the readiness cells

(deftest watch-does-not-lose-the-last-change
  ;; Watch has no loop -- it has one cell doubling as the readiness flag, read
  ;; and written by whichever thread swapped the reference. Two changes landing
  ;; at once must still leave exactly one notification outstanding, and the
  ;; change made once they are all done must reach the consumer.
  ;;
  ;; What is NOT asserted is that the values arrive in the order the reference
  ;; took them. `add-watch` hands the callback the new state, but two callbacks
  ;; racing can reach the process in either order, so the consumer can see a
  ;; later state before an earlier one. That is true of missionary on the JVM
  ;; too -- hence the sentinel, written by this thread once the writers are
  ;; joined, rather than a highest-value-wins finish.
  (dotimes [_ 5]
    (let [a   (atom 0)
          out (promise)]
      ((m/reduce (fn [acc x] (if (= x ::end) (reduced (conj acc x)) (conj acc x)))
                 [] (m/watch a))
       (fn [v] (deliver out [:ok v])) (fn [e] (deliver out [:err e])))
      (in-parallel 4 (fn [_] (dotimes [_ 50] (swap! a inc))))
      (reset! a ::end)
      (let [[tag v] (deref out timeout-ms [:err ::timeout])]
        (is (= :ok tag) (str v))
        (is (= ::end (peek v)))
        ;; a lost flip shows up as the readiness sentinel -- the Process
        ;; itself -- being transferred as though it were a value
        (is (every? #(or (integer? %) (= ::end %)) v))
        (is (apply distinct? v))))))

(deftest observe-delivers-every-accepted-event-once
  ;; Observe's producer is whatever the user's subject hands the emit function
  ;; to, so several threads can be inside it at once. An emit that finds the
  ;; consumer busy is REFUSED -- missionary's cljs behaviour, see
  ;; doc/conformance.md -- and one that is not refused owes the consumer
  ;; exactly one delivery. Losing the arbitration shows up as a value delivered
  ;; twice, or as the process itself arriving as a value.
  (dotimes [_ 5]
    (let [emit-p   (promise)
          accepted (atom [])
          out      (promise)]
      ((m/reduce conj [] (m/eduction (take-while #(not= ::end %))
                                     (m/observe (fn [emit] (deliver emit-p emit) (fn [])))))
       (fn [v] (deliver out [:ok v])) (fn [e] (deliver out [:err e])))
      (let [emit (deref emit-p timeout-ms nil)]
        (is (some? emit))
        (in-parallel 6 (fn [t]
                         (dotimes [i 40]
                           (try (emit [t i])
                                (swap! accepted conj [t i])
                                (catch Throwable _ nil)))))
        ;; the sentinel has to land, so keep offering it until it is accepted
        (loop [tries 0]
          (when (and (< tries 100000)
                     (not (try (emit ::end) true (catch Throwable _ false))))
            (recur (inc tries))))
        (let [[tag v] (deref out timeout-ms [:err ::timeout])]
          (is (= :ok tag) (str v))
          (is (= (count v) (count (distinct v))) "no event delivered twice")
          (is (every? vector? v) "no readiness sentinel leaked as a value")
          ;; every emit that was not refused reached the consumer
          (is (= (set @accepted) (set v))))))))

;; ------------------------------------------------- the tight notifier window

(deftest loops-survive-a-notification-raised-mid-round
  ;; `racing` overlaps the input's notification with the consumer's own round,
  ;; which is where `(set! busy (not busy))` loses a flip: the round's trailing
  ;; toggle and the notifier's leading toggle are one read-modify-write each on
  ;; the same field. A lost flip leaves the loop with nobody in it and the state
  ;; reading idle, so the operator stops pulling while its input sits notified.
  (doseq [[label flow expected]
          [["reduce"     #(m/reduce conj [] (racing (range n)))          (vec (range n))]
           ["reductions" #(m/reduce conj [] (m/reductions + 0 (racing (range n))))
                                                                         (vec (reductions + 0 (range n)))]
           ["buffer"     #(m/reduce conj [] (m/buffer 4 (racing (range n)))) (vec (range n))]
           ["eduction"   #(m/reduce conj [] (m/eduction (map inc) (racing (range n))))
                                                                         (vec (map inc (range n)))]]]
    (testing label
      (dotimes [_ rounds]
        (is (= [:ok expected] (settle (flow))))))))

;; ------------------------------------------------------- combining operators

(deftest zip-of-two-staggered-aps
  ;; ebb-8nq.35. `pending` counts the inputs that still owe a notification, and
  ;; whoever brings it to zero owns the round. It was a plain field, so two
  ;; notifications landing at once lost a decrement and it never reached zero:
  ;; the zip went idle with both inputs notified and nobody holding the round.
  ;; 33 runs in 200 before the atom, and silent -- no throw, no stack.
  ;;
  ;; The stagger matters. Two inputs notifying at the SAME moment stalled 2 in
  ;; 200; a millisecond apart, 33. The window is one notifier's read-modify-write
  ;; straddling the other's, and arriving together mostly misses it.
  (dotimes [_ 60]
    (is (= [:ok [[1 2]]]
           (settle (m/reduce conj [] (m/zip vector
                                            (m/ap (m/? (m/sleep 2 1)))
                                            (m/ap (m/? (m/sleep 1 2))))))))))

(deftest latest-over-two-references-advanced-at-once
  ;; the same defect in the same shape, in the other combining operator:
  ;; Latest.java drives its `sync` word with compareAndSet and ebb had a plain
  ;; read-then-set!, so two notifiers both found `idle` and both took ownership,
  ;; or one overwrote the other's `siblings` link and that notification was
  ;; never consumed. 15 runs in 40 before, and each stall cost the full deadline
  ;; -- this test took 30s and now takes 76ms.
  ;;
  ;; `m/latest` needs CONTINUOUS inputs, which is why these are watches and not
  ;; the aps above. Two references advanced from two threads put the two
  ;; notifications on two executors, which is the window the CAS covers.
  (dotimes [_ 40]
    (let [a (atom 0) b (atom 0) target 60
          out (promise)]
      ((m/reduce (fn [_ x] (if (= x [target target]) (reduced x) x)) nil
                 (m/latest vector (m/watch a) (m/watch b)))
       (fn [v] (deliver out [:ok v])) (fn [e] (deliver out [:err e])))
      (in-parallel 2 (fn [i] (dotimes [_ target] (swap! (if (zero? i) a b) inc))))
      (is (= [:ok [target target]] (deref out timeout-ms [:err ::timeout]))))))
