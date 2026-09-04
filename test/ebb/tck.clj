;; Derived from missionary test/missionary/tck.cljc (EPL 2.0).
;;
;; task-spec, flow-spec, deftask and defflow are verbatim -- they are pure
;; protocol state machines. `check!`, the driver underneath them, is rewritten:
;; missionary's uses LinkedBlockingQueue, ArrayDeque, AtomicReference and a
;; thread per timer. Ebb keeps the same two-queue discipline (events raised on
;; the driving thread run depth-first, events from elsewhere are taken in order)
;; over a vector and a channel, and schedules timers on the runtime's deadline
;; heap instead of spawning threads.
(ns ebb.tck
  (:require [clojure.test :as t]
            [clojure.core.async :as a]
            [ebb.impl.util :as u]))

(defmacro if-try [[binding attempt] success failure]
  `(let [[cond# ~binding]
         (try [true ~attempt]
              (catch Throwable e# [false e#]))]
     (if cond# ~success ~failure)))

(defmacro is [& args] (cons 'clojure.test/is args))

(defn check! [b x]
  (let [driver (Thread/currentThread)
        squeue (atom [])                   ; raised on the driving thread
        aqueue (a/chan 1024)               ; raised from anywhere else
        timers (atom [])                   ; [deadline event], lowest priority
        live   (atom true)
        actor  (fn actor
                 ([x]
                  (if (identical? driver (Thread/currentThread))
                    (swap! squeue conj x)
                    (a/offer! aqueue x))
                  nil)
                 ([x d]
                  (swap! timers conj [(+ (System/currentTimeMillis) d) x])
                  nil))]
    (letfn [(due []
              (let [now (System/currentTimeMillis)
                    [ready pending] [(filter #(<= (first %) now) @timers)
                                     (remove #(<= (first %) now) @timers)]]
                (when-some [t (first ready)]
                  (reset! timers (vec (concat (rest ready) pending)))
                  (second t))))
            (next-event []
              (let [q @squeue]
                (if (seq q)
                  (do (reset! squeue (subvec q 1)) (first q))
                  ;; A timeout means NOTHING ELSE HAPPENED, so it is the lowest
                  ;; priority event, not just another one racing in the queue.
                  ;;
                  ;; Missionary can be laxer: its processes run on the driving
                  ;; thread, so a synchronous completion lands in squeue and
                  ;; beats a 0ms timeout by construction. Ebb runs ap and cp
                  ;; bodies on an owner fiber (ADR-001), so the same completion
                  ;; arrives from another thread even though the driver was
                  ;; blocked waiting for it -- real-time ordering is preserved,
                  ;; but the squeue/aqueue split no longer reflects it. Draining
                  ;; the real events first restores the intent.
                  (if-some [v (a/poll! aqueue)]
                    v
                    (or (due)
                        ;; Wait exactly until the nearest deadline. Polling on a
                        ;; short tick instead would create a timeout channel per
                        ;; tick, and with 200-odd tests that floods the runtime's
                        ;; single deadline heap -- which is the same heap these
                        ;; tests measure against, so the harness would perturb
                        ;; what it is timing. (It did: the suite went flaky.)
                        (let [now  (System/currentTimeMillis)
                              next (some->> (seq @timers) (map first) (apply min))
                              wait (if next (max 0 (- next now)) 30000)
                              [v _] (a/alts!! [aqueue (a/timeout wait)])]
                          ;; alts!! picks at RANDOM among ready ports, so when a
                          ;; completion and the deadline are both ready it can
                          ;; hand back the deadline and lose the completion that
                          ;; is sitting in the queue. Re-poll before accepting a
                          ;; timeout: the rule is that a timeout only wins when
                          ;; nothing else happened.
                          (or v (a/poll! aqueue) (due) ::tick)))))))]
      (actor x)
      (try
        (loop [b b]
          (let [e (next-event)]
            (if (= ::tick e)
              (recur b)
              (when-some [b (b actor e)] (recur b)))))
        (catch Throwable e (t/is (throw e))))
      (reset! live false)
      (a/close! aqueue)
      (t/is (= nil nil)))))

(defn task-spec [opts]
  (let [[status predicate]
        (or (and (not (contains? opts :failure)) (find opts :success))
            (and (not (contains? opts :success)) (find opts :failure))
            (throw (ex-info "Either :success or :failure must be defined." {})))]
    (letfn [(booting [self task]
              (when-some [c (:cancel opts)] (self :cancel c))
              (self :timeout (:timeout opts 0))
              (waiting (task (fn [x] (self [:success x]))
                             (fn [e] (self [:failure e])))))
            (waiting [cancel]
              (fn continue [self event]
                (case event
                  :cancel (do (cancel) continue)
                  :timeout (throw (ex-info "Task timed out." {}))
                  (let [[s r] event]
                    (if (= status s)
                      (when-not (predicate r)
                        (throw (ex-info "Task result did not satisfy given predicate." {})))
                      (throw (ex-info "Task terminated with unexpected status." {})))
                    (cancel) (self :done) exiting))))
            (exiting [_ event]
              (case event
                :done nil
                (:cancel :timeout) exiting
                (throw (ex-info "Task called continuation more than once." {}))))]
      booting)))

(defmacro deftask [name opts task]
  `(t/deftest ~name (check! (task-spec ~opts) ~task)))

(defn flow-spec [opts]
  (letfn [(booting [self flow]
            (when-some [c (:cancel opts)] (self :cancel c))
            (self :timeout (:timeout opts 0))
            (flowing (flow #(self :ready) #(self :terminated))
                     (seq (:results opts)) (:failure opts)))
          (flowing [iterator results failure]
            (fn continue [self event]
              (case event
                :cancel (do (iterator) continue)
                :timeout (throw (ex-info "Flow timed out." {}))
                :ready (if-try [x @iterator]
                         (if-some [[p & results] results]
                           (if (p x)
                             (flowing iterator results failure)
                             (throw (ex-info "Flow value did not satisfy given predicate." {:value x})))
                           (throw (ex-info "Flow produced an unexpected value." {:value x})))
                         (if (nil? results)
                           (if-some [p failure]
                             (if (p x)
                               (flowing iterator nil nil)
                               (throw (ex-info "Flow error did not satisfy given predicate." {:error x})))
                             (throw (ex-info "Flow failed althought it was expected to terminate." {:error x})))
                           (throw (ex-info "Flow failed althought it was expected to produce a value." {:error x}))))
                :terminated (if (nil? results)
                              (if (nil? failure)
                                (do (iterator) (self :done) exiting)
                                (throw (ex-info "Flow terminated althought it was expected to fail." {})))
                              (throw (ex-info "Flow terminated althought it was expected to produce more values." {}))))))
          (exiting [_ event]
            (case event
              :done nil
              (:cancel :timeout) exiting
              :ready (throw (ex-info "Flow called notifier after terminator." {}))
              :terminated (throw (ex-info "Flow called terminator more than once." {}))))]
    booting))

(defmacro defflow [name opts flow]
  `(t/deftest ~name (check! (flow-spec ~opts) ~flow)))
