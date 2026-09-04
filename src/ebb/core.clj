(ns ebb.core
  "Ebb — a port of missionary's task/flow protocol to jolt.

  A TASK is (fn [success failure] cancel).
  A FLOW is (fn [notifier terminator] iterator), where the iterator derefs to
  transfer a value and is called with no args to cancel.

  Where missionary needs cloroutine to fake a resumable stack on the JVM, jolt
  parks a real one: `?` works from any function, not only lexically inside `sp`."
  (:refer-clojure :exclude [reduce])
  (:require [ebb.impl.util :as u]
            [ebb.impl.dataflow :as dataflow]
            [ebb.impl.rendezvous :as rendezvous]
            [ebb.impl.semaphore :as semaphore]
            [ebb.impl.mailbox :as mailbox]
            [ebb.impl.race-join :as race-join]
            [ebb.impl.ambiguous :as ambiguous]
            [ebb.impl.continuous :as continuous]
            [ebb.impl.zip :as zip-impl]
            [ebb.impl.latest :as latest]
            [ebb.impl.sample :as sample]
            [ebb.impl.relieve :as relieve-impl]
            [ebb.impl.buffer :as buffer-impl]
            [ebb.impl.eduction :as eduction-impl]
            [ebb.impl.reductions :as reductions-impl]
            [ebb.impl.group-by :as group-by-impl]
            [ebb.impl.observe :as observe-impl]
            [ebb.impl.store :as store-impl]
            [ebb.impl.watch :as watch-impl]
            [ebb.impl.propagator :as prop]
            [ebb.impl.reactor :as reactor-impl]
            [ebb.impl.affine :as affine]
            [ebb.impl.sleep :as sleep]
            [ebb.impl.seed :as seed]
            [ebb.impl.reduce :as reduce-impl]
            [ebb.impl.thunk :as thunk]
            [ebb.impl.pub :as pub]
            [ebb.impl.sub :as sub]
            [ebb.rs :as rs]
            [jolt.fibers :as fib]))

;; ---------------------------------------------------------------- cancellation

(def cancelled u/cancelled)
(def cancelled? u/cancelled?)

;; ----------------------------------------------------------------------- tasks

(defn sleep
  "Task completing with x (nil if absent) after d milliseconds."
  ([d] (sleep d nil))
  ([d x] (fn [s f] (sleep/run d x s f))))

(defn- cps-call [t s f]
  (try (s (t)) (catch Throwable e (f e))))

(defn attempt
  "Task completing with a thunk that replays the given task's outcome: calling
  it returns the value or rethrows the error."
  [task]
  (fn [s _] (task (fn [x] (s (fn [] x))) (fn [e] (s (fn [] (throw e)))))))

(defn absolve
  "Task running the given task and calling its result as a thunk."
  [task]
  (fn [s f] (task (fn [t] (cps-call t s f)) f)))

(defn compel
  "A task that ignores cancellation."
  [task]
  (fn [s f] (task s f) u/nop))

(defn never
  "A task that never completes; cancelling makes it fail."
  [s f]
  (let [alive (volatile! true)]
    (fn [] (when @alive (vreset! alive false)
             (f (u/cancelled "Never cancelled."))))))

;; --------------------------------------------------------------- running tasks

(defn- run-task* [task]
  (if-some [ps affine/*process*]
    (affine/suspend ps task)
    (let [p (promise)]
      (task (fn [x] (deliver p [:ok x])  nil)
            (fn [e] (deliver p [:err e]) nil))
      (let [[tag v] (deref p)]
        (if (= :ok tag) v (throw v))))))

(def blk
  "Executor for blocking calls: an unbounded pool, so a thread parked in a
  syscall never queues behind another. See `via`."
  thunk/blk)

(def cpu
  "Executor for compute: one thread per available processor. See `via`."
  thunk/cpu)

(defn via-call
  "Task evaluating (thunk) on the given `java.util.concurrent.Executor` and
  completing with its result. Cancellation interrupts the evaluating thread."
  [executor thunk]
  (fn [s f] (thunk/run executor thunk s f)))

(defmacro via
  "Task evaluating body on the given executor -- `blk` for calls that block,
  `cpu` for calls that compute -- and completing with its result. Cancellation
  interrupts the evaluating thread.

  Most ebb code does not need this: `sp` runs on a fiber and `?` parks at any
  depth, so a task that merely waits should be a task, not a thread. `via` is
  for genuinely blocking host calls with no task-shaped equivalent."
  [executor & body]
  `(via-call ~executor (fn [] ~@body)))

;; ------------------------------------------------------- reactive streams

(defn publisher
  "The discrete flow f seen as an `ebb.rs/Publisher`, running f once per
  subscription. See `ebb.rs` for the protocol."
  [f]
  (reify rs/Publisher
    (subscribe [_ s] (pub/run f s) nil)))

(defn subscribe
  "An `ebb.rs/Publisher` seen as a discrete flow."
  [pub]
  (fn [n t] (sub/run pub n t)))

(defn ^:no-doc run-task
  "Run task to completion, parking (not spinning) while it is pending. Returns
  its value or throws its error.

  On a process this is ebb.impl.affine/suspend, which also registers the task
  as the process's cancellation target. Off a process there is nothing to
  cancel and nothing to drive, so it is a plain park."
  [task]
  (if (ambiguous/in-ap?)
    (ambiguous/park task)
    (run-task* task)))



(defmacro ?
  "Run task, parking the current evaluation context until it completes."
  [task] `(run-task ~task))

(defn sp-call
  "Task running (f) as a process on its own fiber. `?` inside f -- at any depth
  -- parks that fiber.

  Returns only once the body has reached its first park, completed, or thrown,
  and each subsequent resumption drives the body to its next park before the
  resuming callback returns. See ebb.impl.affine.

  Cancelling cancels the task the body is parked on; the body sees Cancelled
  thrown from its `?` and decides what to do. It is not killed from outside."
  [f]
  (fn [s fail]
    (let [ps (affine/spawn-process
               (fn []
                 ;; the callbacks are deliberately outside the try: a throw
                 ;; from `s` must not be reported through `fail` as well.
                 (let [r (try [:ok (f)] (catch Throwable e [:err e]))]
                   (if (= :ok (first r)) (s (second r)) (fail (second r))))))]
      (fn [] (affine/kill ps) nil))))

(defn ^:no-doc check [] (affine/check))

(defmacro !
  "Throw Cancelled if the running process has been cancelled, else nil."
  [] `(check))

(defn ^:no-doc ap-run [body n t] (ambiguous/run body n t))

(defmacro ap
  "Discrete flow evaluating body, producing a value per branch. The body may
  park with `?` and fork with `?>`."
  [& body] `(fn [n# t#] (ap-run (fn [] ~@body) n# t#)))

(defmacro ?>
  "Fork the current ap branch for each value of flow. `par` (default 1) bounds
  how many branches may be in flight at once."
  ([flow] `(ambiguous/fork 1 ~flow))
  ([par flow] `(ambiguous/fork ~par ~flow)))

(defn zip
  "Discrete flow combining each successive value of the given flows with f."
  [f & flows]
  (fn [n t] (zip-impl/run f flows n t)))

(defn latest
  "Continuous flow combining the current value of each given flow with f."
  [f & flows]
  (fn [n t] (latest/run f flows n t)))

(defn sample
  "Discrete flow sampling the current value of each continuous flow on each
  value of the sampler, combined with f."
  [f & flows]
  (fn [n t] (sample/run f (first flows) (rest flows) n t)))

(defn relieve
  "Discrete flow relieving backpressure on flow: values the consumer did not
  take are combined with sg (a semigroup), defaulting to keeping the latest."
  ([f] (fn [n t] (relieve-impl/run {} f n t)))
  ([sg f] (fn [n t] (relieve-impl/run sg f n t))))

(defn buffer
  "Discrete flow accumulating up to capacity items of overflow from flow."
  [capacity f] (buffer-impl/flow capacity f))

(defn eduction
  "Discrete flow transforming flow with the composition of the transducers."
  [& xfs]
  (let [f (last xfs) xf (butlast xfs)]
    (fn [n t] (eduction-impl/run (apply comp xf) f n t))))

(defn reductions
  "Discrete flow emitting init then each successive reduction of flow by rf."
  ([rf f] (fn [n t] (reductions-impl/run rf f n t)))
  ([rf init f] (fn [n t] (reductions-impl/run (fn ([] init) ([r x] (rf r x))) f n t))))

(defn watch
  "Continuous flow reflecting the state of a reference type."
  [r] (fn [n t] (watch-impl/run r n t)))

(defn observe
  "Discrete flow observing values emitted by an external subject."
  [s] (fn [n t] (observe-impl/run s n t)))

(defn group-by
  "Discrete flow of [key flow] pairs, grouping flow's values by kf."
  [kf f] (fn [n t] (group-by-impl/run kf f n t)))

(defn memo
  "Task running the given task at most once per reactor, sharing its result."
  [task] (prop/publisher prop/memo nil task))

(defn stream
  "Discrete flow sharing one run of the given flow across all subscribers."
  [flow] (prop/publisher prop/stream nil flow))

(defn signal
  "Continuous flow sharing one run of the given flow across all subscribers."
  ([flow] (prop/publisher prop/signal {} flow))
  ([sg flow] (prop/publisher prop/signal sg flow)))

(defn ^:no-doc reactor-call [i] (fn [s f] (reactor-impl/run i s f)))

(defmacro reactor
  "Run body in a reactor context, in which stream! and signal! may be used."
  [& body] `(reactor-call (fn [] ~@body)))

(defn stream! [f] (reactor-impl/publish f false))
(defn signal! [f] (reactor-impl/publish f true))

(defn store
  "A mutable store combining assignments with a semigroup."
  ([sg] (store-impl/make sg nil))
  ([sg init] (store-impl/make sg init)))

(defmacro cp
  "Continuous flow evaluating body, sampled lazily by downstream. Fork with `?<`."
  [& body] `(fn [n# t#] (continuous/run (fn [] ~@body) n# t#)))

(defn ^:no-doc switch* [flow]
  ;; `?<` means different things to the two process kinds and missionary
  ;; dispatches on the running fiber; ebb dispatches on which prompt is in
  ;; scope. In `cp` it reads a continuous flow and re-runs the continuation on
  ;; change; in `ap` it is a fork whose new values replace the branch in flight,
  ;; which missionary spells as parallelism -1.
  (cond
    (ambiguous/in-ap?) (ambiguous/fork -1 flow)
    (continuous/in-cp?) (continuous/switch flow)
    :else (throw (u/cancelled "?< used outside ap or cp."))))

(defmacro ?<
  "Fork on the latest value of flow: in `cp` the rest of the body re-runs on
  each change, in `ap` each new value interrupts the branch in flight."
  [flow] `(switch* ~flow))

(defmacro amb
  "In an ap block, evaluate each form in turn, producing successive results."
  ([] `(?> none))
  ([form] form)
  ([form & forms]
   (let [n (inc (count forms))]
     `(case (int (?> (seed (range ~n))))
        ~@(interleave (range) (cons form forms))))))

(defmacro amb=
  "In an ap block, evaluate each form concurrently, producing results as they come."
  ([] `(?> none))
  ([form] form)
  ([form & forms]
   (let [n (inc (count forms))]
     `(case (int (?> ~n (seed (range ~n))))
        ~@(interleave (range) (cons form forms))))))

(def none
  "The empty discrete flow."
  (fn [_ t] (t) (reify clojure.lang.IFn (invoke [_] nil)
                       clojure.lang.IDeref (deref [_] (throw (u/cancelled "No value."))))))

(defmacro sp
  "Task evaluating body on a fiber. Body may park with `?` at any depth."
  [& body] `(sp-call (fn [] ~@body)))

;; --------------------------------------------------------- structured combinators

(defn join
  "Task running tasks concurrently, completing with (apply f results). The first
  failure cancels the others and fails the join."
  ([c] (fn [s f] (cps-call c s f) u/nop))
  ([c & ts] (fn [s f] (race-join/run false c ts s f))))

(defn race-failure [& errors] (ex-info "Race failure." {::errors errors}))

(defn race
  "Task running tasks concurrently, completing with the first success. The first
  success cancels the others; if every task fails, race fails."
  ([] (fn [_ f] (f (race-failure)) u/nop))
  ([& ts] (fn [s f] (race-join/run true race-failure ts s f))))

(defn any
  "Task running the given tasks concurrently, completing with the first to
  settle -- by success OR failure -- and cancelling the rest."
  ([] (race))
  ([x] (race x))
  ([x y] (absolve (race (attempt x) (attempt y))))
  ([x y & zs] (absolve (apply race (attempt x) (attempt y) (map attempt zs)))))

(defn timeout
  "Task running the given task, completing with v (nil if absent) if it has not
  settled within ms."
  ([task ms] (any task (sleep ms)))
  ([task ms v] (any task (sleep ms v))))

;; -------------------------------------------------------------------- ports

(defn dfv "Dataflow variable: (dfv v) assigns, (dfv s f) is a task." [] (dataflow/make))
(defn rdv "Rendez-vous: ((rdv v) s f) gives, (rdv s f) takes." [] (rendezvous/make))
(defn sem "Semaphore: (sem) releases a permit, (sem s f) is an acquire task."
  ([] (sem 1)) ([n] (semaphore/make n)))
(defn mbx "Mailbox: (mbx x) posts, (mbx s f) is a fetch task." [] (mailbox/make))

(defmacro holding
  "Acquire lock, evaluate body, release the lock however body exits."
  [lock & body]
  `(let [l# ~lock] (? l#) (try ~@body (finally (l#)))))

;; -------------------------------------------------------------------- flows

(defn seed
  "Discrete flow producing each value of coll."
  [coll]
  (fn [n t] (seed/run coll n t)))

(defn reduce
  "Task reducing flow with rf."
  ([rf flow] (fn [s f] (reduce-impl/run rf flow s f)))
  ([rf init flow]
   (fn [s f] (reduce-impl/run (fn ([] init) ([r x] (rf r x))) flow s f))))
