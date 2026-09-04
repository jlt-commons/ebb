(ns ^:no-doc ebb.impl.util
  "Shared primitives: the Cancelled signal and the host timer seam."
  (:require [jolt.scheme :as scheme]
            [jolt.fibers :as fib]
            [clojure.core.async :as a]))

(defn nop [])

(defn report-uncaught!
  "Last-resort report for a throw with nowhere to go.

  A `cast!` has no reply channel, so a thunk that throws on an owner fiber
  would otherwise vanish. Missionary's protocols say callbacks must not throw,
  so reaching here means a bug -- make it loud rather than silent, which is
  jolt's house rule."
  [where e]
  (binding [*out* *err*]
    (println (str "ebb: uncaught in " where ": " (ex-message e)))
    (flush))
  nil)

;; ------------------------------------------------------------ context-local
;;
;; Missionary's Java impls keep some state in a ThreadLocal -- Reactor's
;; `current` and `delayed` are the ones that matter. The .cljs impls use a plain
;; global, which is the same thing when there is one thread. Ebb ported the
;; global, and on a multi-carrier runtime that means one fiber's reactor context
;; is visible to every other fiber.
;;
;; jolt has no ThreadLocal constructor, and the carrier thread is the WRONG
;; granularity anyway: a fiber can park in the middle of a propagation and let
;; another fiber run on the same carrier, so two propagations would share an
;; entry. The right unit is the fiber, falling back to the thread off-fiber.
;; Returning the fiber (or thread) object itself, rather than a [thread fiber]
;; pair, keeps a read allocation-free.

(defn context-key
  "Identity of the current execution context: the fiber, or the thread when not
  on one. Distinct for any two things that can run concurrently."
  []
  (or (fib/current-fiber) (Thread/currentThread)))

(defn context-local
  "A place whose value is private to the context that set it -- ebb's
  ThreadLocal. Read with `local-get`, write with `local-set!`."
  []
  (atom {}))

(defn local-get [reg] (get @reg (context-key)))

(defn local-set!
  "Set this context's value, or drop the entry when v is nil so contexts that
  come and go do not accumulate."
  [reg v]
  (let [k (context-key)]
    (if (nil? v) (swap! reg dissoc k) (swap! reg assoc k v)))
  v)

;; ------------------------------------------- context carried across a call
;;
;; Per-fiber is nearly the right unit for the reactor's context, and not quite.
;; A propagation invokes a subscriber's notifier, and when that subscriber is an
;; `ap` or a `cp` the notifier is marshalled onto the process's OWN owner fiber
;; (ADR-001 discipline 2), where a per-fiber read finds nothing -- which is how
;; the fiber-local attempt broke `reactor-double-sub` with "Subscription failure
;; : not in reactor context". On the JVM that same notifier runs INLINE on the
;; propagating thread, so a ThreadLocal reaches it.
;;
;; What the ThreadLocal is really giving is the DYNAMIC EXTENT of the call, and
;; ebb's stand-in for that extent is `ebb.impl.server/call!`. So a local
;; registered here TRAVELS with the marshalling hop: the caller snapshots it,
;; the callee installs it for exactly the thunk it was sent with, and puts back
;; what was there before.
;;
;; And it travels BACK. A ThreadLocal is one cell shared by the caller and
;; everything it calls, so a write made by the callee is a write the caller sees
;; on return -- which the reactor depends on: `schedule` writes `delayed` from
;; inside a notifier, and the OUTERMOST `event` is what drains it. Hence the
;; reply carries the callee's final snapshot; see `server/answer`.
;;
;; The distinction that buys is the one the fiber alone cannot make. The same
;; owner fiber runs an ap both when the reactor pulled it -- in the extent of a
;; propagation, so in context -- and when a timer resumed it, with no reactor
;; call above it and therefore out of context. The first must see the reactor,
;; the second must not, and only the call chain tells them apart.

(def ^:private carried
  "Registries that travel with `call!`. Registered at load, never removed, so
  the index of each is stable and a snapshot can be a plain vector."
  (atom []))

(defn carried-local
  "A `context-local` that travels with `ebb.impl.server/call!`."
  []
  (let [reg (context-local)]
    (swap! carried conj reg)
    reg))

(defn capture-context
  "Snapshot the carried locals for the calling context, or nil when none of
  them is set. That is every call made outside a reactor, which is nearly all
  of them, so it answers nil without allocating."
  []
  (let [regs @carried
        n    (count regs)]
    (when (loop [i 0]
            (if (< i n)
              (if (some? (local-get (nth regs i))) true (recur (inc i)))
              false))
      (loop [i 0 snap []]
        (if (< i n) (recur (inc i) (conj snap (local-get (nth regs i)))) snap)))))

(defn install-context!
  "Make snap the carried context here, answering what was here before so the
  caller can put it back. nil means \"nothing set\", in both directions."
  [snap]
  (let [was (capture-context)]
    (when (or snap was)
      (let [regs @carried
            n    (count regs)]
        (loop [i 0]
          (when (< i n)
            (local-set! (nth regs i) (when snap (nth snap i)))
            (recur (inc i))))))
    was))

;; ---------------------------------------------------- batched process starts
;;
;; Starting an `sp` blocks the caller until the body reaches its first park --
;; that is missionary's ordering, and its own tests assert it. Missionary pays
;; nothing for it because the body runs INLINE on the caller's thread. Ebb's
;; bodies own their own fiber, so the same ordering costs a scheduling round
;; trip: 19us on an idle runtime, but 285us once the carriers are saturated.
;;
;; A combinator that starts N children in series therefore pays the SUM of
;; those round trips. Joining 100 processes cost ~100ms, which is most of the
;; rendezvous test's 200ms budget, and left the spawn loop uninterruptible for
;; the duration.
;;
;; The waits do not have to be serial. `batch-starts` collects them instead: a
;; combinator starts every child, then waits for all of them together, so the
;; cost is the SLOWEST first park rather than the total. The observable
;; contract is unchanged -- the combinator still returns only once every child
;; has reached its first park.

(def ^:dynamic *starting*
  "An atom collecting the gates of processes started under `batch-starts`, or
  nil when starts should wait individually."
  nil)

(defn batch-starts
  "Call (f), deferring the wait of every process started during it, then wait
  for them all. Returns f's value."
  [f]
  (let [collected (atom [])]
    (try (binding [*starting* collected] (f))
         (finally (run! deref @collected)))))

(defn cancelled
  "ebb's cancellation signal. missionary uses a dedicated `missionary.Cancelled`
  class; jolt has no user classes, so it is an ex-info carrying ::cancelled."
  [msg]
  (ex-info msg {:type ::cancelled}))

(defn cancelled? [e]
  (= ::cancelled (:type (ex-data e))))

(def ^:private timer-at!
  ;; host/chez/java/async.ss — the runtime's deadline heap, the same one
  ;; core.async/timeout rides on. No cancel: a fired-but-disarmed entry is a nop.
  (scheme/proc "jolt-timer-at!"))

(defn schedule!
  "Run (f) approximately ms from now, ON ITS OWN FIBER. Returns nil; disarm with
  your own flag.

  The fiber is not an optimisation. There is ONE timer thread for the whole
  runtime and it runs each due thunk itself, so a thunk that blocks stalls every
  other timer in the process. Ebb's thunks routinely block: completing a sleep
  resumes a process, and that resumption waits for the process to reach its next
  park (ADR-001 discipline 2). A hundred concurrent sleeps then serialise
  through one thread and everything downstream times out -- which is exactly how
  this was found, as 17 tests that pass alone timing out when run together.

  The fiber is also the containment missionary's #81 lacks. There the callback's
  throw reached the one scheduler thread and killed it, and every later sleep in
  the process deadlocked. Here it can only kill the fiber it was raised on -- but
  it is still reported by name rather than left to the runtime's default handler,
  because a throw out of a timer callback means a consumer callback violated its
  \"must not throw\" clause and that should be legible, not a bare trace."
  [ms f]
  ;; fiber-execute, not fibers/spawn: this is fire-and-forget, and spawn also
  ;; builds a handle and completion machinery nobody reads. It is on the hot
  ;; path -- every sleep in the system lands here.
  (timer-at! (+ (System/currentTimeMillis) ms)
             (fn [] (a/fiber-execute
                      (fn [] (try (f) (catch Throwable e (report-uncaught! "timer callback" e)))))
               nil))
  nil)
