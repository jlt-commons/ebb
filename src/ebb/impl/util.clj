(ns ^:no-doc ebb.impl.util
  "Shared primitives: the Cancelled signal and the host timer seam."
  (:require [jolt.scheme :as scheme]
            [jolt.fibers :as fib]
            [clojure.core.async :as a]))

(defn nop [])

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
  this was found, as 17 tests that pass alone timing out when run together."
  [ms f]
  ;; fiber-execute, not fibers/spawn: this is fire-and-forget, and spawn also
  ;; builds a handle and completion machinery nobody reads. It is on the hot
  ;; path -- every sleep in the system lands here.
  (timer-at! (+ (System/currentTimeMillis) ms)
             (fn [] (a/fiber-execute f) nil))
  nil)
