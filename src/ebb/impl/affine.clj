;; ebb.impl.affine -- the fiber-affinity runtime (ADR-001).
;;
;; A missionary process is a state machine driven SYNCHRONOUSLY by whoever
;; touches it: starting the task runs the body to its first park, and a task's
;; success callback runs the body on to its NEXT park, all on the caller's
;; thread. Callers depend on that, and so does lolcat, which is how 30 of
;; missionary's 33 test namespaces are written.
;;
;; Ebb runs the body on a fiber, so nothing is driven by the caller. This file
;; restores the observable contract with two handshakes and nothing else.
;;
;; ---------------------------------------------------------------------------
;; Why handshakes, and why they are not "mailboxes"
;;
;; Erlang and the Scheme/CML lineage agree on one rule: the waker never runs the
;; waiter's code, it makes it runnable and returns. Erlang copies into the
;; receiver's mailbox and enqueues the receiver on a run queue; Guile Fibers,
;; after Concurrent ML, resumes with (schedule (lambda () (k val))) rather than
;; calling k inline, arbitrating with an atomic W/C/S box so exactly one party
;; commits a rendezvous and is then responsible for scheduling the others.
;;
;; jolt already implements that: alt-deliver! (host/chez/java/async.ss) commits
;; the value and calls jolt-fiber-wake-fn, which enqueues the waiter on its
;; carrier's run queue -- lock order channel mu -> fmu -> wmu -> run-queue mu,
;; the run-queue mutex a leaf the park path never takes. So ebb does not build a
;; mailbox; that would re-implement jolt's scheduler one layer up.
;;
;; What ebb needs is the OTHER half, and Erlang names it too: an asynchronous
;; send does not tell you the receiver got anywhere, so when you need that you
;; use a synchronous request/reply -- gen_server:call, not `!`. Every ebb
;; interaction with a process is that shape:
;;
;;   the driver installs a gate, pokes the process, and waits until the process
;;   is QUIESCENT again -- parked on its next task, or finished.
;;
;;   * spawn-process is the call at start:  missionary's "the body has run to
;;     its first park before the task call returns".
;;   * resume! is the call at each wakeup:  missionary's "the success callback
;;     drives the body on to its next park before returning".
;;
;; The driver PARKS while waiting, so its carrier is released rather than
;; pinned running someone else's code -- the thread-overhang hazard Swift's
;; SE-0472 names for the run-on-the-caller alternative. And a process's
;; continuations only ever exist on its own fiber, so the cross-fiber resume
;; that hangs jolt with no error (jolt.continuations) cannot be expressed.
;;
;; ---------------------------------------------------------------------------
;; The one rule this imposes on the rest of ebb
;;
;; Both handshakes park the driver, and jolt forbids a fiber leaving the CPU
;; while its carrier holds a counted lock (host/chez/locks.ss, enforced at the
;; switch AND statically by park-lock-check.ss against an allowlist that is
;; empty by design). So NOTHING may invoke a task, or a process's callback,
;; from inside a locked region -- including the implicit lock nobody thinks of:
;; realizing a LAZY SEQ takes a counted lock.
;;
;;   (dorun (map-indexed (fn [i t] (t s f)) tasks))   ; parks under the seq lock
;;   (loop [i 0 ts (seq tasks)] ...)                  ; fine
;;
;; That is why missionary's impls loop over an iterator rather than mapping, and
;; why ADR-001 rule 1 insists callbacks fire outside the critical section. When
;; it goes wrong the error names the symptom, not the cause:
;;
;;   jolt-fiber-to-scheduler!: a fiber cannot leave the CPU while its carrier
;;   holds a counted lock (1 held).
;;
;; Read (jolt.scheme/proc "jolt-locks-held") at the park site to find it. A
;; non-zero count on ENTRY means the caller is at fault, not the parking code.
(ns ^:no-doc ebb.impl.affine
  (:require [jolt.fibers :as fib]
            [ebb.impl.util :as u :refer [nop cancelled]]))

;; gate  -- promises of every driver waiting for the body to reach its next
;;          park. A VECTOR, not a single cell. It was a single cell, and
;;          model/ebb/model/handshake.clj shows why that is unsound: two
;;          concurrent resumers install two gates but only the first `deliver`
;;          wakes the body (the second is a no-op on a realized promise), so
;;          there is one open for two waiters and one resumer blocks forever --
;;          in EVERY schedule, not just an unlucky one. A blocked resumer is a
;;          blocked carrier.
;;
;;          A task that completes twice is what produces two resumers;
;;          ebb.impl.sleep was one such source and is fixed. Draining the whole
;;          vector makes the handshake correct regardless of whether another
;;          source remains, which is the property worth having.
;; done  -- set once the body has finished, so a resumer that arrives after the
;;          last drain releases itself instead of waiting for a park that will
;;          never come.
;; token -- nil once cancelled; otherwise the cancel fn of the task this process
;;          is currently parked on (nop while running). Ported from
;;          missionary/impl/Sequential.java, which holds it under the process
;;          monitor; ebb's body runs on its own fiber instead, so the token is
;;          an atom driven by CAS (ADR-001 rule 1).
;;
;; That covers the whole of Sequential.java's monitor, which is the audit item
;; for this file (ebb-8nq.32). Sequential has no pull loop and no `busy`
;; toggle: the three regions it synchronises are the token swap in `suspend`,
;; the token swap in `cancel`, and the read in `check` -- all read-modify-writes
;; on one cell, and all of them CAS here. The remaining fields are the gate
;; vector and the done flag, atoms for the same reason. There is nothing left
;; for a lock to hold, and a lock could not be held anyway: every one of these
;; paths ends in a park.
(deftype Process [^:unsynchronized-mutable fiber gate token done])

(def ^:dynamic *process*
  "The process whose body is running on this fiber, or nil off a process.
  Bound by spawn-process inside the fiber it spawns, so `identical?` against it
  is a race-free 'am I on this process's own fiber?'."
  nil)

(defn- quiescent!
  "Tell every waiting driver the process has parked or finished."
  [^Process ps]
  ;; The empty check is not redundant: most parks have nobody waiting, and it
  ;; keeps them to a plain deref instead of a swap.
  (when (seq @(.-gate ps))
    (doseq [g (first (swap-vals! (.-gate ps) empty))] (deliver g true)))
  nil)

(defn- finish!
  "The body has run out. Release every waiter, now and in future."
  [^Process ps]
  (reset! (.-done ps) true)
  (quiescent! ps)
  nil)

;; --------------------------------------------------------------- cancellation

(def ^:private ^:dynamic *cancelling*
  "True while we are inside a canceller.

  The task protocol says a canceller \"must not block the calling thread\", and
  ebb's resumption handshake blocks by construction. The two meet whenever a
  canceller completes its task synchronously -- `sleep` does exactly that,
  failing with Cancelled from inside the cancel call -- so the handshake has to
  stand down for that one path.

  It is not a shortcut: cancelling a join of 100 children cancels them in a
  loop, and each one waiting for its child to park cost ~1.2ms, or 120ms in
  series. Missionary has no equivalent cost because its bodies run inline on the
  cancelling thread, which is work rather than waiting."
  false)

(defn- arm!
  "Make cancel the process's current cancellation target. If the process has
  already been killed, cancel the task straight away instead -- missionary's
  `suspend`: `if (ps.token == null) c.invoke(); else ps.token = c;`."
  [^Process ps cancel]
  (loop []
    (let [t @(.-token ps)]
      (if (nil? t)
        (binding [*cancelling* true] (cancel))
        (when-not (compare-and-set! (.-token ps) t cancel) (recur))))))

(defn kill
  "Cancel the process: cancel whatever task it is parked on, and make every
  later park fail immediately. Idempotent.

  Cancellation is COOPERATIVE, exactly as in missionary and as in Erlang: it
  does not stop the body. The pending task fails with Cancelled, that surfaces
  as an exception at the `?` that parked, and the body decides what to do --
  which is why `(try (? ...) (catch Cancelled _ ...))` is the idiom. A body that
  never parks and never calls `check` runs to completion regardless."
  [^Process ps]
  (loop []
    (when-some [t @(.-token ps)]
      (if (compare-and-set! (.-token ps) t nil)
        (binding [*cancelling* true] (t))
        (recur))))
  nil)

(defn check
  "Throw Cancelled if the running process has been killed; nil off a process.
  This is missionary's `!`."
  []
  (when-some [^Process ps *process*]
    (when (nil? @(.-token ps))
      (throw (cancelled "Process cancelled."))))
  nil)

;; ------------------------------------------------------------------ the calls

(defn- resume!
  "Hand a completed task's result to the parked process and wait until it is
  quiescent again. No waiting when we are already on the process's own fiber --
  that is a task that completed synchronously inside suspend, and the body will
  simply carry on down the stack we are standing on."
  [^Process ps p v]
  (if (or *cancelling* (some? *process*))
    ;; The resumer is itself a process. Missionary's ordering contract is a
    ;; contract with the OUTSIDE -- a test, lolcat, a timer -- that a callback
    ;; drives the process to its next park before returning. Nothing depends on
    ;; one process blocking on another's progress, and doing so deadlocks: eight
    ;; processes sharing a semaphore, cancelled together, each end up waiting on
    ;; a gate only a peer that is itself waiting could open.
    ;;
    ;; The *cancelling* case is the protocol's own rule: a canceller must not
    ;; block, so a task that fails from inside its own canceller -- `sleep` --
    ;; delivers and returns, and the body advances on its own fiber.
    (deliver p v)
    (let [g (promise)]
      (swap! (.-gate ps) conj g)
      (deliver p v)
      ;; If the body finished before we joined the queue nobody will drain it
      ;; again, so drain it ourselves. Ordering both ways is safe: whichever of
      ;; us observes the other's write does the delivering, and `deliver` on an
      ;; already-open gate is a no-op.
      (when @(.-done ps) (quiescent! ps))
      (deref g)))
  nil)

(defn suspend
  "Run task, parking the running process until it completes. Returns its value
  or throws its error. Registers the task's cancel fn as the process's
  cancellation target first, so killing the process reaches this task."
  [^Process ps task]
  (let [p      (promise)
        cancel (task (fn [x] (resume! ps p [:ok x]) nil)
                     (fn [e] (resume! ps p [:err e]) nil))]
    (arm! ps cancel)
    ;; Only a task that did NOT complete synchronously leaves us parked, and
    ;; only then is the process quiescent. No race: an unrealized promise here
    ;; means the completion is coming from elsewhere, so we will park.
    (when-not (realized? p) (quiescent! ps))
    (let [[tag v] (deref p)]
      (if (= :ok tag) v (throw v)))))

(defn spawn-process
  "Run (f) as a process on its own fiber. Returns the Process once the body has
  reached its first park, completed, or thrown -- not before."
  [f]
  (let [ps (->Process nil (atom []) (atom nop) (atom false))
        g  (promise)]
    (swap! (.-gate ps) conj g)
    (set! (.-fiber ps)
          (fib/spawn
            (fn []
              ;; *starting* is rebound to nil inside the fiber: a child that
              ;; starts processes of its own must not enrol them in the
              ;; parent's batch.
              (binding [*process* ps, u/*starting* nil]
                (try (f)
                     (finally (finish! ps)))))))
    (if-some [collected u/*starting*]
      (swap! collected conj g)     ; the batch will wait for us
      (deref g))
    ps))
