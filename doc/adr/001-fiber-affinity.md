# ADR-001: Fiber affinity and the two concurrency disciplines

Status: accepted
Date: 2026-09-03

## Context

Missionary is a callback system. A task is `(fn [success failure] cancel)` and a
flow is `(fn [notifier terminator] iterator)`. Nothing in those signatures says
which thread a callback arrives on, and in practice it is whichever one completed
the underlying operation.

Two facts about jolt make that ambiguity load-bearing rather than incidental.

**A continuation is bound to the fiber that captured it.** `jolt.continuations`
is explicit: invoking a continuation captured on another fiber "transfers control
into a stack segment this thread is not running; unguarded it HANGS the process
with no error at all." Ebb needs re-entrant `call/cc` for `ap`/`cp`, so this is
not a corner we can avoid. A hang with no error is the worst possible failure
mode — it cannot be caught, logged, or bisected.

**Jolt's carriers are a real thread pool.** Missionary ships two impl sets: the
`.clj` ones synchronize, the `.cljs` ones assume a single thread and take no
locks. The `.cljs` set is the right porting base — it is pure Clojure, and the
`.clj` set is Java — but its lock-free assumption is false on jolt, where
`(jolt.fibers/carrier-count)` is greater than one by default.

Taking the `.cljs` logic and the `.clj` locking discipline is the correct
combination, but "add locks where it seems necessary" is not a discipline. Each
port would invent its own answer and the failures would be nondeterministic.

## Decision

Two disciplines, chosen per component by one question: **does this thing hold a
continuation?**

### 1. Shared components: mirror the Java impl's strategy exactly

Ports, flow operators, and the propagator. These are reached by many fibers at
once and hold no continuations. Take the LOGIC from the `.cljs` file (pure
Clojure) and the SYNCHRONISATION from the `.clj`/Java file, which is the
specification. Missionary splits cleanly into two strategies, and the split is
not arbitrary -- match it rather than picking one:

**CAS on a single state field** -- `Dataflow`, `Rendezvous`, `Semaphore`,
`Mailbox`, `Never`, `RaceJoin`, `Latest`, `Store`. Java holds one `volatile
Object state` and drives it with an `AtomicReferenceFieldUpdater` retry loop. In
ebb this is one `atom` holding the whole state, with `compare-and-set!` in a
`loop`/`recur`. Do not decompose it into several mutable fields: the atomicity of
the port depends on the whole state moving in one CAS.

**`locking` on the impl object** -- `Buffer`, `GroupBy`, `Observe`, `Reductions`,
`Reduce`, `Sample`, `Relieve`, `Watch`, `Reactor`. Java uses `synchronized`
because the state is too large for one word. Mirror it with `locking`.

In both strategies, **a callback is invoked only after leaving the critical
section** -- after the CAS succeeds, or outside the `locking` body. Missionary is
scrupulous about this and so must ebb: a `success`/`notifier` call can re-enter
the same port, and invoking it under the lock deadlocks or corrupts state.

Cancellation identity comes from Java's `Event` (an emitter plus a
success/failure pair, callable to cancel itself). The `.cljs` impls fake it with
a closure in a set, which works single-threaded but loses identity under CAS
retry. Port `Event`.

### 2. Coroutine processes: one owner fiber, driven by handshakes

`Sequential` (`sp`), `Ambiguous` (`ap`), `Continuous` (`cp`).

Each process owns exactly one fiber for its lifetime. Continuations are captured
and resumed only on that fiber, which makes a cross-fiber resume structurally
impossible rather than a latent hang, and gives process state exactly one writer
so the `.cljs` lock-free logic is correct again inside a process.

**The mailbox originally specified here is withdrawn: jolt already provides it.**
Erlang and the CML lineage agree that *the waker never runs the waiter's code; it
makes it runnable and returns.* Erlang copies into the receiver's mailbox and
enqueues the receiver on a run queue; Guile Fibers, after Concurrent ML, resumes
with `(schedule (lambda () (k val)))` rather than calling `k` inline, arbitrating
with an atomic W/C/S box so exactly one party commits a rendezvous. jolt's
`alt-deliver!` (`host/chez/java/async.ss`) does precisely this -- it commits the
value then enqueues the waiter on its carrier's run queue, lock order
`channel mu -> fmu -> wmu -> run-queue mu` with the run-queue mutex a leaf the
park path never takes. A hand-written mailbox would re-implement jolt's
scheduler one layer up.

What ebb needs is the other half, and Erlang names that too: an asynchronous send
tells you nothing about the receiver's progress, so when you need that you use a
synchronous request/reply -- `gen_server:call`, not `!`. Missionary's contract is
exactly a request/reply, because its process is a state machine its caller steps:
starting the task runs the body to its first park, and a success callback runs
the body on to its next park, both before returning. Ebb restores that with two
handshakes in `ebb.impl.affine`, and nothing else:

- **at start** (`spawn-process`) -- return only once the body has parked,
  completed, or thrown.
- **at each wakeup** (`resume!`) -- deliver the result, then wait until the body
  is quiescent again.

In both, the driver installs a gate, pokes the process, and parks until the
process opens it. Parking rather than running the body on the caller's stack
releases the driver's carrier, avoiding the thread-overhang hazard Swift's
SE-0472 names for the run-on-the-caller alternative -- which ebb could not use
anyway, since it would need a continuation to cross fibers.

**A process that holds continuations is an Erlang-shaped server.** `sp` needs
only the two handshakes above, because its body owns the fiber it runs on. `ap`
and `cp` hold captured continuations, and their inputs arrive from everywhere --
a timer thread completing a task, another fiber notifying upstream, the
consumer's thread transferring downstream, anyone cancelling. None of those may
touch process state or resume a continuation directly.

So such a process gets one owner fiber and an inbox, and every entry point goes
through it. That server is `ebb.impl.server`, shared by `ap` and `cp`. A message
sent from the owner's own fiber runs inline rather than deadlocking, which is the
common case -- the owner notifies downstream and downstream transfers straight
back in.

**Which entry points are `call!` and which are `cast!`.** Not all of them need
the reply, and Erlang's distinction is the right one here too:

- **`call!` -- `gen_server:call`.** The *transfer*, because it returns the value
  the caller asked for, and the *start*, because missionary's contract is that a
  process has reached its first park before the call that started it returns.
- **`cast!` -- `!`.** Everything ebb hands OUT: the notifier and terminator it
  gives an upstream flow, the two continuations it gives a task, and the
  canceller it gives downstream. Both protocols say each of these *must not
  block the calling thread*, and `call!` blocks all of them. Enqueue and return
  is what the protocol asks for and what the CML rule above already says: the
  waker does not run the waiter's code.

The single exception is a caller with **ambient context** in scope -- see rule 6.
A carried local stands in for a `ThreadLocal`, whose whole point is that the
callee's writes are visible to the caller on return, and a cast has no reply to
carry them back in. Inside a reactor propagation, therefore, `cast!` degrades to
`call!`. Nothing outside the reactor registers a carried local, so nothing else
pays for it.

Making these casts cost nothing in the runtime and a great deal in the test
suite, which is why it took a second pass (`ebb-8nq.29`): lolcat, missionary's
own DSL, required every event a `call` declared to have been raised before that
call returned, which is only true when callbacks run inline on the caller. It now
waits for them instead. Same events, same order, one instruction later.

This is the same conclusion discipline 2 reaches for `sp`, arrived at from the
other side: there the question was "has the body got anywhere yet", here it is
"whose stack am I allowed to touch". Both answers are a synchronous call to a
single owner.

**Cancellation is a token, and it is cooperative.** Ported from
`Sequential.java`: the token holds the cancel fn of the task the process is
parked on (`nop` while running), `kill` nulls it and invokes it, and a park
attempted after that cancels its task immediately. Missionary holds the token
under the process monitor; ebb's body runs on its own fiber, so the token is an
atom driven by CAS. Cancelling does **not** stop the body -- the pending task
fails with `Cancelled`, that surfaces at the `?` which parked, and the body
decides. This is missionary's model and Erlang's, and on fibers it is the only
one available: there is no safe way to unwind another fiber's stack.

### 3. `ap`/`cp` capture through a prompt, never raw `call/cc`

`sp` needs no continuations -- a fiber parks a real stack. `ap` and `cp` do,
because `?>` forks: the continuation of the fork point runs once per value, so
it must be re-enterable, and a fiber's stack cannot be cloned. This is the one
place ebb reaches below jolt.

Raw `call/cc` captures the rest of the *program*, so resuming one re-runs the
caller -- during development this showed up as the driver loop executing twice
per fork. What is wanted is Guile's `call-with-prompt`: capture only the extent
between the prompt and the capture point, composable, invocable more than once.

Chez 10.4.1 has no native prompts (verified: no `call-with-prompt`,
`abort-to-prompt`, `shift`, or prompt tags). The principled alternative is the
Dybvig / Peyton Jones / Sabry delimited-control library, which builds prompts on
`call/cc` plus a metacontinuation. **Rejected**, on Oleg Kiselyov's own caveat
that such a library "must be used with extreme care in the presence of other
effects: dynamic binding, exceptions, or independent uses of `call/cc`" -- ebb
has all three at once, since jolt's fiber park is itself a `call/1cc` capture.

`ebb.impl.prompt` builds the narrow thing instead: a cell holding the current
delimiter, driven in the Guile Fibers shape -- the driver re-enters the
continuation, the continuation never calls the driver. Two details there are
load-bearing rather than incidental, and both are commented in the file: the
normal return must go through the cell (re-read at return time) or every resume
delivers to the *first* driver call; and that read must happen after the thunk
runs, since `(@cell [::value (thunk)])` evaluates the operator first. A dynamic
`binding` cannot serve as the cell -- bindings survive a capture but not a
re-entry.

Two guards, each pinned by a test that fails without it:

- **Same-executor resume.** Resuming a continuation captured elsewhere transfers
  into a stack segment this carrier is not running, which `jolt.continuations`
  documents as a hang with no error. Refused with a throw.

  The identity is **`(thread, fiber)`, not the fiber alone**. `current-fiber` is
  nil off a fiber, so a fiber-only check passes vacuously between two OS
  threads -- which is how a timer thread came to resume a main-thread
  continuation unnoticed for a while. A continuation belongs to a Scheme
  thread's stack whether or not a fiber is riding it.
- **No fork inside a `finally`.** Re-entry re-runs dynamic-wind after-thunks,
  and jolt compiles `finally` to one. Measured: `finally` runs N+1 times for N
  branches where the answer is N. `catch` has no winder and is allowed; fiber
  parks are unaffected because jolt strips finally winders on a park. Detected
  by walking the winder chain for jolt's own `jolt-finally-marker`, relative to
  the depth recorded at prompt entry, so a `finally` outside the prompt does not
  count.

### 4. A process's prompt is keyed by executor, never held in a global cell

Missionary tracks "which process is running" in a mutable global (`fiber`), set
around each coroutine step and restored after. Ebb copied that and it is unsound
here, for a reason specific to prompts: **an extent returns through whichever
delimiter the prompt cell holds at the time**, so when a resume arrives from a
different frame than the one that entered, the entering frame is abandoned and
its restore never runs. The cell then names a dead prompt, and the next ordinary
`(m/? (m/sleep ...))` anywhere in the program is routed into it.

That surfaced as a `fork-in-finally` error on a task that forks nothing, and it
made the suite alternate pass and fail across runs with nothing changed.

A process's body only ever runs on that process's own owner fiber (discipline
2), so the association is not per-call: it is per-executor and fixed for the
process's lifetime. `ap` and `cp` register their prompt against `(thread, fiber)`
when the process starts and deregister when it retires. Nothing to restore, and
nothing to leak.

### 5. Never invoke a task from inside a locked region

The rule that actually bites, and it is jolt-specific. `host/chez/locks.ss`
enforces that **a fiber never leaves the CPU while its carrier holds a counted
lock** -- checked at the switch, and statically by `park-lock-check.ss` against
an allowlist that is empty by design.

Invoking a task can park. So no task may be invoked from inside a locked region
-- including the implicit lock nobody thinks of: **realizing a lazy sequence
takes a counted lock.**

```clojure
(dorun (map-indexed (fn [i t] (t s f)) tasks))   ;; parks under the seq lock
(loop [i 0 ts (seq tasks)] ...)                  ;; fine
```

This is why missionary's impls loop over an iterator rather than mapping, and on
jolt that shape is load-bearing rather than stylistic. Port the loops as loops.

When it goes wrong the error names the symptom, not the cause:

```
jolt-fiber-to-scheduler!: a fiber cannot leave the CPU while its carrier
holds a counted lock (1 held).
```

Read `(jolt.scheme/proc "jolt-locks-held")` at the park site to find it. A
non-zero count on *entry* means the caller is at fault, not the parking code.

### 6. Ambient context travels with the handshake, in both directions

Missionary's Java impls keep three pieces of state in a `ThreadLocal`:
`Reactor.CURRENT` (which reactor is propagating), `Reactor.DELAYED` (which
reactors have work queued behind this propagation), and `Propagator.context`
(the `Context` a publisher's propagation belongs to, `withInitial(Context::new)`).
The `.cljs` impls use plain globals, which is the same thing when there is one
thread.

Ebb can use neither, and the reason is worth stating because both wrong answers
look right:

- A **global** answers "yes, you are inside a propagation" to every fiber in the
  process. An event raised from a timer fiber while some reactor happened to be
  propagating then took the in-context branch and read `(.-current ps)` — which
  the propagation nils on its way out — as a null `.-ranks`.
- **Per-fiber** is wrong in the opposite direction, and deterministically. On the
  JVM a reactor's flows run *inline on the propagating thread*, so the
  `ThreadLocal` reaches them all. In ebb an `ap` or `cp` inside a reactor runs on
  its own owner fiber (discipline 2), so the subscription made from that fiber
  finds nothing: `Subscription failure : not in reactor context`.

Neither the thread nor the fiber is the unit. What a `ThreadLocal` delimits,
given inline execution, is the **dynamic extent of the call** — and ebb's
stand-in for that extent is the `server/call!` handshake. So state of this kind
is a *carried local* (`ebb.impl.util/carried-local`): the caller snapshots it
into the message, the callee installs it for exactly that thunk and restores
what was there before, and the reply carries the callee's final snapshot back.

The return leg is not symmetry for its own sake. A `ThreadLocal` is one cell
shared by a caller and everything it calls, so a write by the callee is a write
the caller sees on return, and the reactor depends on precisely that: `schedule`
runs from a notifier deep inside a propagation and pushes onto `DELAYED`, while
the *outermost* `event` is what drains it afterwards. Carry the context down but
not back and that push is stranded on the callee's fiber, where nothing drains
it — the reactor stops with no error at all.

What this buys is the distinction the fiber alone cannot make. The same owner
fiber runs an `ap` both when a propagation pulled it — in the extent of the
call, so in context — and when a timer resumed it, with no reactor call above
it and therefore out of context. Only the call chain tells those apart, and the
JVM gets the same answer for the same reason.

**A `ThreadLocal` with an initial value is two cells, not one**, and
`Propagator.context` is the case that shows it. `withInitial(Context::new)` says
both "this executor's own scratch Context, reused" and "the Context of the
propagation in flight". Only the second is carried; the first is a plain
`context-local`, allocated once per executor. Carrying both would be worse than
useless: a carried local that is never unset makes `cast!` degrade to `call!`
forever (discipline 2), so every callback would go back to blocking. The active
cell is therefore set by `enter` and cleared by the `leave` that matches it, and
`(some? (capture-context))` means exactly "a propagation is in flight above me".

## Consequences

**`sp` gets simpler than missionary's.** A jolt fiber parks a real stack, so `sp`
needs no coroutine at all — no macro transform, no state machine. `?` therefore
works at any call depth, lifting cloroutine's lexical restriction. The owner
fiber *is* the process.

**`ap`/`cp` pay a hop.** A callback cannot resume the process inline; it enqueues
and the owner fiber picks it up. This costs a scheduling hop per resumption and
is the price of not hanging. Do not optimize it away with an "if we are already
on the owner fiber, resume inline" fast path until there is a benchmark
justifying it — that path reintroduces exactly the reentrancy the discipline
exists to prevent.

The hop is dearer than a microbenchmark suggests, and knowing why matters when
reading one. A hot owner fiber — one being hammered in a tight loop — answers a
`call!` in about 27µs. A real one is **cold**, parked on its channel between
messages, and waking it costs 45–55µs. So count hops, but price them cold:
`ebb-8nq.36` concluded marshalling was negligible by multiplying a hop count by
the hot figure, and `ebb-8nq.40` found it was about half the cost of starting an
`ap`. What follows from that is to remove hops rather than shave them —
`server/start!` runs a process's start-up on the owner fiber as it spawns it,
which is one handshake where there were two.

**Impl ports are not free translations.** Every shared-impl port must consult the
Java file for its synchronisation strategy even though it derives its logic from
the `.cljs` file. Note both sources in the provenance header.

**This was found the hard way, which is why it is written down.** The feasibility
spike ported `Dataflow` and `Rendezvous` straight from `.cljs` with unsynchronised
mutable fields. Each worked in isolation and under repetition; running a dfv
handoff and an rdv handoff in the same process deadlocked, because two fibers lost
an update to the waiter set and both sides waited forever. A silent hang, exactly
the failure mode this ADR exists to eliminate -- and it took a bisect to find.

**The `finally` hazard is separate and narrower.** Re-entering a continuation
re-runs `dynamic-wind` after-thunks, so a `finally` whose `try` lexically
contains a fork point runs more than once. `catch` is unaffected, and fiber parks
are unaffected because jolt already strips finally winders on a park. This is
handled by a guard at the fork site (`ebb.impl.prompt/finally-depth`), not by
this ADR.

## Alternatives rejected

**Pin everything to one carrier** (`set-carrier-count! 1`). Makes the `.cljs`
assumption true globally and needs no discipline at all — but it makes ebb
single-threaded, which forfeits the reason to run on fibers, and it is a
process-global setting a library has no business imposing on its host.

**Lock the coroutine processes too.** Uniform, but it does not solve the real
problem: a lock stops concurrent mutation, it does not stop a continuation being
resumed from the wrong fiber. The hang remains, now harder to see.

**Port a cloroutine-equivalent macro transform instead of using `call/cc`.**
Sidesteps affinity entirely, since a state machine has no stack to be bound to.
Rejected as ~500–800 lines of the hardest code in the project, reimplementing
what Chez already does natively — and it must hand-compile `try`/`catch`/`loop`/
`case` to be correct. Worth revisiting only if affinity proves unworkable in
practice; the fallback is real, which is part of why the `call/cc` route is
acceptable to try first.
