# ebb vs. missionary: known divergences

Every difference between ebb and missionary that a user could observe. Following
jolt's own house pattern (`test/conformance/known-divergences.edn`), this is
meant to be exhaustive: a divergence that is not listed here is a bug, not a
feature.

Ebb runs **missionary's own test suites** for every ported namespace, edited
only where this file says so. That is the evidence behind most of these rows.

## Deliberately absent

| API | Why |
|---|---|
| `via`, `via-call`, `blk`, `cpu` | JVM `Executor`s. Jolt has fibers; `sp` already runs on one, and `?` parks at any depth, so the thing these existed to work around is not present. |
| `publisher`, `subscribe` | Reactive Streams interop, out of scope for the port. |
| `Pub`, `Sub`, `Thunk` | Implementation support for the two rows above. |

## Behavioural differences

**`Cancelled` is a value, not a class.** Missionary throws
`missionary.Cancelled`; jolt has no user-defined classes, so ebb throws an
`ex-info` carrying `{:type ::ebb.impl.util/cancelled}`. Use `m/cancelled?`
rather than `(instance? Cancelled e)`. This is the only edit most ported tests
needed.

**`?` works at any call depth — ebb is a superset here.** Cloroutine's transform
is lexical, so in missionary `?` must appear syntactically inside the `sp` body
and a helper function cannot park. Ebb's `sp` is a fiber with a real stack:

```clojure
(defn helper [ms v] (m/? (m/sleep ms v)))   ; an ordinary fn that parks
(m/? (m/sp (inc (helper 10 5))))            ;=> 6
```

Code written for missionary keeps working; code written for ebb may not port
back.

**A fork inside a `try` with `finally` is refused.** Jolt compiles `finally` to a
`dynamic-wind` after-thunk, so re-entering a continuation re-runs it — measured
at N+1 runs for N branches, where N is correct. Rather than corrupt cleanup
silently, `?>`/`?<` throw `::ebb.impl.prompt/fork-in-finally` when the fork sits
lexically inside a `finally`'s `try`. Narrow in practice: `catch` is unaffected,
`sp` and every fiber park are unaffected (jolt strips finally winders on a
park), and in missionary's entire `src` + `test` + `doc` corpus `catch` appears
12 times against a single user-facing `finally`, which wraps a park, not a fork.

**Continuations are executor-affine.** A continuation may only be resumed on the
`(thread, fiber)` that captured it; anything else throws
`::ebb.impl.prompt/wrong-fiber` rather than hanging, which is what jolt does
unguarded. Ebb satisfies this itself — `ap` and `cp` processes marshal every
entry point to an owner fiber — so it is visible only to code reaching into
`ebb.impl.prompt` directly.

**`ap` and `cp` cost a scheduling hop per resumption**, because a callback
enqueues on the owner rather than resuming inline. `sp` does not.

## Host differences inherited from jolt

- **Strings are codepoint-indexed.** `(count "😀")` is 1.
- **`compare-and-set!` on an atom compares by value**, where JVM Clojure uses
  reference identity. Harmless for ebb's CAS ports — their states are sets and
  maps of `Event` deftypes, which have identity equality, and no `Event` is
  removed and re-added, so ABA is unreachable. **Any future CAS port over
  value-equal states must check this.**
- **No thread interruption.** Missionary's `Observe` consults the JVM interrupt
  flag; ebb's is ported from the `.cljs` impl and detects overflow structurally.
  `observe_test`'s `interrupt-current!` is a no-op in ebb — left in place so the
  test keeps its shape, and neutered because the flag otherwise survives the
  test and kills the next `promise` deref on that thread.

## Known defect: the suite can hang, and three tests overrun their budgets

**Roughly one run in ten of `ebb.core-test` hangs outright** in the `mailbox`
test, spinning all four carriers at 75-80% CPU each rather than sitting idle.
It is a livelock, not a quiet deadlock. Tracked as `ebb-8nq.23`, and not
explained yet. When the suite does not hang it passes: 225 tests, 265
assertions.

The same three tests -- `semaphore`, `rendezvous`, `mailbox` -- also sometimes
fail with `Task timed out` instead. All three have the same shape: 100 `sp`
processes hot-looping on a port under an inner `(m/timeout ... 100)`, inside a
task the harness cancels at 150ms and fails at 200ms.

### What the measurements say

The cost is **cancellation wind-down**, not steady-state throughput. Measured as
the delay between the inner 100ms timeout firing and the join of the 100
processes finally settling, three runs each:

| Shape | Wind-down after the inner timeout |
|---|---|
| 100 hot processes, plain `sleep 0`, nothing compelled | **30-40 ms** |
| `holding` a 7-permit semaphore | 13-29 ms |
| `compel` + mailbox | 57-83 ms |
| `compel` + rendezvous | **143-193 ms** |

Against a total budget of 200ms of which the inner phase already spends 100, a
143-193ms wind-down does not fit. `rendezvous` passes when it passes only
because the harness's own 150ms cancel reaches the outer task while stragglers
are still unwinding.

Two earlier conclusions here were wrong, and are corrected rather than deleted
because both misdirected the search:

- **"Cancelling 100 `sp` processes takes 1 ms."** That measured processes
  sitting parked. Cancelling 100 processes that are hot-looping takes 30-40 ms
  with no `compel` involved at all -- so the baseline, not `compel`, is already
  most of the mailbox overrun.
- **"The hangs came from concurrent background jobs competing for carriers."**
  They did not. The hang reproduces on an otherwise idle machine, running
  `ebb.core-test` alone, one run at a time.

Ruled out by measurement: primitive throughput (a rendezvous handoff is 13.5 us,
a `(? (sleep 0))` park ~27 us), and `Event` identity under jolt's
value-comparing `compare-and-set!` -- `deftype` equality on jolt is identity, as
the CAS ports assume.

Not yet explained: why the wind-down costs ~0.7 ms per process when a park costs
27 us, and why the `mailbox` case tips over into a livelock. A standalone
reproducer of the `mailbox` test alone did not hang in 25 runs, so the
surrounding suite is part of the trigger.

Neither test has been given a looser budget. A test that is far inside its
budget and still trips is reporting something real, and widening it would hide
the next regression in the same place.

## Tests not ported, and why

| Namespace | Why |
|---|---|
| `pub_test`, `sub_test` | Reactive Streams, out of scope. |
| `tck.cljc` | Ported as `ebb.tck`, but `check!` is rewritten -- see below. `task-spec`, `flow-spec`, `deftask` and `defflow` are verbatim. |
| `test_dev.clj` | A kaocha watch entry point. |

### The tck driver had to change, and why that is a real difference

Missionary's `check!` splits events two ways: raised on the driving thread means
reentrant, so handle it depth-first; raised anywhere else means asynchronous,
so queue it. That split works because a missionary process runs *on the caller's
thread*, so a synchronous completion is same-thread by construction and beats a
`:timeout 0` without a race.

Ebb runs `ap` and `cp` bodies on an owner fiber (ADR-001), so the same
completion arrives from a different thread even though the driver was blocked
waiting for it. Real-time ordering is preserved; the thread identity no longer
reflects it. Two tests -- `relieve-input-failure` and
`observe-callback-nop-when-done`, both of which declare no `:timeout` and so get
0ms -- failed purely on that.

Ebb's `check!` therefore treats a timeout as the **lowest-priority** event: it is
delivered only when no real event is pending, which is what "nothing happened in
time" means anyway.

Two things that rule turns out to require, both learned by getting them wrong:

- **Wait until the nearest deadline, do not poll.** A polling version created a
  timeout channel per tick and flooded the runtime's single deadline heap --
  which is the same heap these tests measure against, so the harness perturbed
  what it was timing.
- **Re-poll the queue before accepting a timeout.** `alts!!` chooses at random
  among ready ports, so when a completion and the deadline are both ready it can
  hand back the deadline and drop a completion that is already queued. That was
  a ~25% flake on `reactor-delayed`, whose body actually completes in under a
  millisecond against a 10ms budget -- the failure looked like ebb being slow and
  was not.

## Implementation notes that are not divergences

These changed shape without changing behaviour, and are recorded so a reader
comparing the two sources is not surprised.

- **`Eduction`'s buffer grows by doubling.** The `.cljs` impl keeps a growable JS
  array and calls `.push`; a jolt array has a fixed length. Caught by the `cat`
  transducer, which emits more values than it consumes.
- **`Mailbox` uses one persistent vector**, not the two flipped JS arrays, which
  cannot survive a CAS retry.
- **Mutable top-level state is an atom.** `Propagator`'s rank counter and
  `Reactor`'s `current`/`delayed` are plain `def` + `set!` in ClojureScript,
  which jolt (like Clojure) rejects.
- **`Event` is ported from the Java**, not the `.cljs` closure-in-a-set, which
  has no stable identity to remove on a CAS retry.
- **Combinators loop rather than map.** Realizing a lazy seq takes a counted
  lock, and jolt forbids parking under one, so `(dorun (map-indexed ...))` over
  child tasks deadlocks. See ADR-001 rule 4.
