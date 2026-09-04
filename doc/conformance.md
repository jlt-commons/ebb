# ebb vs. missionary: known divergences

Every difference between ebb and missionary that a user could observe. Following
jolt's own house pattern (`test/conformance/known-divergences.edn`), this is
meant to be exhaustive: a divergence that is not listed here is a bug, not a
feature.

Ebb runs **missionary's own test suites** for every ported namespace, edited
only where this file says so. That is the evidence behind most of these rows.

## Deliberately absent

Nothing. Every public var of missionary's API is present, including
`via`/`via-call`/`blk`/`cpu` and `publisher`/`subscribe`.

## Behavioural differences

**Reactive Streams is a protocol here, not a Java interface.** Missionary's
`publisher` returns an `org.reactivestreams.Publisher`; jolt has no Java
interfaces to implement, so ebb states the same three roles as protocols in
`ebb.rs` — `Publisher`, `Subscriber`, `Subscription`, with the same method
names and the same contract. The state machines are ports of `Pub.java` and
`Sub.java`, and missionary's own `pub_test` and `sub_test` run against them
with only the interface names swapped. Bridging to a real Reactive Streams
implementation means extending these protocols to it.

**`(subscribe (publisher f))` is not a supported round trip** — in ebb or in
missionary. `Pub` calls `on-next` synchronously from inside `request`, and
`Sub` calls `request` from inside `transfer`, so wiring one straight into the
other re-enters the flow notifier before the pending transfer has returned.
Each side is correct against a counterparty that does not do this.

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

- **A local may not share a name with a mutable `deftype` field.** In some
  shapes such a local reads the FIELD rather than the binding, so a snapshot
  taken before a `set!` comes back holding the value that `set!` wrote. The
  classic missionary idiom

  ```clojure
  (let [pending (.-pending ps)]     ; snapshot
    (set! (.-pending ps) nil)       ; clear
    (doseq [c pending] (c)))        ; ...iterates the CLEARED value
  ```

  is therefore unsafe as written, and every port must rename the local. This is
  not universal — small reproductions of the same shape behave correctly, so
  the trigger is narrower than "any shadowing" and has not been isolated — but
  it is real, and silent. It cost three ported tests and one intermittent
  reactor failure before it was found:

  | Where | What it did |
  |---|---|
  | `Sub.transfer`/`cancel` | dropped one value per stream, and never cancelled upstream |
  | `Reactor.transfer` | `ack` nils `.-value` when pending hits zero, so the last acking subscriber transferred `nil` — an intermittent failure, since it depends on ack order |
  | `Ambiguous.cancel` | iterated the just-emptied park list, so no park-cancel ran |
  | `Propagator` | bufferized a just-nilled subscription |

  Ebb's convention is to prefix such locals `v-`. Worth reporting upstream; see
  `ebb-8nq.24`.

- **Strings are codepoint-indexed.** `(count "😀")` is 1.
- **`compare-and-set!` on an atom compares by value**, where JVM Clojure uses
  reference identity. Harmless for ebb's CAS ports — their states are sets and
  maps of `Event` deftypes, which have identity equality, and no `Event` is
  removed and re-added, so ABA is unreachable. **Any future CAS port over
  value-equal states must check this.**
- ~~**No thread interruption.**~~ **Withdrawn — this was wrong.** jolt
  implements the JVM interrupt protocol in full: `.interrupt`,
  `.isInterrupted`, `Thread/interrupted` (clearing), and an
  `InterruptedException` thrown out of a blocking wait. `observe_test`'s
  `interrupt-current!` really interrupts, as missionary's does. The only edit
  is that the test clears the flag in a `finally`: it is thread state, not test
  state, so left set it survives into the next namespace and kills the first
  blocking wait there.

## Known defect: three tests overrun their budget

`ebb.core-test`'s `rendezvous` fails on roughly 4 runs in 5, and
`reactor-delayed` occasionally. Everything else in the suite is stable, and the
suite **no longer hangs**. Tracked as `ebb-8nq.23`.

### The hang is fixed, and how it was found is worth repeating

The suite used to hang outright — about 1 run in 7 on the default 4 carriers,
and **6 times out of 6 on sixteen**. Hammering it located the namespace and
little else, because more runs only ever sample more schedules.

Two relational models under [`model/`](../model/README.md) enumerated *every*
interleaving instead, and between them explained it end to end:

- `ebb.impl.sleep` is the one task completed by two different threads — the
  runtime timer fires it, whoever cancels the process cancels it. The port took
  the `.cljs` logic, which arbitrates with a plain mutable flag because
  ClojureScript has one thread. **12 of 20 interleavings complete it twice.**
  This was an ADR-001 rule 1 violation: `.cljs` logic without `Sleep.java`'s
  synchronisation, which arbitrates under the scheduler lock.
- A doubly-completed task resumes its process twice. `ebb.impl.affine` kept the
  driver's gate in a single cell, and `deliver` on a realized promise is a
  no-op, so two resumers install two gates but produce one wake-up. **Every
  schedule strands a resumer** on a gate nobody opens — and a stranded resumer
  is a blocked carrier.

`Sleep` now claims completion under a lock, and the gate is a drained queue
rather than an overwritten cell. Hangs at 16 carriers went from 6/6 to **0 of
15**.

### What remains is arithmetic, not flakiness

`semaphore`, `rendezvous` and `mailbox` each run 100 `sp` processes hot-looping
on a port under an inner `(m/timeout ... 100)`, inside a task the harness fails
at 200ms. Wind-down after the inner timeout fires:

| Shape | Wind-down |
|---|---|
| 100 hot processes, plain `sleep 0`, nothing compelled | 43-55 ms |
| `holding` a 7-permit semaphore | 20-23 ms |
| `compel` + mailbox | 76-87 ms |
| `compel` + rendezvous | **120-124 ms** |

100ms of inner phase plus 120ms of wind-down does not fit in 200ms. That is not
a race; it is a throughput shortfall, and it is why `rendezvous` fails most
runs. Per process the wind-down is ~1.2ms, against a 27us park — so the cost is
in the handshake round trip, which is where to look next.

**The timeout rate went UP when the hang was fixed** (from ~2 runs in 15 to
~12). The likely reason is that the deadlock was masking work: a stranded
resumer meant a wind-down that never completed and so was never paid for.
Recorded rather than smoothed over, because it is the kind of thing that looks
like a regression and is not.

### Corrections to earlier conclusions here

Four, kept because each sent the search the wrong way:

- **"Cancelling 100 `sp` processes takes 1 ms"** measured *parked* processes.
- **"The hangs came from concurrent background jobs."** They did not.
- **"Scheduling jitter while tests share four carriers."** Refuted: more
  carriers made the hang far worse, not better.
- **"A data race we cannot localise."** It was two, and enumeration found both.

Ruled out by measurement: primitive throughput; `compare-and-set!` cost (flat
0.20us regardless of state size); `Event` identity under jolt's value-comparing
CAS (`deftype` equality is identity); process leakage between tests; and the
`ap`/`cp` prompt registries, which return to zero entries after every test.

Neither budget has been widened.

## Tests not ported, and why

| Namespace | Why |
|---|---|
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
