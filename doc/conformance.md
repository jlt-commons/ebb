# ebb vs. missionary: known divergences

Every difference between ebb and missionary that a user could observe. Following
jolt's own house pattern (`test/conformance/known-divergences.edn`), this is
meant to be exhaustive: a divergence that is not listed here is a bug, not a
feature.

Ebb runs **missionary's own test suites** for every ported namespace, edited
only where this file says so. That is the evidence behind most of these rows.

This file is the prose half of the registry. The other half is
[`divergences.edn`](divergences.edn), which `ebb.conformance-test` checks on
every `bin/test` run so the two cannot drift: a divergence whose pinning test is
renamed fails, an entry whose prose is deleted fails, and prose marking an id the
registry has never heard of fails. The `<!-- divergence: … -->` comments below
are those anchors — invisible when rendered, and not something you can reword by
accident.

What the gate cannot do is notice a divergence nobody wrote down. That needs
missionary as an oracle, and missionary needs a JVM, which is the one thing ebb
does without. A developer *can* run one — see `ebb-8nq.38`'s notes for the
recipe, and several entries here were settled that way rather than by reading
missionary's sources — but it can never be a condition of the suite.

## Deliberately absent

Nothing. Every public var of missionary's API is present, including
`via`/`via-call`/`blk`/`cpu` and `publisher`/`subscribe`.

## Behavioural differences

<!-- divergence: reactive-streams-is-a-protocol -->
**Reactive Streams is a protocol here, not a Java interface.** Missionary's
`publisher` returns an `org.reactivestreams.Publisher`; jolt has no Java
interfaces to implement, so ebb states the same three roles as protocols in
`ebb.rs` — `Publisher`, `Subscriber`, `Subscription`, with the same method
names and the same contract. The state machines are ports of `Pub.java` and
`Sub.java`, and missionary's own `pub_test` and `sub_test` run against them
with only the interface names swapped. Bridging to a real Reactive Streams
implementation means extending these protocols to it.

<!-- divergence: subscribe-publisher-round-trip -->
**`(subscribe (publisher f))` is not a supported round trip** — in ebb or in
missionary. `Pub` calls `on-next` synchronously from inside `request`, and
`Sub` calls `request` from inside `transfer`, so wiring one straight into the
other re-enters the flow notifier before the pending transfer has returned.
Each side is correct against a counterparty that does not do this.

<!-- divergence: cancelled-is-a-value -->
**`Cancelled` is a value, not a class.** Missionary throws
`missionary.Cancelled`; jolt has no user-defined classes, so ebb throws an
`ex-info` carrying `{:type ::ebb.impl.util/cancelled}`. Use `m/cancelled?`
rather than `(instance? Cancelled e)`. This is the only edit most ported tests
needed.

<!-- divergence: park-at-any-depth -->
**`?` works at any call depth — ebb is a superset here.** Cloroutine's transform
is lexical, so in missionary `?` must appear syntactically inside the `sp` body
and a helper function cannot park. Ebb's `sp` is a fiber with a real stack:

```clojure
(defn helper [ms v] (m/? (m/sleep ms v)))   ; an ordinary fn that parks
(m/? (m/sp (inc (helper 10 5))))            ;=> 6
```

Code written for missionary keeps working; code written for ebb may not port
back.

<!-- divergence: fork-in-finally-refused -->
**A fork inside a `try` with `finally` is refused.** Jolt compiles `finally` to a
`dynamic-wind` after-thunk, so re-entering a continuation re-runs it — measured
at N+1 runs for N branches, where N is correct. Rather than corrupt cleanup
silently, `?>`/`?<` throw `::ebb.impl.prompt/fork-in-finally` when the fork sits
lexically inside a `finally`'s `try`. Narrow in practice: `catch` is unaffected,
`sp` and every fiber park are unaffected (jolt strips finally winders on a
park), and in missionary's entire `src` + `test` + `doc` corpus `catch` appears
12 times against a single user-facing `finally`, which wraps a park, not a fork.

<!-- divergence: continuations-are-executor-affine -->
**Continuations are executor-affine.** A continuation may only be resumed on the
`(thread, fiber)` that captured it; anything else throws
`::ebb.impl.prompt/wrong-fiber` rather than hanging, which is what jolt does
unguarded. Ebb satisfies this itself — `ap` and `cp` processes marshal every
entry point to an owner fiber — so it is visible only to code reaching into
`ebb.impl.prompt` directly.

<!-- divergence: ap-cp-cost-a-hop -->
**`ap` and `cp` cost a scheduling hop per resumption**, because a callback
enqueues on the owner rather than resuming inline. `sp` does not.

## Host differences inherited from jolt

<!-- divergence: deftype-field-shadowing -->
- **A local may not share a name with a mutable `deftype` field** — a convention,
  no longer a known defect. It was recorded as one: four sites in `impl/` were
  fixed by renaming such locals to a `v-` prefix, and the symptom was a snapshot
  taken before a `set!` coming back holding the value `set!` wrote. On jolt
  v0.8.1 it **does not reproduce** — not at either site where it was confirmed
  (renaming back leaves the suite green), and not in eleven constructed shapes,
  including inside a `deftype` method, where field names really are implicitly
  bound. Either jolt fixed it, or the original diagnosis was confounded: both
  renames landed in the same commit as structural changes, and the symptom is
  indistinguishable from a lost read-modify-write under concurrency — the defect
  class `ebb-8nq.31`/`.32`/`.35` kept finding. The convention stays because it
  costs nothing; `ebb-8nq.24` carries the evidence, and says to rule out
  concurrency first if it ever comes back.

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

<!-- divergence: strings-are-codepoint-indexed -->
- **Strings are codepoint-indexed.** `(count "😀")` is 1.
<!-- divergence: cas-compares-by-value -->
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

## Known defects

The suite is **20 runs in 20 clean**. The two tests that used to flake sit
against 10ms budgets, and both now have headroom — `ebb-8nq.40` cut the cost of
starting an `ap` by about a third, which is most of what they were spending:

| | p50 | p90 | p99 | max | over 10ms |
|---|---|---|---|---|---|
| `reactor-delayed` | 4.72ms | 5.17ms | 8.10ms | 9.65ms | 0/400 |
| `ambiguous-eval-order` | 1.98ms | 2.22ms | 4.58ms | 9.90ms | 0/400 |

Neither has ever produced a wrong value; both were ebb being slow. No budget has
been widened. What remains, and what was fixed,
is worth stating precisely because most of it was found by measurement rather
than by reading.

### Fixed

| Defect | Cause |
|---|---|
| Suite hung ~1 run in 7, 6/6 at 16 carriers | `Sleep` completed twice — a non-atomic flag arbitrating the timer against the canceller — and two resumers then deadlocked on a single-cell gate. Found by enumerating interleavings ([model/](../model/README.md)), not by re-running. |
| `rendezvous` overran its 200ms budget | `race-join` started children in series and each start blocked until the child's first park: ~100ms for 100 children, and the spawn loop was uninterruptible meanwhile. Starts are now batched. |
| `reactor-delayed`, `reactor-double-sub` — no critical section | Propagation had **no critical section**, so two fibers could propagate one process and one nilled `.-current` while the other read its ranks. Now a claim/trampoline: commit under the process monitor, propagate outside it. |
| Silent hang under load | `ebb.impl.server` read `offer!` returning false as "retired" when it also means "full", and ran the thunk inline on the caller — resuming a continuation off its owner fiber. The mailbox is unbounded now. |
| `reactor-delayed`, 2 full-suite runs in 50 — a shared context | `ebb.impl.reactor`'s `current` and `delayed` were plain globals where `Reactor.java` uses a `ThreadLocal`, so a propagation on one fiber was in scope for every other: an event raised from a timer fiber took `event`'s in-context branch and read `(.-current ps)` — nilled on the propagation's way out — as a null `.-ranks`. Per-fiber was tried before and fails the other way, deterministically, because a reactor's `ap` runs on its own owner fiber. Neither unit is the `ThreadLocal`'s: what it delimits is the dynamic extent of the call, so both are now *carried locals* that travel with the `server/call!` handshake — and back, since `schedule` writes `delayed` from inside a notifier and the outermost `event` is what drains it. ADR-001 rule 6. |
| `ap` with high parallelism stalled ~1 run in 15 (`#108`'s shape) | `ebb.impl.reduce` carried the `.cljs` `busy` toggle and none of `Reduce.java`'s `synchronized (p)`. `(set! busy (not busy))` is a read and a write, so a notification raised from the `ap`'s owner fiber while the starting thread ran its own first round lost a flip: the reduction went idle with `result` still at the initial accumulator while its input sat notified with all 400 values queued. Arbitration is now a CAS claim on one atom — the monitor is unportable here, since `ready` derefs the input and that parks (ADR-001 rule 5). `ebb-8nq.32` audits the operators that still toggle unsynchronised. |
| Every other ported flow operator toggled `busy` unsynchronised | The same lost flip as `reduce` above, unfixed in nine more files (`ebb-8nq.32`). The claim protocol is now `ebb.impl.arbiter`, and `reductions`, `relieve`, `sample`, `buffer` and `group-by` all use it; `eduction` uses the atomic counter `Eduction.java` uses instead of a monitor; `watch`, `observe` and the parts of `sample`/`relieve`/`buffer`/`group-by` a claim cannot reach use `locking` over field updates with every deref and callback outside it; `continuous` and `affine` carry headers saying why their ports need no arbitration at all. Nothing in the suite reproduced any of them, so `test/ebb/stress_test.clj` was written to — its `racing` driver raises the input's next notification from an executor that is already running, released the instant the consumer takes a value, and it fails all five of the old implementations. |
| `ap`/`cp` notifiers blocked their caller | Every callback ebb handed out was a synchronous `server/call!`, which both protocols forbid: a notifier, a terminator, a canceller and both task continuations *must not block the calling thread* (`ebb-8nq.29`). They are `server/cast!` now — enqueue and return. What blocked the fix was not the runtime but lolcat; see below. |
| A cast lost when its owner fiber retired | Found by the fix above. A sender read `live`, the owner retired and took its last drain, and only then did the sender enqueue. A `call!` at least hangs its caller; a cast has nobody waiting, so the message just disappeared — a cancel that never happens and a process that never terminates. `ebb.impl.server` now sets `drained` *before* the final drain, so a sender that reads it runs its own message and one that does not has already enqueued for that drain. |
| Starting an `ap` or `cp` cost two handshakes | Not a defect, a cost, and the measurement that found it corrected an earlier wrong conclusion. Instrumenting one `(m/reduce conj [] (m/ap (m/?> (m/seed [1]))))` end to end showed 41µs for `server/spawn` and another 47µs for the `call!` that followed it, out of 336µs — because a real owner fiber is **cold**, parked on its channel, and waking it costs a full scheduling round trip rather than the ~27µs a hot benchmark loop measures. `ebb-8nq.36` had concluded marshalling was ~150µs of a ~5ms reactor run and therefore not the problem; on this shape it is about half. `server/start!` now spawns the owner fiber and runs the process's start-up on it as one handshake. Minimum of 45 measurements: **333µs → 218µs**. (`ebb-8nq.40`) |
| `sample` deref'd one input from two parties at once | `Sample.java` holds the process monitor across BOTH derefs of a slot — `dirty`'s eager discard and `transfer`'s resample — and ebb cannot hold anything across a deref, since for an `ap` input it parks (rule 5). The port let both run together: `mark!` answered "already dirty, go drain" *without clearing the flag*, so `resample!` took the still-dirty slot and deref'd alongside it. Every flow impl in ebb and in missionary assumes its transfer is never concurrent with itself — that assumption is what all those `busy` toggles are for — so this is the shape that corrupts them. Measured directly, by counting derefs inside the input at once: **2630 overlaps in 20 runs**, now 0. Each slot carries its own claim; a `dirty` that loses leaves its count behind for the holder and never waits, and a `resample!` that loses parks on a gate, which cannot deadlock because the holder never waits on it. The eager drain stays — it is what keeps a sampled input flowing between samples, and deferring it stalls `sample-async`. (`ebb-8nq.33`) |
| `sample` leaked its readiness sentinel into the combinator | Found by `stress_test`'s own sample case at about 1 run in 100, and only under load. `resample!` asked "is this slot dirty?" and read the slot's cached value in **two separate `locking` regions**, so a `dirty` landing in between made the read answer with the sentinel — the `args` array itself — which went straight into the combinator's arguments. A defect introduced by `ebb-8nq.32`'s own port, and the reason the file's header claims a snapshot prevents exactly this: the snapshot keeps `(vec args)` clean, but `resample!` was reading the slot directly. One region for both questions now. |
| `capture-context` scanned a registry on every hop | Not a defect, a cost: it is called five times per `server/call!` — the caller snapshots, the callee installs and restores, the reply snapshots, the answer installs — and each scan walked every carried registry with a native `current-fiber` call and a map lookup per registry. It answers nil for one atom read now (`carried-live`), which is the honest common case: nobody in the process is inside a reactor propagation. Worth ~10% of a hop, and it tightened the tail rather than the median — `reactor-delayed`'s over-budget rate went 4 in 400 to 1 in 400 and its p99 10.1ms to 6.5ms. Measuring this also **refuted** the reason it was looked at: marshalling is ~150µs of `reactor-delayed`'s ~5ms, and the prompt is 2–3µs, so neither is where the time goes. (`ebb-8nq.36`, continued in `ebb-8nq.40`) |
| `latest` hung the runtime on an input with no value | `m/latest` needs continuous inputs and must report `Uninitialized continuous flow` for one that has none, as `m/sample` reports `Undefined continuous flow`. Given an `ap` it never completed, and took the process with it — a main thread waiting on a promise elsewhere never got its answer either, so `bin/test` had to be killed. One constructor argument: `Latest.java` leaves `ps.step` **null** until `arity == initialized`, and that null is what says "no value yet" — `transfer` reports on it and `ready` drains instead of notifying. Ebb passed the step callback there, so the branch was unreachable and a malformed program went off into the sampling path. (`ebb-8nq.37`) |
| An error did not end an `ap`'s output | Handing an error downstream ends what an `ap` will deliver: `Ambiguous.java`'s `transfer` nils the notifier and drops head/tail in the same breath as returning it, so branches still in flight are discarded. Ebb delivered the error and carried on, and a consumer that did not cancel saw values arrive *after* `:done`. The suite never noticed because nearly every consumer in it is `m/reduce`, whose transfer catch cancels the input — the `ap` was being killed from outside and the difference never showed. What it does **not** do is force termination: an `ap` whose input has not terminated is not over just because it failed, which is what missionary's own `?>-cancel-with-crash` asserts and what a first attempt at this got wrong. Settled by running a nine-case matrix against missionary b.47 rather than by reading `Ambiguous.java`, whose two `c.done` sites look equivalent from outside. (`ebb-8nq.38`) |
| `?<` did not interrupt a branch that had forked | `?<` promises that each new value interrupts the branch in flight, and `ebb.impl.ambiguous` kept ONE cancel fn on the switch's own `Choice` — so it reached a task the branch was parked on directly, and nothing else. As soon as the branch forked (any `?>`, any `amb` of two or more forms) the park belonged to a descendant, the switch had no handle on it, and the replaced branch ran on beside its replacement. `Ambiguous.java` recurses over the whole subtree with `walk`/`cancel`; ebb keeps choices in one vector with a `parent` link, so `cut!` is a reverse filter over that — deepest first, since cancelling a flow can re-enter the pump. Every `Choice` records its park's canceller now, not only switches. Debounce, where the park sits directly under the switch, was correct throughout. (`ebb-8nq.34`) |
| `zip` and `latest` stalled outright | The same lost arbitration as the row above, in two files `ebb-8nq.32`'s audit never looked at — that pass grepped for a `busy` field, and `zip` counts with `pending` while `latest` uses a `sync` word. `Zip.java` holds a `ReentrantLock` across every `pending` update and both deref loops; the port had `(set! (.-pending ps) (dec (.-pending ps)))` in each input's notifier, so two inputs notifying at once lost a decrement and the counter never reached zero. `Latest.java` drives `sync` with `compareAndSet`; the port kept the state machine and dropped the CAS, and its `owner` field asked "is somebody in transfer" where the Java asks "am I" — under ADR-001 that is the wrong question, since a re-entrant notification arrives on the input's own owner fiber and an unrelated one on whichever thread swapped a reference. Measured: `zip` over two staggered `ap`s 33/200 stalled → 0/200; `latest` over two `m/watch`es advanced from two threads 15/40 → 0/40, and that case went from 30s to 76ms. `pending` is an atom, `sync` is CAS'd, `owner` holds an executor identity. (`ebb-8nq.35`) |
| `ebb.impl.propagator` kept one global `Context` | Also found by the fix above, and the same defect as `ebb-8nq.30` in a second file: `Propagator.java` has `ThreadLocal<Context> context = ThreadLocal.withInitial(Context::new)`, `Propagator.cljs` has a global, and ebb had the global. `enter` claims the outermost slot by finding `cursor` nil and `leave` releases it, so two propagations sharing one Context see each other's — the second is told it is nested, does not drain `delayed`, and the process waiting there is never ticked. Silent: the flow stops. Now two cells, because the `ThreadLocal` was doing two jobs — a per-executor Context reused between propagations (`withInitial`), and a *carried* local naming the one in flight (ADR-001 rule 6), set only between `enter` and its matching `leave` so that `cast!` stays a cast the rest of the time. `(m/reduce f nil (m/signal (m/ap (m/? m/never))))` hung 9 runs in 300 on the two of these together. |

### Open

Nothing is currently known to be wrong. What is left is a constraint rather
than a defect:

- **Inside a reactor, `ap`/`cp` callbacks are still synchronous.** `server/cast!` degrades to `call!` when the caller has ambient context in scope, because a carried local stands in for a `ThreadLocal` and the callee's writes have to be visible to the caller on return — a cast has no reply to bring them back in. ADR-001 rule 6.

No test budget has been widened. A test far inside its budget that still trips
is reporting something real.

## Checked against missionary's issue tracker

`test/ebb/missionary-issues-test` runs the repros from missionary's issues, so
a behaviour that is right today stays right. Three kinds:

**Regressions — closed upstream, ebb has the fix.** `#74` zip terminating on
the shorter input; `#126` `latest` with zero inputs; `#125` `signal` reporting
an uninitialised flow instead of leaking; `#115` a sleep spawned from another
sleep's termination callback not running user code on an interrupted thread.

**Open upstream, and ebb does not have the bug.** `#116` — cancelling an `ap`
propagates into every child branch, which missionary does not do. `#85` —
posting to a mailbox returns nil, as documented. `#81` — a throw out of a timer
callback kills missionary's single scheduler thread and deadlocks every later
`m/sleep`; ebb runs each timer callback on its own fiber, so it is contained to
that fiber and reported by name. (`(m/? (m/ap …))`, the expression the issue
uses to provoke it, still never completes under ebb: it applies a flow where a
task is expected, so the callback that would have completed it is the one that
threw. That is the malformed call, not the timer, and the test asserts the
difference.) `#108` — `(m/?> ##Inf …)` over
many tasks is correct and roughly linear at 400; the reported knee is higher,
and `ebb-8nq.25` tracks finding it. This is the test that caught the lost
notification in `reduce` above, which is why it is kept at a size that costs
the suite something.

**Open design questions, where matching missionary is the answer.** `#111` —
`(m/sem 0)` and `(m/sem -1)` construct, and acquiring blocks. `#82` —
cancelling a `via` whose body catches `Throwable` still *succeeds*, with
whatever the handler returned; that is correct, because cancellation is
cooperative and a body that swallows the interrupt has not been cancelled.
`#131` — arity 2 of `reduce` and `reductions` seeds by calling the reducing
function with no arguments rather than taking the first element, as
`clojure.core` would; ebb matches, and the test is what will say ebb has to
change if missionary does.

<!-- divergence: watch-derefs-on-transfer -->
**Open upstream, and ebb fixes it.** `#89` — `m/watch` takes the new state from
the watch callback's fourth argument, and references do not order their watches,
so two concurrent `swap!`s can reach the process in the wrong order and the later
state is lost. Ebb had it too, and about fifteen times more often: 2 wrong in 40
against missionary b.47's 1 in 300 on the issue's own repro, a fiber pool spacing
the two `via cpu` callbacks further apart than a thread pool does. Ebb now takes
the fix the maintainer states in the issue — ignore the callback's value, deref
the reference on transfer — and is 0 in 300. **This is a deliberate divergence**:
`m/watch` no longer reports the states its callbacks carried but whatever the
reference holds when the consumer gets there, so it skips more intermediate
states, and a change landing between the readiness mark and the deref can
produce the same value twice. Losing the *last* state is what it no longer does.
See `ebb.impl.watch`'s header for why the mark must precede the deref.

**Open upstream, and ebb answers the question.** `#127` — `m/?` called from a
plain function that a coroutine calls. Missionary calls this undefined and it
throws, because cloroutine rewrites `?` at the syntactic breakpoint and a call
one frame down finds no coroutine state. Ebb has no coroutine to rewrite: an
`sp` body owns a fiber and `?` parks it, so depth is irrelevant and it simply
works — the issue's own option 2, decided by the runtime rather than by us. See
*`sp` is a superset* above.

**Regressions checked and present.** `#69` continuous operators skipping work on
duplicates (`latest` does not recombine when nothing changed); `#75` a
`group-by` consumer terminating immediately on input crash; `#130` two
consecutive derefs of a signal with no ready callback between them. All three
were compared against missionary b.47 directly rather than reasoned about.

**No repro to run.** `#94` (make `ap`'s switch continuous-time like `cp`'s),
`#101` (fairness for `sem`/`dfv`/`mbx`/`rdv`), `#102` (bound `mbx` and `blk`)
are upstream design requests with no observable behaviour to pin.

`#86` — `(ap (?< input) (try (loop [] (amb (? never) (recur))) (catch Cancelled _ (amb))))`
infinite-loops in missionary on the second input, because it resumes the `amb`'s
second alternative after the `Cancelled` has already unwound past it into the
catch. Ebb does not: a throw abandons the continuation the fork would have
resumed. The correct trace has **no value in it at all** — `amb` is sequential,
its first alternative is `(? never)`, which neither produces nor terminates, so
the second can never start; a branch here can only end by being cancelled, and a
cancelled branch evaluates to `(amb)`. The flow's one and only notification is
the one cancellation produces, and the transfer answering it throws `Cancelled`
and ends the flow. The `#(prn :ready)` consumer in the issue never transfers,
which is why the trace originally recorded against this looked truncated.

## Tests not ported, and why

| Namespace | Why |
|---|---|
| `tck.cljc` | Ported as `ebb.tck`, but `check!` is rewritten -- see below. `task-spec`, `flow-spec`, `deftask` and `defflow` are verbatim. |
| `test_dev.clj` | A kaocha watch entry point. |

<!-- divergence: tck-timeout-is-lowest-priority -->
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

<!-- divergence: lolcat-waits-for-declared-events -->
### lolcat had to change too, for the same reason

`lolcat.core/call` runs a function and then requires every event that function
was *declared* to raise to have already been raised. That is only true when the
callbacks run inline on the caller, which is missionary's model and not ebb's:
under ADR-001 rule 2 every callback an `ap` or `cp` hands out is a `cast!` onto
its owner fiber, so the event lands a moment after the call returns.

Each `call` now carries a promise, delivered when the last of its declared events
is consumed, and `call-attempt` waits on it for up to `*event-timeout*` (2s)
before it checks. What is relaxed is only **when**: the same events, in the same
declared order, one instruction later instead of before the call returns. A call
with nothing outstanding never touches the promise -- which matters, because
`observe_test`'s `overflow` runs one with the thread's interrupt flag
deliberately set, and a blocking deref there is observable.

The cost is that a genuinely missing event now takes 2s to report instead of
being instant. That bought 8 of the 9 `ap_test`/`cp_test` cases that made
`ebb-8nq.29` look unfixable.

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
