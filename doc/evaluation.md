# Feasibility evaluation

Written before the port started, to decide whether it was possible at all. Kept
because its reasoning is the reason the code is shaped the way it is, and
because a reader asking "why `call/cc` and not a macro transform?" is asking a
question this document answers.

Its conclusions held. The three places it got the *remedy* wrong are corrected
inline, marked **Correction**. The discipline that grew out of the
fiber-affinity hazard it identifies is
[adr/001-fiber-affinity.md](adr/001-fiber-affinity.md), and the final list of
differences from missionary is [conformance.md](conformance.md).

## Verdict

Feasible, and in one respect the port comes out *better* than the original.
Missionary is ~3,750 lines split into two layers, and they carry very different risk:

| Layer | Lines | Port risk |
|---|---|---|
| Callback plumbing — tasks, flows, ports, the propagator DAG | ~2,100 | **Low.** Mechanical translation of the `.cljs` impls. |
| Sequential/Ambiguous/Continuous — the `sp`/`ap`/`cp` coroutines | ~650 | **Medium.** Needs a replacement for `cloroutine`; jolt offers two, both validated below. |
| `via`/`blk`/`cpu`, `publisher`/`subscribe`, Pub/Sub/Thunk | ~250 | **Dropped.** JVM executors → jolt fibers; reactive-streams is out of scope. |

Missionary's two hard dependencies both disappear:

- **reactive-streams** — dropped per scope. `Pub`, `Sub` and the TCK go with it.
- **cloroutine** — the load-bearing one. Missionary needs it because the JVM
  cannot capture a stack, so `sp`/`ap`/`cp` bodies are macro-rewritten into
  clonable state machines. Jolt runs on Chez, which captures stacks natively.

## What was actually verified on jolt

Every claim below was run, not inferred. Probes are in the session scratchpad;
the surviving slice is in `src/`.

**The runtime carries the primitives the `.cljs` impls assume.** `deftype` with
`^:unsynchronized-mutable` fields, external `set!` on `.-field`, multi-arity
`clojure.lang.IFn` and `IDeref` participation on a `deftype`, `reify`,
`object-array`/`aset`, `##Inf`, `iterator`/`hasNext`/`next`, `locking`,
`transient` sets. The `.cljs` impls are the right base — the `.clj` ones are
Java. Translation is: `IFn (-invoke …)` → `clojure.lang.IFn (invoke …)`,
`^:mutable` → `^:unsynchronized-mutable`, `catch :default` → `catch Throwable`,
`missionary.Cancelled` → an `ex-info` carrying `::cancelled`, `js/setTimeout` →
the runtime's deadline heap (`jolt-timer-at!` in `host/chez/java/async.ss`, the
same timer `core.async/timeout` rides on).

**Fibers give `sp` for free, and lift missionary's biggest ergonomic
restriction.** A jolt fiber parks a real stack: verified parking inside a nested
function call, and inside `try`/`finally`. Cloroutine's transform is lexical, so
in missionary `?` must appear syntactically inside the `sp` body — a helper
function cannot park. In ebb `sp` is a fiber and `?` parks at any depth:

```clojure
(defn nested [ms v] (m/? (m/sleep ms v)))   ; an ordinary fn that parks
(defn deeper [v] (inc (nested 10 v)))
(m/? (m/sp (deeper 5)))                     ;=> 6
```

That is `sp` done — no macro transform, no state machine, ~15 lines.

**`ap`/`amb` needs more than fibers, and Chez supplies it.** `?>` forks the
continuation once per value, so `Ambiguous` calls the coroutine's *clone*
arity (`((.-coroutine p) boot c)` in `backtrack`). You cannot clone a fiber's
stack. But raw Chez `call/cc` is reachable through `jolt.scheme/proc`, and it is
fully re-entrant — verified multi-shot resume, a `yield`-style generator, a
nested-and-`try`-wrapped yield, and the two-branch fork `amb` needs. It also
holds up **on a fiber, with a park inside the re-entered extent**, which was the
main thing that could have killed this approach:

```clojure
(def ccc (jolt.scheme/proc "call-with-current-continuation"))
(fib/spawn (fn []
  (let [saved (atom nil) out (atom [])]
    (let [x (ccc (fn [k] (reset! saved k) 1))]
      (a/<!! (a/timeout 5))                 ; park inside the resumed extent
      (swap! out conj x)
      (when (< x 3) (@saved (inc x))))
    @out)))                                 ;=> [1 2 3]
```

So `ap` can be built on native continuations rather than a 500-line macro
transform — simpler *and* more complete than cloroutine, which has to hand-compile
`try`/`catch`/`loop`/`case` into its state machine.

> **Correction.** True, but not with raw `call/cc`, which is what this spike
> used. `call/cc` captures the rest of the *program*, so resuming one re-runs
> the caller — in practice, the driver loop ran twice per fork. What `ap` needs
> is a *delimited* continuation. Chez 10.4.1 has no native prompts, so
> `ebb.impl.prompt` builds the narrow one ebb needs on top of `call/cc`. The
> conclusion holds — it is still far less code than a macro transform, and the
> prompt layer is ~170 lines — but "native continuations" understates the work
> by one layer. See ADR-001 §3.

## The hazards, scoped

**1. `finally` re-runs on continuation re-entry — but only in one narrow shape.**
Jolt compiles `finally` to a `dynamic-wind` after-thunk, so re-entering a
captured continuation re-runs it. Probing each shape separately narrows this a
long way:

| Shape | Result |
|---|---|
| Fiber park inside `try`/`finally` | **Safe** — runs exactly once. Jolt already strips finally winders on a park. |
| `catch` (no `finally`) around a fork | **Safe** — no winder; the handler still catches correctly across re-entry. |
| `finally` where the fork is *outside* the `try` | **Safe** — once per branch, correctly. |
| `finally` with a fork point *lexically inside* it | **Broken** — ran 3× for two yields. |

So the hazard is exactly: `finally`, plus a fork (`?>`/`?<`/`amb`), inside
`ap`/`cp`. It cannot touch `sp`, `?`, or any parking, and `catch` is untouched
throughout — which matters, because `(try (m/? …) (catch Cancelled _ (m/amb)))`
is the idiom missionary actually leans on.

How often does the broken shape occur? In missionary's entire `src` + `test` +
`doc` corpus, `catch` appears 12 times and user-facing `finally` appears **once**
— in `holding`, which wraps a park, not a fork:

```clojure
(defmacro holding [lock & body]
  `(let [l# ~lock] (? l#) (try ~@body (finally (l#)))))
```

**Ebb can detect the shape exactly, at runtime, with no change to jolt.** All six
Scheme bindings the fiber scheduler uses for this — `sa-current-winders`,
`sa-current-winders-set!`, `sa-winder-rtd`, `sa-winder-in`, `jolt-finally-in`,
`jolt-finally-marker` — resolve through `jolt.scheme/proc`. Walking the chain for
finally winders is ~15 lines of embedded Scheme and reports exactly:

```
bare              0
inside (finally)  1
nested            2
catch-only        0
```

That turns a silent-corruption bug into a thrown error at the fork site, which is
the jolt house rule ("never silently half-works"). No upstream PR is needed to
proceed.

The case for eventually landing a supported seam in jolt is real but weaker and
later: `jolt.scheme` is documented as host-specific, and a tree-shaken binary can
drop a binding out from under a library. Land that PR once ebb has proven which
seam it actually wants — read-only detection, or strip-and-restore — rather than
guessing now.

**2. Continuations are fiber-affine.** `jolt.continuations` documents that
resuming a continuation captured on another fiber *hangs the process with no
error*. Missionary's callbacks arrive on whatever thread completed the task, so
`ap` must funnel resumptions back onto the owning fiber via a mailbox. Missionary
processes are already single-threaded internally, so this is a design constraint,
not a blocker — but it has to be built in from the start, not retrofitted. This,
not `finally`, is the item to design around first.

> **Correction.** The priority call was right — affinity, not `finally`, is what
> the design had to be built around — but the mailbox was the wrong remedy.
> jolt's `alt-deliver!` already *is* that mailbox: it commits the value and
> enqueues the waiter on its carrier's run queue rather than running it inline,
> which is exactly the Erlang/CML rule that the waker never runs the waiter's
> code. Hand-writing one would have re-implemented jolt's scheduler a layer up.
> What ebb actually needed was the other half — a *synchronous* request/reply to
> a single owner fiber, `gen_server:call` rather than `!` — because missionary's
> contract is that starting a process, and completing a task it waits on, both
> run the body to its next park before returning. See ADR-001 §2.

A related caution: the `.cljs` impls assume a single thread and take no locks
(the `.clj` ones use `synchronized`). Jolt's fibers run on a multi-carrier pool,
so ported impls need `locking` where the Java versions synchronize.

> **Correction.** The hazard is real — a spike that ported `Dataflow` and
> `Rendezvous` from `.cljs` with unsynchronised mutable fields deadlocked as
> soon as both ran in one process — but `locking` is not the uniform answer.
> Missionary's Java impls split two ways, and the split is deliberate: a port
> whose whole state fits in one word drives a single `volatile` field with an
> `AtomicReferenceFieldUpdater` retry loop, and only the ones too large for that
> use `synchronized`. Ebb mirrors the split per impl — one atom and a
> `compare-and-set!` loop, or `locking` — rather than locking everything. See
> ADR-001 §1.

## What jolt's own `go` does not give us

`clojure.core.async/go` in jolt has a CPS pass (`sm-cps-go-body`) that rewrites
the rest of a body into a closure so a park it can see does not have to capture a
stack segment. It is tempting to extend it to cover `try`/`fn*`/`case*`. That is
not worth doing, and the pass's own comments say why:

- It is **opportunistic, not load-bearing**: "it can cost a park its cheap
  representation, never its correctness." Every bail already falls back to a
  correct fiber park. This is a *performance* pass.
- `case*` is **already unreachable** — jolt's `case` expands to `let*` + `if`. It
  is listed only to make the fallback true by construction.
- `fn*` **cannot be rewritten in principle**. A closure that parks may be called
  from anywhere; you would have to CPS every possible caller. Native fiber
  capture is the correct answer, and it is what happens today.
- `try` is genuinely doable — the comment notes "the rewrite would have to carry
  the exception frame explicitly" — but it buys speed on a subset of parks, in
  the hardest part of a CPS transform.

Most decisively: **it would not help ebb at all.** The pass emits a one-shot
closure handed to the scheduler via `__sm-take`/`__sm-put`. `ap` needs a coroutine
that can be *cloned* at a fork point. A one-shot closure chain is the wrong shape
no matter how many special forms it learns.

`core.async` and `core.async.flow` are the right substrate for channels, timers
and dispatch, but they do not replace missionary's operators. Missionary's value
is the continuous/discrete flow distinction, lazy sampling, and glitch-free DAG
propagation — `core.async.flow` is a static process graph over channels and
solves a different problem.

## Suggested sequencing

1. **Callback core** — `Dataflow`, `Rendezvous`, `Semaphore`, `Mailbox`, `Seed`,
   `Reduce`, `Never`, `Sleep`, `RaceJoin`.
2. **Flow operators** — `Zip`, `Latest`, `Sample`, `Relieve`, `Buffer`,
   `Eduction`, `Reductions`, `GroupBy`, `Watch`, `Observe`, `Store`. Pure
   translation, no new mechanism.
3. **`Propagator`** (563 lines) — `signal`/`stream`/`reactor`. The densest file,
   but still pure callback. Port before touching coroutines.
4. **`ap`/`cp` on `call/cc`**, behind a fiber-affine resumption mailbox, with a
   finally-winder guard at the fork site. No jolt change required to start.

Steps 1–3 are ~2,100 lines with no unsolved problems in front of them, and **28
of missionary's 33 test namespaces** exercise only that layer — the port can be
validated against missionary's own suite (plus its `lolcat` DSL, 402 lines of
portable Clojure) long before the coroutines land.

The port was built in this order, and all four steps are done.

