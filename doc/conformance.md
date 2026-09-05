# ebb vs. missionary: divergences

Every difference between ebb and missionary that a user could observe. This list
is meant to be exhaustive, following jolt's own `known-divergences.edn` pattern.
A divergence missing from here is a bug rather than a feature.

Ebb runs missionary's own test suites for every ported namespace, edited only
where this file says so. That's the evidence behind most of what follows.

[`divergences.edn`](divergences.edn) is the machine readable half of this
registry, and `ebb.conformance-test` checks the two against each other on every
`bin/test` run. The `<!-- divergence: … -->` comments below are the anchors it
matches on. They don't render, so you can't reword one by accident.

The gate can't notice a divergence nobody wrote down. Catching those needs
missionary itself as an oracle, which needs a JVM. See CONTRIBUTING.md for how
to run one.

## Deliberately absent

Nothing. Every public var of missionary's API is present, including
`via`, `via-call`, `blk`, `cpu`, `publisher` and `subscribe`.

## Behavioural differences

<!-- divergence: reactive-streams-is-a-protocol -->
**Reactive Streams is a protocol here rather than a Java interface.**
Missionary's `publisher` returns an `org.reactivestreams.Publisher`. Jolt has no
Java interfaces to implement, so ebb states the same three roles as protocols in
`ebb.rs`: `Publisher`, `Subscriber` and `Subscription`, with the same method
names and the same contract. The state machines are ports of `Pub.java` and
`Sub.java`, and missionary's own `pub_test` and `sub_test` run against them with
the interface names swapped. Bridging to a real Reactive Streams implementation
means extending these protocols to it.

<!-- divergence: subscribe-publisher-round-trip -->
**`(subscribe (publisher f))` isn't a supported round trip**, in ebb or in
missionary. `Pub` calls `on-next` synchronously from inside `request`, and `Sub`
calls `request` from inside `transfer`. Wiring one straight into the other
re-enters the flow notifier before the pending transfer has returned. Each side
is correct against a counterparty that doesn't do this.

<!-- divergence: cancelled-is-a-value -->
**`Cancelled` is a value rather than a class.** Missionary throws
`missionary.Cancelled`. Jolt has no user-defined classes, so ebb throws an
`ex-info` carrying `{:type ::ebb.impl.util/cancelled}`. Use `m/cancelled?`
instead of `(instance? Cancelled e)`. This is the only edit most ported tests
needed.

<!-- divergence: park-at-any-depth -->
**`?` works at any call depth, so ebb is a superset here.** Cloroutine's
transform is lexical, which means missionary needs `?` to appear syntactically
inside the `sp` body and a helper function can't park. Ebb's `sp` is a fiber
with a real stack.

```clojure
(defn helper [ms v] (m/? (m/sleep ms v)))   ; an ordinary fn that parks
(m/? (m/sp (inc (helper 10 5))))            ;=> 6
```

Code written for missionary keeps working. Code written for ebb may not port
back.

<!-- divergence: fork-in-finally-refused -->
**A fork inside a `try` with `finally` is refused.** Jolt compiles `finally` to
a `dynamic-wind` after-thunk, so re-entering a continuation re-runs it. That
measures at N+1 runs for N branches, where N is correct. Rather than corrupt
cleanup silently, `?>` and `?<` throw `::ebb.impl.prompt/fork-in-finally` when
the fork sits lexically inside a `finally`'s `try`. This stays narrow in
practice. `catch` is unaffected, and so are `sp` and every fiber park, since
jolt strips finally winders on a park.

<!-- divergence: continuations-are-executor-affine -->
**Continuations are executor-affine.** A continuation may only be resumed on the
executor that captured it. Anything else throws
`::ebb.impl.prompt/wrong-fiber` rather than hanging, which is what jolt does
unguarded. Ebb satisfies this itself, since `ap` and `cp` processes marshal
every entry point to an owner fiber, so it's visible only to code reaching into
`ebb.impl.prompt` directly.

<!-- divergence: post-drives-the-consumer-on-its-own-fiber -->
**Delivering to a parked process runs its continuation on its own fiber, not on
yours — so delivering from inside a lock the process needs is a deadlock.**
Missionary's ports call the waiting party's callback inline: `(mbx x)` runs the
fetching process's continuation on the posting thread, which is why in
missionary a `locking` block around the post is re-entered by the same thread
and survives. Ebb's port hands the value over and then, per ADR-001, *parks the
poster until that process is quiescent again* — parked on its next task, or
finished. The continuation is on another fiber, and a monitor the poster holds
is not re-entrant for it.

```clojure
(def lock (Object.))
(def mb (m/mbx))

;; a consumer that needs the lock after taking
((m/sp (let [v (m/? mb)] (locking lock (handle v)))) (fn [_]) (fn [_]))

(locking lock (mb :x))     ; missionary: fine. ebb: neither side ever returns.
```

Measured: the post never returns and the consumer never runs, whether the
poster is a plain thread or a fiber. It applies to every hand-off — `mbx`,
`dfv`, `rdv`, `sem`, and starting or cancelling a task — because they all
deliver the same way.

The rule is the one ADR-001 states for ebb's own internals, and it holds for
callers too: **do not invoke a task, or deliver to a port, from inside a
critical section.** Collect what you want to send while you hold the lock, and
send it after you have let go.

```clojure
(let [pending (locking lock (let [out (compute!)] (collect out)))]
  (doseq [x pending] (mb x)))       ; delivered with nothing held
```

<!-- divergence: ap-cp-cost-a-hop -->
**`ap` and `cp` cost a scheduling hop per resumption**, because a callback
enqueues on the owner instead of resuming inline. `sp` doesn't.

**Inside a reactor, `ap` and `cp` callbacks are synchronous.** `server/cast!`
falls back to `call!` when the caller has ambient context in scope. A carried
local stands in for a `ThreadLocal`, so the callee's writes have to be visible
to the caller on return, and a cast has no reply to bring them back in. See
ADR-001 rule 6.

## Host differences inherited from jolt

<!-- divergence: strings-are-codepoint-indexed -->
**Strings are codepoint-indexed.** `(count "😀")` is 1.

<!-- divergence: cas-compares-by-value -->
**`compare-and-set!` on an atom compares by value**, where JVM Clojure uses
reference identity. This is harmless for ebb's CAS ports, whose states are sets
and maps of `Event` deftypes with identity equality, and no `Event` is removed
and re-added, so ABA is unreachable. Any future CAS port over value-equal states
has to check this.

<!-- divergence: deftype-field-shadowing -->
**Prefix a local that would shadow a mutable `deftype` field with `v-`.** This
is a convention rather than a known defect. The hazard it guards against is a
snapshot taken before a `set!` coming back holding what the `set!` wrote.

```clojure
(let [pending (.-pending ps)]     ; snapshot
  (set! (.-pending ps) nil)       ; clear
  (doseq [c pending] (c)))        ; iterates which value?
```

On jolt v0.8.1 that reads correctly, and eleven constructed shapes of it read
correctly too. If you ever do see a snapshot come back wrong, suspect
concurrency first. A lost read-modify-write looks identical from the outside and
is far more likely.

## Checked against missionary's issue tracker

`test/ebb/missionary-issues-test` runs the repros from missionary's issues, so a
behaviour that's right today stays right.

| Issue | Where ebb stands |
|---|---|
| `#74` zip terminating on the shorter input | closed upstream, ebb has the fix |
| `#126` `latest` with zero inputs | closed upstream, ebb has the fix |
| `#125` `signal` reporting an uninitialised flow | closed upstream, ebb has the fix |
| `#115` a sleep spawned from a sleep's termination callback | closed upstream, ebb has the fix |
| `#69` continuous operators skipping duplicates | closed upstream, ebb has the fix |
| `#75` `group-by` consumers terminating on input crash | closed upstream, ebb has the fix |
| `#130` consecutive derefs of a signal | closed upstream, ebb has the fix |
| `#116` cancelling an `ap` reaching child branches | open upstream, ebb doesn't have the bug |
| `#85` `mbx` posting returns nil | open upstream, ebb doesn't have the bug |
| `#81` a throwing timer callback | open upstream, ebb contains it to one fiber |
| `#108` `?>` with unbounded parallelism | open upstream, ebb is correct and roughly linear at 400 |
| `#111` `sem` with a non-positive permit count | open upstream, ebb matches missionary |
| `#82` cancelling a `via` whose body swallows the interrupt | open upstream, ebb matches missionary |
| `#131` arity 2 of `reduce` and `reductions` | open upstream, ebb matches missionary |
| `#86` `?<` over `amb` with `recur` | open upstream, ebb terminates where missionary loops |
| `#94`, `#101`, `#102` | design requests with no observable behaviour to pin |

Two of these are worth spelling out.

<!-- divergence: watch-derefs-on-transfer -->
**`#89`, where ebb diverges deliberately.** `m/watch` in missionary takes the
new state from the watch callback's fourth argument. References don't order
their watches, so two concurrent `swap!` calls can reach the process out of
order and the later state is lost. Ebb takes the fix the maintainer states in
the issue, which is to ignore the callback's value and deref the reference on
transfer. So `m/watch` reports whatever the reference holds when the consumer
gets there rather than the states its callbacks carried. It skips more
intermediate states that way, and a change landing between the readiness mark
and the deref can produce the same value twice. What it no longer does is lose
the last state. `ebb.impl.watch`'s header explains why the mark has to precede
the deref.

**`#127`, where ebb answers an open question.** Calling `m/?` from a plain
function that a coroutine calls is undefined in missionary and throws, because
cloroutine rewrites `?` at the syntactic breakpoint and a call one frame down
finds no coroutine state. Ebb has no coroutine to rewrite. An `sp` body owns a
fiber and `?` parks it, so depth doesn't matter and this just works. That's the
issue's own option 2, settled by the runtime rather than by us.

## Tests not ported

| Namespace | Why |
|---|---|
| `tck.cljc` | Ported as `ebb.tck`, but `check!` is rewritten. `task-spec`, `flow-spec`, `deftask` and `defflow` are verbatim. |
| `test_dev.clj` | A kaocha watch entry point. |

<!-- divergence: tck-timeout-is-lowest-priority -->
### The tck driver

Missionary's `check!` splits events two ways. An event raised on the driving
thread is reentrant, so it gets handled depth-first, and one raised anywhere
else is asynchronous, so it gets queued. That split works because a missionary
process runs on the caller's thread, which makes a synchronous completion
same-thread by construction.

Ebb runs `ap` and `cp` bodies on an owner fiber, so the same completion arrives
from a different thread even though the driver was blocked waiting for it.
Real-time ordering still holds, but thread identity no longer reflects it.

Ebb's `check!` therefore treats a timeout as the lowest-priority event. It gets
delivered only when no real event is pending, which is what "nothing happened in
time" means anyway. Two things that rule requires:

- **Wait until the nearest deadline instead of polling.** A polling version
  creates a timeout channel per tick and floods the runtime's single deadline
  heap, which is the same heap these tests measure against.
- **Re-poll the queue before accepting a timeout.** `alts!!` chooses at random
  among ready ports, so with a completion and a deadline both ready it can hand
  back the deadline and drop a completion that's already queued.

<!-- divergence: lolcat-waits-for-declared-events -->
### lolcat

`lolcat.core/call` runs a function and then requires every event that function
was declared to raise to have already been raised. That holds only when the
callbacks run inline on the caller, which is missionary's model. Under ADR-001
rule 2 every callback an `ap` or `cp` hands out is a `cast!` onto its owner
fiber, so the event lands a moment after the call returns.

Each `call` now carries a promise, delivered once the last of its declared
events is consumed. `call-attempt` waits on that promise for up to
`*event-timeout*`, which defaults to 2 seconds, before it checks. What's relaxed
is only the timing. The same events still have to arrive, in the same declared
order, one instruction later instead of before the call returns. A call with
nothing outstanding never touches the promise, which matters because
`observe_test`'s `overflow` runs one with the thread's interrupt flag set and a
blocking deref there is observable.

The cost is that a genuinely missing event takes 2 seconds to report instead of
being instant.

## Implementation notes that aren't divergences

These changed shape without changing behaviour. They're recorded so a reader
comparing the two sources isn't surprised.

- **`Eduction`'s buffer grows by doubling.** The `.cljs` impl keeps a growable
  JS array and calls `.push`, but a jolt array has a fixed length.
- **`Mailbox` uses one persistent vector** instead of the two flipped JS arrays,
  which can't survive a CAS retry.
- **Mutable top-level state is an atom.** `Propagator`'s rank counter and
  `Reactor`'s `current` and `delayed` are plain `def` plus `set!` in
  ClojureScript, which jolt rejects the same way Clojure does.
- **`Event` is ported from the Java** rather than the `.cljs` closure-in-a-set,
  which has no stable identity to remove on a CAS retry.
- **Combinators loop rather than map.** Realizing a lazy seq takes a counted
  lock and jolt won't park under one, so `(dorun (map-indexed ...))` over child
  tasks deadlocks. See ADR-001 rule 5.
