# ebb

A port of [missionary](https://github.com/leonoel/missionary) to
[jolt](https://github.com/jolt-lang/jolt) — pure Clojure, no JVM, no
ClojureScript, no `cloroutine`.

Missionary is a functional effect system for Clojure: composable **tasks** (one
value, eventually) and **flows** (many values, backpressured), with real
cancellation and glitch-free dataflow propagation. Ebb is that system running on
jolt's Chez Scheme runtime, on fibers.

```clojure
(require '[ebb.core :as m])

;; a task: run two sleeps concurrently, take both
(m/? (m/join vector (m/sleep 100 :a) (m/sleep 200 :b)))
;=> [:a :b]

;; a flow: for each of 1..3, emit it twice, one every 10ms
(m/? (->> (m/ap (let [x (m/?> (m/seed [1 2 3]))]
                  (m/? (m/sleep 10))
                  (m/amb x x)))
          (m/reduce conj [])))
;=> [1 1 2 2 3 3]
```

**Status: feature-complete, with one open defect.** Every missionary
implementation in scope is ported — the three process primitives (`sp`, `ap`,
`cp`), the propagator and reactor, and every port and flow operator. Ebb runs
**missionary's own test namespaces**, edited only where
[doc/conformance.md](doc/conformance.md) says so: **225 tests, 265 assertions**.

The defect is cancellation wind-down. Three tests cancel 100 hot-looping
processes at once; that takes 30–40ms even with nothing compelled, and up to
193ms through a compelled rendezvous, against budgets that assume far less.
About one run in ten of `ebb.core-test` does not merely overrun but **hangs**,
spinning every carrier. Measurements and what they rule out are in
[doc/conformance.md](doc/conformance.md#known-defect-the-suite-can-hang-and-three-tests-overrun-their-budgets);
the bug is `ebb-8nq.23`. Do not treat ebb as production-ready until it is
closed.

## Install

Ebb needs [jolt](https://github.com/jolt-lang/jolt) (v0.8.1 or newer) and
nothing else — no Chez, no JVM, no C toolchain:

```bash
curl -sL https://raw.githubusercontent.com/jolt-lang/jolt/main/install | bash
```

Then depend on ebb as a git library, pinning a commit:

```clojure
{:deps {jlt-commons/ebb {:git/url "https://github.com/jlt-commons/ebb"
                         :git/sha "<full-sha>"}}}
```

There is no release on Clojars yet.

## What differs from missionary

Two differences you will actually notice; the exhaustive list is
[doc/conformance.md](doc/conformance.md), and anything not listed there is a
bug.

**`?` parks at any call depth.** Cloroutine's transform is lexical, so in
missionary `?` must appear syntactically inside the `sp` body and a helper
function cannot park. Ebb's `sp` is a fiber with a real stack:

```clojure
(defn helper [ms v] (m/? (m/sleep ms v)))   ; an ordinary fn that parks
(m/? (m/sp (inc (helper 10 5))))            ;=> 6
```

Missionary code keeps working here; ebb code may not port back.

**`Cancelled` is a value, not a class.** Jolt has no user-defined classes, so
ebb throws an `ex-info`. Test it with `m/cancelled?`, not `instance?`. This is
the only edit most ported tests needed.

Deliberately absent: `via`/`blk`/`cpu` (JVM executors — jolt has fibers, and `?`
parks anywhere) and `publisher`/`subscribe` (Reactive Streams, out of scope).

## Tests

```bash
bin/test                    # the whole suite
bin/test ebb.rdv-test       # one namespace
```

Test namespaces are discovered by scanning `test/ebb/*_test.clj`, so adding a
file is enough. `bin/port-test` and `bin/port-impl` mechanically translate more
of missionary's sources, warning on the constructs that need a human.

## Documentation

- [doc/conformance.md](doc/conformance.md) — every observable difference from
  missionary. Exhaustive by intent, following jolt's own
  `known-divergences.edn` pattern.
- [doc/adr/001-fiber-affinity.md](doc/adr/001-fiber-affinity.md) — the design,
  and why it is shaped this way. Five rules, each one earned by a bug.
- [doc/evaluation.md](doc/evaluation.md) — the feasibility study the port was
  built on, with its wrong turns corrected inline.

## Why "ebb"

It suggests a natural, rhythmic flow — much like data moving through a reactive
system. One punchy syllable, memorable, and not taken by an existing library.

## License

[EPL 2.0](LICENSE), the same license as missionary and jolt. Files derived from
missionary carry a header naming the source they came from; see
[NOTICE](NOTICE).
