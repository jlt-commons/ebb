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

**Status: feature-complete, with one open defect.** Every public var of
missionary's API is ported — the three process primitives (`sp`, `ap`, `cp`),
the propagator and reactor, every port and flow operator, `via`/`blk`/`cpu`,
and `publisher`/`subscribe`. Ebb runs **missionary's own test namespaces**,
edited only where [doc/conformance.md](doc/conformance.md) says so: **238
tests, 278 assertions**.

The suite is **14 runs in 15 clean, with no hangs**. Getting there fixed four
concurrency defects, three of them found by measurement or by enumerating
interleavings rather than by reading code — the story is in
[model/](model/README.md) and
[doc/conformance.md](doc/conformance.md#known-defects), which also lists the
three that remain open.

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

**Reactive Streams is a protocol, not a Java interface.** `publisher` returns
something implementing `ebb.rs/Publisher` rather than
`org.reactivestreams.Publisher` — same three roles, same contract, stated in
Clojure because jolt has no Java interfaces. Missionary's own `pub_test` and
`sub_test` run against it with only the interface names swapped.

Nothing else is missing: `via`/`via-call`/`blk`/`cpu` are present too, and
cancellation really does interrupt the evaluating thread.

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
- [CONTRIBUTING.md](CONTRIBUTING.md) — how to build, test and port; the
  conventions a change is expected to follow.
- [model/README.md](model/README.md) — core.logic models that enumerate every
  interleaving of the concurrency design. They found the two races behind a
  hang that re-running the suite could not localise.

## Why "ebb"

It suggests a natural, rhythmic flow — much like data moving through a reactive
system. One punchy syllable, memorable, and not taken by an existing library.

## License

[EPL 2.0](LICENSE), the same license as missionary and jolt. Files derived from
missionary carry a header naming the source they came from; see
[NOTICE](NOTICE).
