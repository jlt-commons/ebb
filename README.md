
## Ebb

Ebb suggests a natural, rhythmic flow, much like data moving through a reactive
system. It is a port of [missionary](https://github.com/leonoel/missionary) to
[Jolt](https://github.com/jolt-lang/jolt) written in pure Clojure.

Missionary is a functional effect system for Clojure. It gives you composable
**tasks**, which produce one value eventually, and **flows**, which produce many
values with backpressure. Both come with real cancellation and glitch-free
dataflow propagation. Ebb is that system running on Jolt's Chez Scheme runtime
using fibers.

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

Every public var of missionary's API is supported. That covers the three process
primitives `sp`, `ap` and `cp`, the propagator and reactor, every port and flow
operator, `via`/`blk`/`cpu`, and `publisher`/`subscribe`.

Ebb runs missionary's own test namespaces, edited only where
[doc/conformance.md](doc/conformance.md) says so. The suite is 281 tests and
1187 assertions, 196 of those tests coming straight from missionary.

## Install

Ebb needs Jolt v0.8.1 or newer.


Depend on ebb as a git library by pinning a commit:

```clojure
{:deps {jlt-commons/ebb {:git/url "https://github.com/jlt-commons/ebb"
                         :git/sha "<full-sha>"}}}
```

There's no release on Clojars yet.

## What differs from missionary

The exhaustive list lives in
[doc/conformance.md](doc/conformance.md), and four user visible
differences are as follows.

**`?` parks at any call depth.** Cloroutine's transform is lexical, so
missionary needs `?` to appear syntactically inside the `sp` body and a helper
function can't park. Ebb's `sp` is a fiber with a real stack, so depth doesn't
matter.

```clojure
(defn helper [ms v] (m/? (m/sleep ms v)))   ; an ordinary fn that parks
(m/? (m/sp (inc (helper 10 5))))            ;=> 6
```

Missionary code keeps working here, but Ebb code may not port back.

**Delivering to a parked process runs its continuation on its own fiber.**
Missionary calls the waiting party's callback inline, on the delivering thread.
Ebb hands the value over and parks the deliverer until that process is
quiescent again, so the continuation runs elsewhere — and a lock you are
holding is not re-entrant for it.

```clojure
((m/sp (let [v (m/? mb)] (locking lock (handle v)))) (fn [_]) (fn [_]))

(locking lock (mb :x))     ; missionary: fine. ebb: neither side ever returns.
```

Deliver with nothing held: collect inside the critical section, send after it.
This covers `mbx`, `dfv`, `rdv`, `sem` and task invocation alike.

**`Cancelled` is a value rather than a class.** Jolt has no user-defined
classes, so ebb throws an `ex-info`. Test it with `m/cancelled?` instead of
`instance?`.

**Reactive Streams is a protocol rather than a Java interface.** `publisher`
returns something implementing `ebb.rs/Publisher`, not
`org.reactivestreams.Publisher`. Same three roles and same contract, stated in
Clojure because Jolt has no Java interfaces. Missionary's own `pub_test` and
`sub_test` run against it with the interface names swapped.

## Tests

```bash
bin/test                    # the whole suite
bin/test ebb.rdv-test       # one namespace
```

## Documentation

- [doc/conformance.md](doc/conformance.md) walks through every observable
  difference from missionary.
- [doc/divergences.edn](doc/divergences.edn) is the same registry in machine
  readable form. `ebb.conformance-test` checks the two against each other on
  every `bin/test` run.
- [doc/adr/001-fiber-affinity.md](doc/adr/001-fiber-affinity.md) is the design.
  Six rules, and everything in `impl/` follows them.
- [doc/evaluation.md](doc/evaluation.md) is the feasibility study the port was
  built on.
- [CONTRIBUTING.md](CONTRIBUTING.md) covers building, testing and porting, plus
  the conventions a change is expected to follow.
- [model/README.md](model/README.md) holds core.logic models that enumerate
  every interleaving of the concurrency design.

## License

[EPL 2.0](LICENSE), the same license as missionary and jolt. Files derived from
missionary carry a header naming their source. See [NOTICE](NOTICE).
