# ebb: Guide

## Why this exists

`ebb` is a port of [missionary](https://github.com/leonoel/missionary) to
[Jolt](https://github.com/jolt-lang/jolt): the same composable **tasks** (one
value, eventually) and **flows** (many values, with backpressure), the same
real cancellation and glitch-free dataflow propagation, running on Jolt's
Chez Scheme runtime over fibers instead of the JVM. Every public var of
missionary's API is supported, including the three process primitives `sp`,
`ap` and `cp`, the propagator and reactor, every port and flow operator,
`via`/`blk`/`cpu`, and `publisher`/`subscribe`.

Missionary's own test namespaces run against ebb unmodified, except where
[conformance](conformance.md) says otherwise: 281 tests and 1187 assertions,
196 of those straight from missionary itself.

## What's here

- [Feasibility evaluation](evaluation.md) - written before the port started,
  to decide whether it was possible at all. Its conclusions held; the
  document is kept because its reasoning is why the code is shaped the way
  it is.
- [ADR-001: Fiber affinity](adr/001-fiber-affinity.md) - the design that
  grew out of the evaluation's central hazard. Six rules, and everything in
  `impl/` follows them.
- [Conformance](conformance.md) - every observable difference from
  missionary, exhaustive by design: a divergence missing from here is a bug
  rather than a feature. The machine-readable half,
  [divergences.edn](https://github.com/jlt-commons/ebb/blob/main/doc/divergences.edn),
  is what `ebb.conformance-test` checks this page against on every test run -
  read it on GitHub rather than here, since it's data, not a page.

## Install and test

Ebb needs Jolt v0.8.1 or newer. There's no release on Clojars yet; depend on
it as a git library by pinning a commit:

```clojure
{:deps {jlt-commons/ebb {:git/url "https://github.com/jlt-commons/ebb"
                         :git/sha "<full-sha>"}}}
```

```bash
bin/test                    # the whole suite
bin/test ebb.rdv-test       # one namespace
```

[CONTRIBUTING.md](https://github.com/jlt-commons/ebb/blob/main/CONTRIBUTING.md)
covers building, testing and porting, plus the conventions a change is
expected to follow. `model/README.md`, in the repo itself, holds core.logic
models that enumerate every interleaving of the concurrency design.
