# Contributing to ebb

Ebb ports [missionary](https://github.com/leonoel/missionary) to
[jolt](https://github.com/jolt-lang/jolt). Most changes are ports rather than
designs, so upstream behaviour is the specification and missionary's own tests
are the gate.

## Build and test

Ebb is a jolt project. There's no build step, no JVM and no Chez install. You
need `jolt` on PATH, v0.8.1 or newer.

```bash
bin/test                    # the whole suite
bin/test ebb.rdv-test       # one namespace
```

Test namespaces are found by scanning `test/ebb/*_test.clj`, so adding a file is
enough. Nothing to register. CI runs the same `bin/test` through
`.github/workflows/tests.yml`.

`bin/port-impl` and `bin/port-test` translate more of missionary mechanically.
Both warn on the constructs a human has to look at.

## Layout

- `src/ebb/core.clj` holds the entire public API. Everything else is `^:no-doc`.
- `src/ebb/impl/*.clj` is one namespace per missionary impl, each with a
  provenance header naming the source it derives from.
- `test/ebb/*_test.clj` is missionary's own test namespaces, edited only where
  `doc/conformance.md` records a divergence.
- `test/lolcat/` is missionary's concatenative test DSL, ported near verbatim.

## The six rules

The design lives in `doc/adr/001-fiber-affinity.md`. Read it before changing
anything under `impl/`. Most of these rules are jolt-specific enough that
ordinary Clojure instincts get them wrong.

1. Shared components take their logic from the `.cljs` impl but mirror the
   **Java** impl's synchronisation, which means a CAS on one atom or `locking`.
   Pull-loop operators use the claim protocol in `ebb.impl.arbiter`.
2. `sp`, `ap` and `cp` each own one fiber, served by `ebb.impl.server`. Anything
   the process needs an answer from is a `call!`, so that covers the transfer
   and the start. Anything the process hands out is a `cast!`, because a
   notifier, a terminator, a task continuation and a canceller all must not
   block the calling thread.
3. `ap` and `cp` capture through `ebb.impl.prompt`, never raw `call/cc`.
4. A process's prompt is keyed by executor, never held in a global cell.
5. Never invoke a task inside a locked region. That includes the implicit lock
   nobody expects, since realizing a lazy seq takes a counted lock and jolt
   won't park under one. Port missionary's loops as loops rather than as `map`.
6. Ambient context travels with the handshake and back again, never through a
   global and never per-fiber. See `ebb.impl.util/carried-local`. The reactor's
   `current` and `delayed` work this way, and so does the propagator's
   `Context`.

## Conventions

**Record every observable difference in both halves of the registry.**
`doc/conformance.md` is the prose and `doc/divergences.edn` is the machine
readable index. A difference missing from them is a bug rather than a feature,
and writing it down is part of the change that introduces it.
`ebb.conformance-test` checks the two against each other on every `bin/test`
run, so a new entry needs a `<!-- divergence: id -->` anchor in the prose. It
also needs either a `:pinned-by` test var that resolves or an `:unpinned`
reason explaining why no test can pin it.

**Use missionary as an oracle when behaviour is in question.** On a machine with
a JVM you can compile missionary's Java impls out of a checkout and hand the
same program to both sides.

```bash
cd missionary && CP=$(clojure -Spath)
javac --release 21 --enable-preview -cp "$CP" -d target/classes $(find java -name '*.java')
clojure -J--enable-preview -M probe.clj
```

Reading `Ambiguous.java` is a poor substitute. Several conformance entries were
settled by running the program after reasoning about the source gave the wrong
answer. The oracle needs a JVM, so treat it as a developer tool rather than a
condition of `bin/test`.

**Name your sources.** A ported file's header records which missionary file the
logic came from and which one the synchronisation came from. Under rule 1 those
are usually two different files.

**Port missionary's logic rather than reinventing it.** The tests are
missionary's too, so a rewrite that passes is still a divergence.

**Prefix a local that would shadow a mutable `deftype` field with `v-`.** The
hazard is a snapshot taken before a `set!` coming back holding what the `set!`
wrote. If you ever see that, suspect concurrency first. A lost read-modify-write
looks identical from the outside and is far more likely.

**Don't widen a timing budget to make a test pass.** A test sitting far inside
its budget that still trips is reporting something real.

**Keep the license at EPL 2.0**, matching missionary and jolt. See `NOTICE`.
