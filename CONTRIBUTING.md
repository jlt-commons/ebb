# Contributing to ebb

Ebb is a port of [missionary](https://github.com/leonoel/missionary) to
[jolt](https://github.com/jolt-lang/jolt). Most changes are therefore *ports*
rather than designs: the upstream behaviour is the specification, and
missionary's own tests are the gate.

## Build & Test

Ebb is a jolt project: no build step, no JVM, no Chez install. It needs `jolt`
on PATH (v0.8.1 or newer).

```bash
bin/test                    # the whole suite
bin/test ebb.rdv-test       # one namespace
```

Test namespaces are discovered by scanning `test/ebb/*_test.clj`, so adding a
file is enough — nothing to register. CI runs the same `bin/test`
(`.github/workflows/tests.yml`).

Two porting helpers translate more of missionary mechanically and warn on the
constructs a human has to look at: `bin/port-impl` and `bin/port-test`.

## Architecture Overview

Ebb is a port of [missionary](https://github.com/leonoel/missionary) to jolt.

- `src/ebb/core.clj` — the entire public API. Everything else is `^:no-doc`.
- `src/ebb/impl/*.clj` — one namespace per missionary impl, each carrying a
  provenance header naming the missionary source it derives from.
- `test/ebb/*_test.clj` — **missionary's own test namespaces**, edited only
  where `doc/conformance.md` records a divergence.
- `test/lolcat/` — missionary's concatenative test DSL, ported verbatim.

The design is one document: **`doc/adr/001-fiber-affinity.md`**. Read it before
changing anything in `impl/`. Its six rules exist because each one was learned
from a bug, and most of them are jolt-specific enough that ordinary Clojure
instincts get them wrong:

1. Shared components mirror the **Java** impl's sync strategy (CAS on one atom,
   or `locking`) while taking their logic from the `.cljs` impl. The claim
   protocol every pull-loop operator uses is `ebb.impl.arbiter`.
2. `sp`/`ap`/`cp` each own one fiber, and `ebb.impl.server` is that server.
   What the process needs an answer from is a `call!` (`gen_server:call`) —
   the transfer, and the start. What it hands OUT is a `cast!` (`!`) — the
   notifier, the terminator, both task continuations, the canceller — because
   every one of those "must not block the calling thread".
3. `ap`/`cp` capture through `ebb.impl.prompt`, never raw `call/cc`.
4. A process's prompt is keyed by executor, never a global cell.
5. **Never invoke a task inside a locked region** — including the implicit lock
   nobody expects: realizing a lazy seq takes a counted lock, and jolt forbids
   parking under one. Port missionary's loops as loops, not as `map`.
6. Ambient context (the reactor's `current`/`delayed`, the propagator's
   `Context`) travels **with the handshake, both ways** — never a global, never
   per-fiber. `ebb.impl.util/carried-local`.

## Conventions & Patterns

- **Every observable difference from missionary goes in `doc/conformance.md`
  AND `doc/divergences.edn`.** They are two halves of one registry, following
  jolt's own `known-divergences.edn` pattern: a divergence that is not listed
  is a bug, not a feature, and adding one is part of the change that causes it.
  `ebb.conformance-test` gates them against each other on every `bin/test`, so
  a new entry needs a `<!-- divergence: id -->` anchor in the prose and either
  a `:pinned-by` test var that exists or an `:unpinned` reason saying why one
  cannot exist.
- **There is an oracle, and it is worth using.** missionary itself runs on a
  machine with a JVM: compile its Java impls out of a checkout and hand the
  same program to both sides.

  ```bash
  cd missionary && CP=$(clojure -Spath)
  javac --release 21 --enable-preview -cp "$CP" -d target/classes $(find java -name '*.java')
  clojure -J--enable-preview -M probe.clj
  ```

  Several entries in `doc/conformance.md` were settled that way after reasoning
  about `Ambiguous.java` gave the wrong answer twice. It needs a JVM, so it is
  a developer tool and never a condition of `bin/test`.
- **Ported files name their sources.** A header records which missionary file
  the logic came from and which one the synchronisation came from, since under
  ADR-001 rule 1 those are usually two different files.
- **Port missionary's logic rather than reinventing it.** The tests are
  missionary's too, so a rewrite that passes is still a divergence.
- **Prefix a local that would share a name with a mutable `deftype` field
  `v-`.** This was adopted as a workaround for a suspected jolt defect — the
  local reading the *field*, so a snapshot taken before a `set!` came back
  holding what the `set!` wrote. On jolt v0.8.1 that **does not reproduce**
  (`ebb-8nq.24`), and it may never have been a compiler bug: the symptom is
  indistinguishable from a lost read-modify-write under concurrency, which is
  the defect class that turned up all over the impls. The convention stays
  because it costs nothing and the shape is worth flagging — but if a snapshot
  comes back wrong, **suspect concurrency first**, not the compiler.
- **License is EPL 2.0**, matching missionary and jolt. Keep it that way; see
  `NOTICE`.
- Timing-sensitive tests are not given looser budgets to make them pass. A test
  far inside its budget that still trips is reporting something real.
