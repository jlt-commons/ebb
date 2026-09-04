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
changing anything in `impl/`. Its five rules exist because each one was learned
from a bug, and three of them are jolt-specific enough that ordinary Clojure
instincts get them wrong:

1. Shared components mirror the **Java** impl's sync strategy (CAS on one atom,
   or `locking`) while taking their logic from the `.cljs` impl.
2. `sp`/`ap`/`cp` each own one fiber, driven by synchronous handshakes
   (`gen_server:call`, not `!`). `ebb.impl.server` is that server.
3. `ap`/`cp` capture through `ebb.impl.prompt`, never raw `call/cc`.
4. A process's prompt is keyed by `(thread, fiber)`, never a global cell.
5. **Never invoke a task inside a locked region** — including the implicit lock
   nobody expects: realizing a lazy seq takes a counted lock, and jolt forbids
   parking under one. Port missionary's loops as loops, not as `map`.

## Conventions & Patterns

- **Every observable difference from missionary goes in `doc/conformance.md`.**
  That file is exhaustive by intent, following jolt's own
  `known-divergences.edn` pattern: a divergence that is not listed there is a
  bug, not a feature. Adding one is part of the change that causes it.
- **Ported files name their sources.** A header records which missionary file
  the logic came from and which one the synchronisation came from, since under
  ADR-001 rule 1 those are usually two different files.
- **Port missionary's logic rather than reinventing it.** The tests are
  missionary's too, so a rewrite that passes is still a divergence.
- **Never give a local the same name as a mutable `deftype` field.** In some
  shapes the local reads the *field*, so a snapshot taken before a `set!` comes
  back holding what the `set!` wrote — silently. Missionary's impls do this
  constantly (`(let [pending (.-pending ps)] (set! (.-pending ps) nil) ...)`),
  so it is the single most likely way to break a port. Prefix such locals
  `v-`. It has already cost four bugs, one of them intermittent; see
  `doc/conformance.md`.
- **License is EPL 2.0**, matching missionary and jolt. Keep it that way; see
  `NOTICE`.
- Timing-sensitive tests are not given looser budgets to make them pass. A test
  far inside its budget that still trips is reporting something real.
