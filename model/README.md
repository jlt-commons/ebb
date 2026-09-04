# Relational models of ebb's concurrency design

A concurrency bug that only appears on eight or more carriers is one the test
suite **samples**. Running the suite more times explores more schedules but can
never report that you have seen them all. These models use
[core.logic](https://github.com/clojure/core.logic) to enumerate *every*
interleaving of a small number of threads over a shared state, so a result here
is a proof about the model rather than evidence about a run.

```bash
bin/model            # all of them
bin/model sleep      # just one
```

They are not part of the library — `ebb` itself has no dependencies. core.logic
comes in under the `:model` alias in `deps.edn`.

## What they found

Ebb hung roughly one run in seven on four carriers, and **six times out of six
on sixteen**. Hammering the suite had located the failing namespace and not much
else. These two models, together, explain it end to end:

- **`sleep`** — `ebb.impl.sleep` is the one task completed by two *different*
  threads: the runtime timer fires it, and whoever cancels the process cancels
  it. The port took the `.cljs` logic, which arbitrates with a plain mutable
  flag because ClojureScript has one thread. Enumerated: **12 of 20
  interleavings complete the sleep twice**, once with success and once with
  failure. Missionary's `Sleep.java` arbitrates under the scheduler lock, so
  this was an ADR-001 rule 1 violation — `.cljs` logic without the Java's
  synchronisation.

- **`handshake`** — a doubly-completed task resumes its owning process twice.
  `ebb.impl.affine` kept the driver's gate in a single cell, and `deliver` on an
  already-realized promise is a no-op, so two resumers install two gates but
  generate one wake-up. Enumerated: **every schedule strands a resumer** on a
  gate nobody will ever open. A stranded resumer is a blocked carrier; a hundred
  processes winding down at once is every carrier at 100%.

Both are fixed — `Sleep` claims completion under a lock, and the gate is a
queue that is drained rather than a cell that is overwritten. The hang went from
6/6 at sixteen carriers to **0 of 15**.

## Reading them

Each model defines a `stepo` relation over `[action state state']`,
`ebb.model.interleave/schedules` produces every interleaving of the thread
programs, and `runo` threads a schedule through `stepo`. The models are
deliberately small: they cover the arbitration, not the whole impl. A model
that says "sound" is a statement about what it models.
