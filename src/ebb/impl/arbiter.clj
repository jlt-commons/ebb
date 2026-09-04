;; ebb.impl.arbiter -- the claim protocol, ADR-001 rule 1's CAS option.
;;
;; Derived from the `busy` toggle in missionary's .cljs impls and the process
;; monitor its .java impls wrap around it (missionary, EPL 2.0).
;;
;; ---------------------------------------------------------------------------
;; What the toggle means
;;
;; Every pull-loop operator in missionary -- Reduce, Reductions, Relieve,
;; Sample, Buffer, GroupBy -- is one loop guarded by one boolean:
;;
;;   while (ps.busy = !ps.busy) { ...one round... }
;;
;; Three parties call into it: the input's notifier, the input's terminator, and
;; the consumer's transfer. The toggle sorts out which of them runs the loop:
;;
;;   busy false  the loop is idle
;;   busy true   somebody is running it, and nothing is pending
;;   busy false  somebody is running it, and a round is pending  <-- again false
;;
;; The third state reuses the first's representation, which is sound in
;; javascript because the call stack already says whether you are inside the
;; loop. Reading and writing one bit is enough THERE. Everywhere else it is not:
;; `(set! busy (not busy))` is a read and a write, so two of them from different
;; executors interleave and one flip is lost. The loop then has nobody in it and
;; `busy` false, which reads as idle -- so the operator never pulls again while
;; its input sits notified. Nothing throws and nothing hangs in a stack; the
;; pipeline simply stops (ebb-8nq.31, measured on `m/reduce`).
;;
;; The JVM impls do not have the problem because every entry point is wrapped in
;; `synchronized (ps)`, which makes the read and the write one step -- and also
;; serialises everything else the round touches.
;;
;; ---------------------------------------------------------------------------
;; Why not the monitor
;;
;; Because a round DEREFS THE INPUT, and for an `ap` or a `cp` input that deref
;; is a `call!` onto the input's owner fiber -- the caller parks. Parking under a
;; counted lock is what ADR-001 rule 5 forbids, and this is the shape it forbids
;; it for: the fiber we would be waiting for is the one that blocks next on the
;; same lock inside its own notifier.
;;
;; So the arbitration is rule 1's other option, a CAS on one atom, and the three
;; states get three names:
;;
;;   nil    idle
;;   :run   a caller is running the loop
;;   :more  a caller is running the loop, and a round is due
;;
;; ---------------------------------------------------------------------------
;; How to port a loop
;;
;; `(set! busy (not busy))` appears in two positions, and they become different
;; calls here -- which is the whole content of the port:
;;
;;   ENTRY, from a notifier or a terminator     -> `claim!`
;;   CONTINUE, the loop's own next iteration    -> `release!`
;;
;; and Java's `break` out of the loop becomes "return without releasing": the
;; claim stays held, and whoever the round handed the callback to gives it back
;; with a `release!` when it is done. That is how `busy` stays true from the
;; moment the notifier is handed out until the consumer's `transfer` returns,
;; and it is load-bearing -- it is what keeps `transfer` from racing the next
;; round over the process's fields, so an operator whose loop always breaks
;; before notifying needs no further arbitration at all.
;;
;; An operator whose loop notifies and KEEPS PULLING (Buffer) or that has a
;; second entry point outside the loop (Sample's per-input `dirty`, GroupBy's
;; `group`/`cancel`) still has fields shared with a concurrent `transfer`. Those
;; get `locking` over the field updates alone, with every deref and every
;; callback outside it -- rule 5 again.
(ns ^:no-doc ebb.impl.arbiter)

(defn arbiter
  "A fresh claim cell, HELD BY THE CALLER.

  Every one of missionary's `run` functions begins `ps.busy = true` -- or, in
  Sample's case, holds the process monitor across the whole of construction --
  so that a notification raised while the input is still being built is
  recorded rather than run against a half-built process. Whoever constructs one
  of these therefore owes a `release!` once the process is assembled, which is
  where the recorded round is replayed."
  []
  (atom :run))

(defn claim!
  "Take the loop, or leave a round due for whoever holds it. True when taken.
  Never waits, so a notifier that lands here does not block."
  [a]
  (nil? (first (swap-vals! a #(if (nil? %) :run :more)))))

(defn release!
  "Hand the loop back. True when a round came due while we ran, in which case
  the claim STAYS with the caller and it must run another one."
  [a]
  (= :more (first (swap-vals! a #(if (= :more %) :run nil)))))
