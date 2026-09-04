(ns ebb.model.handshake
  "What does a doubly-completed task do to ebb.impl.affine's handshake?

  ADR-001 discipline 2: a resumption drives the body to its next park before
  returning. `resume!` installs a fresh gate promise, delivers the result, then
  waits on that gate; the body opens whatever gate is CURRENT at its next park,
  via `quiescent!`.

  Two facts make that fragile, and both are in the shipped code:

    * `gate` is a single cell, so a second install overwrites the first.
    * `(deliver p v)` on an already-realized promise is a NO-OP, so a second
      resumer wakes the body zero extra times.

  Two resumers therefore install two gates but generate one wake-up. This model
  asks how many schedules survive that -- which matters because ebb.model.sleep
  shows a cancelled sleep can produce exactly this."
  (:refer-clojure :exclude [==])
  (:require [clojure.core.logic :refer :all]
            [ebb.model.interleave :as il]))

;; state: [gate opened waiting delivered credit]
;;   gate      the promise currently installed: nil / :g1 / :g2
;;   opened    gates the body has actually delivered
;;   waiting   resumers parked on their gate
;;   delivered has the result promise been realized yet
;;   credit    is the body runnable (one per REAL delivery, not per attempt)

(defne stepo [action s0 s1]
  ([:r1/install [_ o w d c] [:g1 o w d c]])
  ([:r2/install [_ o w d c] [:g2 o w d c]])
  ([:r1/await   [g o w d c] [g o [:g1 . w] d c]])
  ([:r2/await   [g o w d c] [g o [:g2 . w] d c]])

  ;; deliver: only the FIRST one realizes the promise and wakes the body.
  ([:r1/deliver [g o w false _] [g o w true true]])
  ([:r1/deliver [g o w true  c] [g o w true c]])
  ([:r2/deliver [g o w false _] [g o w true true]])
  ([:r2/deliver [g o w true  c] [g o w true c]])

  ;; the body parks only when it is runnable, and opens the current gate
  ([:b/park [nil o w d true] [nil o w d false]])
  ([:b/park [:g1 o w d true] [:g1 [:g1 . o] w d false]])
  ([:b/park [:g2 o w d true] [:g2 [:g2 . o] w d false]])
  ([:b/park [g o w d false]  [g o w d false]]))

(def r1 [:r1/install :r1/deliver :r1/await])
(def r2 [:r2/install :r2/deliver :r2/await])
(def one-resumer  [r1])
(def two-resumers [r1 r2])

(defn- drain
  "Let the body park for as long as it is runnable. Modelling the body this way
  rather than as another interleaved thread is deliberate: it grants the body
  EVERY chance to open gates, so a resumer still left waiting is stuck because
  of the protocol, not because the schedule starved the body of parks."
  [[g o w d c]]
  (if c
    (recur [g (if g (cons g o) o) w d false])
    [g o w d c]))

(defn stuck [progs]
  (for [sched (il/schedules progs)
        :let [end (first (run* [q]
                           (fresh [g o w d c]
                             (il/runo stepo sched [nil () () false false] [g o w d c])
                             (== q [g o w d c]))))
              [_ opened waiting] (drain end)
              orphans (remove (set opened) waiting)]
        :when (seq orphans)]
    [(vec sched) (vec orphans)]))

(defn -main [& _]
  (println "\nebb.impl.affine -- can a resumer wait on a gate nobody opens?\n")
  (doseq [[label progs] [["one resumer"  one-resumer]
                         ["two resumers" two-resumers]]]
    (let [all (count (il/schedules progs))
          bad (stuck progs)]
      (println (format "%-14s %4d schedules, %4d leave a resumer stuck  (%s)"
                       label all (count bad)
                       (if (zero? (count bad)) "sound" "DEADLOCK")))
      (doseq [[sched orphans] (take 2 bad)]
        (println "     " (pr-str sched) "-> stuck on" (pr-str orphans)))))
  (println (str "\nOne resumer is sound in every schedule. Two are stuck in every schedule:\n"
                "two gates are installed but only the first delivery wakes the body, so\n"
                "there is exactly one open for two waiters -- no interleaving escapes it.\n"
                "A stuck resumer is a blocked carrier. With 100 processes winding down at\n"
                "once, that is how every carrier goes to 100% and stays there."))
  (System/exit 0))
