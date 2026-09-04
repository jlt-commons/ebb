(ns ^:no-doc ebb.impl.util
  "Shared primitives: the Cancelled signal and the host timer seam."
  (:require [jolt.scheme :as scheme]
            [clojure.core.async :as a]))

(defn nop [])

(defn cancelled
  "ebb's cancellation signal. missionary uses a dedicated `missionary.Cancelled`
  class; jolt has no user classes, so it is an ex-info carrying ::cancelled."
  [msg]
  (ex-info msg {:type ::cancelled}))

(defn cancelled? [e]
  (= ::cancelled (:type (ex-data e))))

(def ^:private timer-at!
  ;; host/chez/java/async.ss — the runtime's deadline heap, the same one
  ;; core.async/timeout rides on. No cancel: a fired-but-disarmed entry is a nop.
  (scheme/proc "jolt-timer-at!"))

(defn schedule!
  "Run (f) approximately ms from now, ON ITS OWN FIBER. Returns nil; disarm with
  your own flag.

  The fiber is not an optimisation. There is ONE timer thread for the whole
  runtime and it runs each due thunk itself, so a thunk that blocks stalls every
  other timer in the process. Ebb's thunks routinely block: completing a sleep
  resumes a process, and that resumption waits for the process to reach its next
  park (ADR-001 discipline 2). A hundred concurrent sleeps then serialise
  through one thread and everything downstream times out -- which is exactly how
  this was found, as 17 tests that pass alone timing out when run together."
  [ms f]
  ;; fiber-execute, not fibers/spawn: this is fire-and-forget, and spawn also
  ;; builds a handle and completion machinery nobody reads. It is on the hot
  ;; path -- every sleep in the system lands here.
  (timer-at! (+ (System/currentTimeMillis) ms)
             (fn [] (a/fiber-execute f) nil))
  nil)
