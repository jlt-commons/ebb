;; Derived from missionary (EPL 2.0):
;;   logic           <- missionary/impl/Sleep.cljs
;;   synchronisation <- missionary/impl/Sleep.java  (the scheduler lock)
;;
;; Sleep is the one task in ebb completed by two DIFFERENT threads: the runtime
;; timer fires it, and whoever cancels the process cancels it. The .cljs impl
;; arbitrates with a plain mutable flag, which is enough on one thread. It is
;; not enough here, and ADR-001 rule 1 says to take the synchronisation from the
;; Java: there, the timer nulls success/failure while holding the scheduler lock
;; and Process.invoke reads-and-nulls failure under the same lock, so exactly
;; one side wins.
;;
;; A one-way atomic true->false is the same arbitration without a lock, and it
;; is all this needs. `model/ebb/model/sleep.clj` enumerates both versions: the
;; flag double-completes in 12 of 20 interleavings, the CAS in 0 of 6. Double
;; completion is not a cosmetic duplicate -- it resumes the owning process
;; twice, and model/ebb/model/handshake.clj shows two concurrent resumers
;; deadlock one of themselves in EVERY schedule.
(ns ^:no-doc ebb.impl.sleep
  (:require [ebb.impl.util :as u]))

(declare ->Process)

(defn- claim!
  "Atomically take the right to complete this sleep. True for exactly one
  caller. `locking` rather than an atom because every sleep in the system
  allocates one of these and the hot tests do a hundred thousand of them; the
  callback is invoked OUTSIDE the critical section, as the Java does, since it
  resumes a process and must never run under a lock (ADR-001 rule 5)."
  [^Process slp]
  (locking slp
    (let [v-pending (.-pending slp)]
      (set! (.-pending slp) false)
      v-pending)))

(deftype Process [failure ^:unsynchronized-mutable pending]
  clojure.lang.IFn
  (invoke [this]
    (when (claim! this) (failure (u/cancelled "Sleep cancelled.")))
    nil))

(defn run [d x s f]
  (let [slp (->Process f true)]
    (u/schedule! d (fn [] (when (claim! slp) (s x))))
    slp))
