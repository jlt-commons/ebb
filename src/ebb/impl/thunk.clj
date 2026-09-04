;; Derived from missionary/impl/Thunk.java (missionary, EPL 2.0).
;;
;; Runs a thunk on an Executor and completes a task with its result;
;; cancellation interrupts the running thread. Ported rather than dropped
;; because jolt implements the JVM interrupt protocol in full -- .interrupt,
;; .isInterrupted, Thread/interrupted, and InterruptedException out of a
;; blocking wait -- which is the whole mechanism this file needs.
;;
;; Per ADR-001 rule 1 the Java synchronises on the process, so this uses
;; `locking`, and the continuation is invoked only after leaving it.
;;
;; The `thread` field is a three-state cell, exactly as in the Java:
;;   nil    not started
;;   Thread the thread currently evaluating the thunk
;;   ps     "cancelled" -- self-reference as a sentinel
(ns ^:no-doc ebb.impl.thunk
  (:import [java.util.concurrent Executors ThreadFactory]))

(defn- daemon-factory
  "Named DAEMON threads, as missionary's Thunk does. Not cosmetic: a pool of
  non-daemon threads keeps the process alive after main returns, so a script
  that ever touched `via` would hang instead of exiting."
  [prefix]
  (let [id (atom -1)]
    (reify ThreadFactory
      (newThread [_ r]
        (doto (Thread. r (str "ebb " prefix "-" (swap! id inc)))
          (.setDaemon true))))))

(def cpu
  "Bounded pool for compute. One thread per available processor."
  (Executors/newFixedThreadPool (.availableProcessors (Runtime/getRuntime))
                                (daemon-factory "cpu")))

(def blk
  "Unbounded pool for blocking calls -- a thread parked in a syscall is not
  costing a core, so this grows as needed rather than queueing."
  (Executors/newCachedThreadPool (daemon-factory "blk")))

(deftype Process [thunk success failure ^:unsynchronized-mutable thread]
  clojure.lang.IFn
  ;; (ps) -- cancel: interrupt the evaluating thread, or pre-mark as cancelled
  ;; so the evaluation interrupts itself as soon as it starts.
  (invoke [this]
    (let [t (locking this
              (let [t (.-thread this)]
                (when-not (identical? t this) (set! (.-thread this) this))
                t))]
      (when (and (some? t) (not (identical? t this))) (.interrupt ^Thread t)))
    nil))

(defn- evaluate [^Process ps]
  ;; Claim the thread, or -- if cancel got here first -- interrupt ourselves so
  ;; the thunk sees the flag from its very first blocking call.
  (locking ps
    (let [t (Thread/currentThread)]
      (if (identical? (.-thread ps) ps)
        (.interrupt t)
        (set! (.-thread ps) t))))
  (let [[x cont] (try [((.-thunk ps)) (.-success ps)]
                      (catch Throwable e [e (.-failure ps)]))]
    ;; Clear the flag if we were cancelled, so the pool thread goes back clean;
    ;; otherwise mark ourselves done so a later cancel interrupts nobody.
    (locking ps
      (if (identical? (.-thread ps) ps)
        (Thread/interrupted)
        (set! (.-thread ps) ps)))
    (cont x)))

(defn run [executor thunk s f]
  (let [ps (->Process thunk s f nil)]
    ;; jolt's Executor shim takes an IFn, not a Runnable
    (.execute ^java.util.concurrent.Executor executor (fn [] (evaluate ps)))
    ps))
