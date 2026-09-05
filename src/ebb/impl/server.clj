;; ebb.impl.server -- the owner fiber that `ap` and `cp` processes run on.
;;
;; Why they need one is ADR-001: a process holds captured continuations, and a
;; continuation may only be resumed on the executor that captured it, so every
;; entry point (upstream notify, task completion, downstream transfer, cancel)
;; has to be marshalled onto one fiber. That is Erlang's gen_server:call: the
;; caller needs the reply, or at least needs the work done, before it continues.
;;
;; ---------------------------------------------------------------------------
;; Reentrant serving, and why a plain gen_server:call is not enough
;;
;; Flows compose, so two processes routinely call each other. `(ap (? (sleep d
;; (?> inner))))` over an inner `ap` is one line of ordinary missionary and it
;; is exactly the shape that breaks:
;;
;;   outer's owner transfers from inner  -> waits for inner's owner
;;   inner's owner, serving that, notifies downstream
;;                                       -> waits for outer's owner
;;   both wait forever
;;
;; Erlang has the same hazard and normally answers it by not letting servers
;; call each other synchronously. Ebb cannot take that way out for the TRANSFER
;; direction: a transfer returns the value, so the caller has to wait for it.
;;
;; So the server is REENTRANT: a call made from an owner fiber keeps serving
;; that fiber's own inbox while it waits for its reply. The owner is then never
;; blocked to its own callers, only to the work it is waiting on, and the cycle
;; above resolves -- inner's notify is served by outer's owner on outer's own
;; fiber, which is precisely where outer's continuations live.
;;
;; ---------------------------------------------------------------------------
;; call! and cast!, and which callback gets which (ebb-8nq.29)
;;
;; The other direction does NOT have to be synchronous, and must not be. A
;; notifier and a terminator "must not block", and neither may a canceller or
;; either task continuation -- so every callback ebb hands OUT is a `cast!`,
;; enqueue-and-return, and only the transfer and the initial start are `call!`.
;;
;; This was blocked for a while on the test suite rather than on the runtime.
;; Missionary gets the ordering those callbacks appear to have by running
;; everything INLINE on the caller's thread, and lolcat -- missionary's own DSL,
;; which 30 of its 33 test namespaces are written in -- bakes that in: a `call`
;; required every event it declared to have been raised BEFORE the call
;; returned, so "the notification arrives a moment later" could not be written
;; down. Casting all four callbacks failed 9 of missionary's ap and cp cases for
;; that reason alone. lolcat now waits for the events a call declared instead of
;; failing on the spot (see test/lolcat/core.clj), which is a relaxation of WHEN,
;; not of what or in what order, and 8 of those 9 came back.
;;
;; The ninth was real, and `cast!` still handles it synchronously: a caller with
;; ambient context in scope -- the reactor, ebb's stand-in for missionary's
;; ThreadLocal -- needs the callee's writes back, and a cast has nothing to
;; bring them back in. See `cast!`.
;;
;; ---------------------------------------------------------------------------
;; start!, and why starting is not just spawn-then-call
;;
;; A process's owner fiber has to run its start-up -- register the prompt, run
;; the body to its first suspension, pump once -- and the obvious way to arrange
;; that is `spawn` followed by `call!`. That is two handshakes, and the second
;; is the expensive kind: the fiber has only just been created and is parked on
;; its channel, so waking it costs a COLD scheduling round trip. Measured on
;; `(m/reduce conj [] (m/ap (m/?> (m/seed [1]))))`, 41us for the spawn and 47us
;; for the call!, out of 336us end to end.
;;
;; `start!` does both in one: it spawns the fiber and hands it the start-up as
;; its first act, delivering `ready` only once that has run. 333us -> 218us on
;; the minimum of 45 measurements (ebb-8nq.40). Cold is also the number to
;; reason with elsewhere: a hot owner answers a call! in ~27us and a cold one in
;; 45-55us, and pricing hops at the hot figure is what led ebb-8nq.36 to
;; conclude marshalling was negligible when it is about half.
;;
;; The mailbox below is UNBOUNDED. That was a bug fix before it was a
;; prerequisite for cast!: offer! on a full channel returns false exactly as it
;; does on a closed one, and the old code read both as "retired" and ran the
;; thunk INLINE on the caller -- resuming a continuation off its owner fiber,
;; which jolt.continuations documents as a hang with no error. A `live` flag now
;; says retired, and the queue cannot fill.
(ns ^:no-doc ebb.impl.server
  (:require [ebb.impl.prompt :as p]
            [ebb.impl.util :as u]
            [jolt.fibers :as fib]
            [clojure.core.async :as a]))

;; executor identity -> that owner's inbox, so a call can find its own
(def ^:private inboxes (atom {}))

(defn- serve-one!
  "Run one message. reply is nil for a cast, which wants no answer -- and whose
  throw must not be swallowed into a channel nobody reads.

  The message carries its sender's ambient context (ebb.impl.util's carried
  locals -- today, the reactor's `current` and `delayed`), which is installed
  for exactly this thunk and put back afterwards. That is what makes this hop
  equivalent to the JVM's inline call: on the JVM the notifier runs on the
  propagating thread and reads its ThreadLocal, here it runs on the owner fiber
  and reads what the propagation sent with it. Serving is REENTRANT, so the
  restore has to be the previous values rather than nil -- a message served
  while another is waiting for a reply must give that one its context back.

  It travels BOTH ways. A ThreadLocal is one cell shared by the caller and
  everything it calls, so a write made by the callee is a write the caller sees
  when it returns -- and the reactor depends on exactly that: `schedule`, run
  from a notifier deep in a propagation, pushes onto `delayed`, and the
  OUTERMOST `event` is what drains it afterwards. Carrying the context down but
  not back leaves that push on the callee's fiber, where nothing drains it, and
  the reactor simply stops. So the reply carries the callee's final snapshot and
  `answer` installs it.

  No `finally`: both arms catch Throwable already, so nothing escapes, and a
  winder here would sit inside every prompt extent started under it."
  [[thunk reply ctx]]
  (let [was (u/install-context! ctx)]
    (if (nil? reply)
      (try (thunk) (catch Throwable e (u/report-uncaught! "ap/cp owner" e)))
      (let [r (try [::ok (thunk)] (catch Throwable e [::err e]))]
        (a/offer! reply (conj r (u/capture-context)))))
    (u/install-context! was))
  nil)

(defn- answer
  "Unwrap a reply, adopting the context the callee left behind -- unconditional,
  because a callee that CLEARED the context (a propagation ending, say) has to
  clear it here too."
  [[tag v ctx]]
  (u/install-context! ctx)
  (if (= ::ok tag) v (throw v)))

(deftype Owner [queue signal
                ^:unsynchronized-mutable id
                ;; live  -- retire! has not been called
                ;; drained -- the fiber has stopped taking work, so anything
                ;;            enqueued from here on is the sender's to run
                ^:unsynchronized-mutable live
                ^:unsynchronized-mutable drained])

(defn- drain!
  "Take every queued message, or nil when there are none."
  [^Owner o]
  (let [[old _] (swap-vals! (.-queue o) empty)]
    (seq old)))

(defn- push!
  "Enqueue a message and wake the owner. Never blocks and never drops: the queue
  is unbounded and the channel carries only a nudge.

  The `drained` check is what keeps a CAST from being lost. A sender reads
  `live`, the owner retires and takes its last drain, and only then does the
  sender enqueue -- for a `call!` that shows up as a reply that never comes, but
  a cast has nobody waiting, so the message just disappears. That is a cancel
  that never happens and a process that never terminates: 9 runs in 300 of
  `(m/reduce f nil (m/signal (m/ap (m/? m/never))))` hung on exactly this.

  The owner sets `drained` BEFORE its final drain, so the two orderings are both
  safe: reading false means the final drain is still ahead of us and will take
  our message, and reading true means we run it ourselves. `drain!` is one
  atomic swap, so only one side ever gets a given message."
  [^Owner o msg]
  (swap! (.-queue o) conj msg)
  (a/offer! (.-signal o) true)
  (when (.-drained o) (run! serve-one! (drain! o)))
  nil)

(defn owner-id [^Owner o] (.-id o))

(defn owner
  "Allocate an owner. Its fiber does not exist until `start!`."
  []
  (->Owner (atom []) (a/chan (a/sliding-buffer 1)) nil true false))

(defn start!
  "Spawn o's fiber, run (f) ON IT, and return once that has happened. f may be
  nil, in which case this is just \"start serving\".

  Fusing the two is worth doing rather than tidy. Starting an ap used to cost a
  spawn handshake AND a `call!`, and the call! is the expensive kind: the owner
  fiber has just been created and is parked on its channel, so waking it costs
  a cold scheduling round trip rather than the hot one a tight benchmark loop
  measures. Instrumented on `(m/reduce conj [] (m/ap (m/?> (m/seed [1]))))`,
  that was 41us for the spawn and another 47us for the first call!, out of 336us
  end to end. One handshake now (ebb-8nq.40).

  The caller's ambient context travels with it, and back, exactly as `call!`
  does -- an ap started from inside a reactor propagation must see it, and
  anything the body pushes onto `delayed` must reach the caller. ADR-001 rule 6."
  [^Owner o f]
  (let [ready (promise)
        ctx   (u/capture-context)]
    (fib/spawn
      (fn []
        (let [me (p/here)]
          (set! (.-id o) me)
          (swap! inboxes assoc me o)
          (if f
            (let [was (u/install-context! ctx)]
              ;; f is the process's own start-up; a throw out of it has nowhere
              ;; to go but the report, as a cast's would
              (try (f) (catch Throwable e (u/report-uncaught! "ap/cp owner start" e)))
              (deliver ready [(u/capture-context)])
              (u/install-context! was))
            (deliver ready nil))
          (loop []
            (run! serve-one! (drain! o))
            (when (.-live o)
              (when (some? (a/<!! (.-signal o)))
                (recur))))
          ;; retired. Flag first, drain second: a sender that reads the flag
          ;; after we set it runs its own message, and one that reads it before
          ;; has already enqueued for the drain below.
          (set! (.-drained o) true)
          (run! serve-one! (drain! o))
          (swap! inboxes dissoc me))))
    ;; only a start that RAN something has a context to give back. A plain
    ;; spawn has done no work on the caller's behalf, and installing its empty
    ;; snapshot would clear a propagation the caller is still inside --
    ;; reactor-double-sub catches exactly that.
    (when-some [reply (deref ready)]
      (u/install-context! (nth reply 0)))
    o))

(defn spawn
  "Start an owner fiber. Returns once it is serving."
  []
  (start! (owner) nil))

(defn retire!
  "Stop the owner fiber. Later work runs inline on its caller."
  [^Owner o]
  (set! (.-live o) false)
  (a/close! (.-signal o))
  nil)


(defn call!
  "Run thunk on o's fiber and answer its value, or throw its error."
  [^Owner o thunk]
  (let [me (p/here)]
    (if (= (.-id o) me)
      (thunk)                                   ; already home
      (if-not (.-live o)
        (thunk)                                 ; retired: nothing left to serialise
        (let [reply (a/promise-chan)]
          (push! o [thunk reply (u/capture-context)])
          (if-some [^Owner mine (get @inboxes me)]
            ;; we are an owner ourselves -- keep serving our own callers while
            ;; we wait, or a mutual call deadlocks (see the header)
            (loop []
              (run! serve-one! (drain! mine))
              (let [[v ch] (a/alts!! [reply (.-signal mine)])]
                (cond
                  (identical? ch reply) (answer v)
                  ;; our own signal closed (we were retired mid-call): stop
                  ;; serving, or alts!! spins handing back nil forever
                  (nil? v) (do (run! serve-one! (drain! mine)) (answer (a/<!! reply)))
                  :else (recur))))
            (answer (a/<!! reply))))))))


(defn cast!
  "Hand thunk to o's fiber and RETURN AT ONCE, without waiting for it to run.

  This is Erlang's `!` to `call!`'s `gen_server:call`, and it is what the flow
  and task protocols actually ask for of the callbacks a process hands out: a
  notifier, a terminator, a canceller and both task continuations `must not
  block the calling thread`. `call!` blocks all of them (ebb-8nq.29).

  Three cases still run synchronously, and each is exact rather than cautious:

  * already on o's fiber -- the hop is a no-op either way;
  * o retired -- there is no fiber left to serialise against;
  * the caller has AMBIENT CONTEXT in scope. That is the reactor, and it is the
    one thing a cast cannot express. A carried local stands in for missionary's
    ThreadLocal, which is one cell shared by a caller and everything it calls,
    so a write the callee makes is a write the caller must see on return --
    `schedule` pushes onto `delayed` from inside a notifier and the OUTERMOST
    `event` drains it. A cast has no reply to put that snapshot in, and worse,
    it runs the thunk against a snapshot of a propagation that may already have
    been torn down. So inside a reactor the hop goes back to being synchronous.
    Nothing else in ebb registers a carried local, so nothing else pays."
  [^Owner o thunk]
  (if (or (= (.-id o) (p/here)) (not (.-live o)))
    (thunk)
    (if-some [ctx (u/capture-context)]
      (call! o thunk)
      (push! o [thunk nil nil])))
  nil)
