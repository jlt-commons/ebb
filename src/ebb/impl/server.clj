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
;; call each other synchronously. Ebb cannot take that way out: the transfer
;; direction has to be synchronous (it returns the value) and the notify
;; direction has to be synchronous too (missionary's protocol, and lolcat, both
;; depend on a notification having been delivered before the notifier returns).
;;
;; So the server is REENTRANT: a call made from an owner fiber keeps serving
;; that fiber's own inbox while it waits for its reply. The owner is then never
;; blocked to its own callers, only to the work it is waiting on, and the cycle
;; above resolves -- inner's notify is served by outer's owner on outer's own
;; fiber, which is precisely where outer's continuations live.
;;
;; ---------------------------------------------------------------------------
;; A known protocol violation, and why it is still here (ebb-8nq.29)
;;
;; Every call! made from a callback ebb hands OUT blocks that callback's caller,
;; and both specs forbid it: a notifier and a terminator "must not block", and
;; so must a canceller and both task continuations. Missionary gets the ordering
;; those callbacks appear to have by running everything INLINE on the caller's
;; thread -- work, not waiting. Ebb cannot: resuming an ap or cp continuation is
;; bound to the owner fiber, so the choice is to block or to deliver later.
;;
;; Delivering later was measured, and it costs more than it is worth today: 9 of
;; missionary's own ap_test and cp_test cases fail, because lolcat is a
;; synchronous DSL -- `event` mutates one global context during a call, with no
;; waiting and no thread safety -- so "the notification arrives a moment later"
;; is not expressible in it. Even making only the canceller asynchronous breaks
;; three. Fixing this properly means adapting or replacing lolcat, which is
;; missionary's test DSL carried over verbatim.
;;
;; The mailbox below is UNBOUNDED regardless. That is not for a future cast!:
;; it is a bug fix. offer! on a full channel returns false exactly as it does on
;; a closed one, and the old code read both as "retired" and ran the thunk
;; INLINE on the caller -- resuming a continuation off its owner fiber, which
;; jolt.continuations documents as a hang with no error. A `live` flag now says
;; retired, and the queue cannot fill.
(ns ^:no-doc ebb.impl.server
  (:require [ebb.impl.prompt :as p]
            [ebb.impl.util :as u]
            [jolt.fibers :as fib]
            [clojure.core.async :as a]))

;; executor identity -> that owner's inbox, so a call can find its own
(def ^:private inboxes (atom {}))

(defn- serve-one!
  "Run one message. reply is nil for a cast, which wants no answer -- and whose
  throw must not be swallowed into a channel nobody reads."
  [[thunk reply]]
  (if (nil? reply)
    (try (thunk) (catch Throwable e (u/report-uncaught! "ap/cp owner" e)))
    (a/offer! reply (try [::ok (thunk)] (catch Throwable e [::err e]))))
  nil)

(defn- answer [[tag v]]
  (if (= ::ok tag) v (throw v)))

(deftype Owner [queue signal ^:unsynchronized-mutable id ^:unsynchronized-mutable live])

(defn- push!
  "Enqueue a message and wake the owner. Never blocks and never drops: the queue
  is unbounded and the channel carries only a nudge."
  [^Owner o msg]
  (swap! (.-queue o) conj msg)
  (a/offer! (.-signal o) true)
  nil)

(defn- drain!
  "Take every queued message, or nil when there are none."
  [^Owner o]
  (let [[old _] (swap-vals! (.-queue o) empty)]
    (seq old)))

(defn owner-id [^Owner o] (.-id o))

(defn spawn
  "Start an owner fiber. Returns once it is serving."
  []
  (let [o     (->Owner (atom []) (a/chan (a/sliding-buffer 1)) nil true)
        ready (promise)]
    (fib/spawn
      (fn []
        (let [me (p/here)]
          (set! (.-id o) me)
          (swap! inboxes assoc me o)
          (deliver ready true)
          (loop []
            (run! serve-one! (drain! o))
            (when (.-live o)
              (when (some? (a/<!! (.-signal o)))
                (recur))))
          ;; retired: serve whatever arrived before the close
          (run! serve-one! (drain! o))
          (swap! inboxes dissoc me))))
    (deref ready)
    o))

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
          (push! o [thunk reply])
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
