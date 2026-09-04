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
;; So the server is REENTRANT instead: a call made from an owner fiber keeps
;; serving that fiber's own inbox while it waits for its reply. The owner is
;; then never blocked to its own callers, only to the work it is waiting on, and
;; the cycle above resolves -- inner's notify is served by outer's owner on
;; outer's own fiber, which is precisely where outer's continuations live.
(ns ^:no-doc ebb.impl.server
  (:require [ebb.impl.prompt :as p]
            [jolt.fibers :as fib]
            [clojure.core.async :as a]))

;; executor identity -> that owner's inbox, so a call can find its own
(def ^:private inboxes (atom {}))

(defn- serve-one! [[thunk reply]]
  (a/offer! reply (try [::ok (thunk)] (catch Throwable e [::err e])))
  nil)

(defn- answer [[tag v]]
  (if (= ::ok tag) v (throw v)))

(deftype Owner [inbox ^:unsynchronized-mutable id])

(defn owner-id [^Owner o] (.-id o))

(defn spawn
  "Start an owner fiber. Returns once it is serving."
  []
  (let [inbox (a/chan 1024)
        o     (->Owner inbox nil)
        ready (promise)]
    (fib/spawn
      (fn []
        (let [me (p/here)]
          (set! (.-id o) me)
          (swap! inboxes assoc me inbox)
          (deliver ready true)
          (loop []
            (when-some [msg (a/<!! inbox)]
              (serve-one! msg)
              (recur)))
          (swap! inboxes dissoc me))))
    (deref ready)
    o))

(defn retire!
  "Close the inbox so the owner fiber exits. Later calls run inline."
  [^Owner o]
  (a/close! (.-inbox o))
  nil)

(defn call!
  "Run thunk on o's fiber and answer its value, or throw its error."
  [^Owner o thunk]
  (let [me (p/here)]
    (if (= (.-id o) me)
      (thunk)                                   ; already home
      (let [reply (a/promise-chan)]
        (if-not (a/offer! (.-inbox o) [thunk reply])
          (thunk)                               ; retired: nothing left to serialise
          (if-some [mine (get @inboxes me)]
            ;; we are an owner ourselves -- keep serving our own callers while
            ;; we wait, or a mutual call deadlocks (see the header)
            (loop []
              (let [[v ch] (a/alts!! [reply mine])]
                (cond
                  (identical? ch reply) (answer v)
                  ;; our own inbox closed (we were retired mid-call): stop
                  ;; serving it, or alts!! spins handing back nil forever
                  (nil? v) (answer (a/<!! reply))
                  :else (do (serve-one! v) (recur)))))
            (answer (a/<!! reply))))))))
