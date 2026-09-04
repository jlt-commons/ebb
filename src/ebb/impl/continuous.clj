;; Derived from missionary/impl/Continuous.cljs (missionary, EPL 2.0).
;;
;; `cp` -- a CONTINUOUS flow: it always has a current value, and it is sampled
;; lazily by downstream rather than pushed. `?<` reads the current value of an
;; upstream continuous flow and, when that value changes, re-runs the rest of
;; the body from that point. So a cp body is an incremental computation, and
;; each `?<` is a memo point in it.
;;
;; That maps onto the prompt directly: `?<` suspends, the driver samples the
;; flow and resumes; when the flow later changes, the driver resumes THE SAME
;; continuation again with the new value. Choices created after the one being
;; recomputed belonged to the invalidated continuation, so they are pruned --
;; missionary keeps them in rank order in a pairing heap for exactly this, and
;; ebb keeps them in creation order and takes the lowest dirty rank.
;;
;; ---------------------------------------------------------------------------
;; ARBITRATION (ADR-001 rule 1). Continuous.java wraps its entry points in
;; `synchronized (ps)` and Continuous.cljs stands in for that with a `busy`
;; toggle; this file carries NEITHER, and needs neither, because it is not a
;; port of that structure at all. A cp owns a fiber (discipline 2), and every
;; mutable field below -- `live`, `value`, `choices`, `dirty`, `drain`,
;; `notified`, `sampling`, `ended`, and every field of every Choice -- is
;; touched only from a thunk passed to `call!`, which is a synchronous
;; request/reply onto that one fiber. The fiber IS the monitor, and it is the
;; monitor a locked region could not be: the work it serialises parks, which is
;; exactly what rule 5 forbids under a counted lock.
;;
;; So the audit item for this file (ebb-8nq.32) is discharged by an invariant
;; rather than by a lock: NOTHING here may read or write a Process or Choice
;; field except inside `call!`. The two atoms in the file are not arbitration --
;; `prompts` is a registry keyed by executor, and neither is per-process state.
(ns ^:no-doc ebb.impl.continuous
  (:require [ebb.impl.prompt :as p]
            [ebb.impl.util :refer [nop cancelled]]
            [ebb.impl.server :as server]))

(declare transfer cancel call! cast! settle! step! register! deregister! current)

(def ^:private none ::none)

(deftype Process [notifier terminator prompt body owner
                  ^:unsynchronized-mutable live
                  ^:unsynchronized-mutable value
                  ^:unsynchronized-mutable choices   ; vector, creation order = rank
                  ^:unsynchronized-mutable dirty     ; set of ranks needing recompute
                  ^:unsynchronized-mutable drain     ; pruned choices with an untransferred value
                  ^:unsynchronized-mutable notified
                  ;; true while a transfer is computing. An upstream that
                  ;; notifies mid-sample is supplying the value we are in the
                  ;; middle of reading, not invalidating it, so downstream must
                  ;; not be woken for it.
                  ^:unsynchronized-mutable sampling
                  ^:unsynchronized-mutable ended]
  clojure.lang.IFn
  (invoke [this] (cast! this #(cancel this)) nil)
  clojure.lang.IDeref
  (deref [this] (call! this #(transfer this))))

(deftype Choice [k rank
                 ^:unsynchronized-mutable iterator
                 ^:unsynchronized-mutable ready   ; upstream notified, not yet sampled
                 ^:unsynchronized-mutable done
                 ^:unsynchronized-mutable live
                 ;; sampled at least once; only then does a notification mean
                 ;; CHANGE rather than first value
                 ^:unsynchronized-mutable established
                 ^:unsynchronized-mutable value])

;; ------------------------------------------------------------------- plumbing

(defn- call! [^Process ps thunk] (server/call! (.-owner ps) thunk))
(defn- cast! [^Process ps thunk] (server/cast! (.-owner ps) thunk))


(defn- prune!
  "Discard every choice at or after rank -- they belong to the continuation this
  recompute is about to replace."
  [^Process ps rank]
  (let [chs (.-choices ps)
        gone (subvec chs rank)]
    (set! (.-choices ps) (subvec chs 0 rank))
    (set! (.-dirty ps) (into #{} (remove #(>= % rank)) (.-dirty ps)))
    (doseq [^Choice ch gone]
      (when (.-live ch)
        (set! (.-live ch) false)
        (when-some [it (.-iterator ch)] (it)))
      ;; A pruned choice whose upstream had already notified still owes the flow
      ;; protocol a transfer. missionary leaves it in the dirty heap and drains
      ;; it when the loop gets there, i.e. after the live work -- so it is
      ;; collected here and drained once the value has settled, not now.
      (when (.-ready ch)
        (set! (.-drain ps) (conj (.-drain ps) ch))))
    nil))

(defn- sample!
  "The current value of ch's upstream flow.

  A continuous flow must have a value to give. One that has not notified has
  none, so it is not a flow we can switch on: cancel it -- which per the flow
  protocol makes it notify and then fail or terminate, drained by the callback
  below -- and report it. Missionary does the same: detach, cancel, throw."
  [^Choice ch]
  (if (.-ready ch)
    (do (set! (.-ready ch) false) @(.-iterator ch))
    (do (set! (.-live ch) false)
        (when-some [it (.-iterator ch)] (it))
        (throw (ex-info "Undefined continuous flow." {:type ::undefined})))))

;; ------------------------------------------------------------------ the driver

(defn- advance!
  "Run the body forward from one prompt result until it settles on a value."
  [^Process ps r]
  (loop [r r]
    (cond
      (p/value? r) (set! (.-value ps) [::ok (p/payload r)])
      (p/error? r) (set! (.-value ps) [::err (p/payload r)])
      :else
      (let [[tag flow] (p/payload r)
            k          (p/continuation r)
            rank       (count (.-choices ps))
            ch         (->Choice k rank nil false false true false none)]
        (set! (.-choices ps) (conj (.-choices ps) ch))
        (set! (.-iterator ch)
              (flow (fn [] (cast! ps (fn []
                                       (if (.-live ch)
                                         (do (set! (.-ready ch) true)
                                             (when (.-established ch)
                                               (set! (.-dirty ps) (conj (.-dirty ps) (.-rank ch)))))
                                         ;; pruned or cancelled: drain and discard,
                                         ;; the flow protocol requires the transfer
                                         (try @(.-iterator ch) (catch Throwable _ nil)))
                                       (settle! ps))) nil)
                    (fn [] (cast! ps (fn []
                                       (set! (.-done ch) true)
                                       (settle! ps))) nil)))
        (when-not (.-live ps)
          (set! (.-live ch) false)
          ((.-iterator ch)))
        (recur (let [x (try [::ok (sample! ch)] (catch Throwable e [::err e]))]
                 (set! (.-established ch) true)
                 (set! (.-value ch) (second x))
                 (step! ps #(p/resume (.-prompt ps) k x))))))))

(defn- recompute! [^Process ps]
  (if (= none (.-value ps))
    (advance! ps (step! ps #(p/start (.-prompt ps) (.-body ps))))
    (loop []
      (when-some [rank (first (sort (.-dirty ps)))]
        (let [^Choice ch (nth (.-choices ps) rank)]
          (set! (.-dirty ps) (disj (.-dirty ps) rank))
          (let [x (try [::ok (sample! ch)] (catch Throwable e [::err e]))]
            ;; An upstream that re-emits the value it already had has not
            ;; changed anything, so the continuation stays valid and is not
            ;; re-run. missionary makes the same comparison in `pull`.
            (if (and (= ::ok (first x)) (= (second x) (.-value ch)))
              (recur)
              (do (set! (.-value ch) (second x))
                  (prune! ps (inc rank))
                  (advance! ps (step! ps #(p/resume (.-prompt ps) (.-k ch) x)))))))))))

;; --------------------------------------------------------------------- events

(defn- exhausted? [^Process ps]
  (or (and (not (.-live ps)) (empty? (.-drain ps)))
      (and (empty? (.-dirty ps))
           (every? #(.-done ^Choice %) (.-choices ps)))))

(defn- end! [^Process ps]
  (set! (.-ended ps) true)
  (call! ps deregister!)
  (server/retire! (.-owner ps))
  ((.-terminator ps))
  nil)

(defn- settle! [^Process ps]
  (cond
    (.-sampling ps) nil
    (.-ended ps) nil
    ;; A CANCELLED process with nothing left to give is over even if a
    ;; notification is still outstanding -- nobody is coming to transfer it, and
    ;; waiting for one strands the owner fiber for the life of the program.
    (and (not (.-live ps)) (exhausted? ps)) (end! ps)
    (.-notified ps) nil
    (seq (.-dirty ps)) (do (set! (.-notified ps) true) ((.-notifier ps)))
    (exhausted? ps) (end! ps)
    :else nil))

(defn- drain! [^Process ps]
  (let [chs (.-drain ps)]
    (set! (.-drain ps) [])
    (doseq [^Choice ch chs]
      (set! (.-ready ch) false)
      (try @(.-iterator ch) (catch Throwable _ nil)))
    (seq chs)))

(defn- transfer [^Process ps]
  (set! (.-notified ps) false)
  (set! (.-sampling ps) true)
  (try
    ;; Loop, don't compute once: the body itself can make an upstream notify
    ;; (input-change-from-continuation), so a value is only final when nothing
    ;; is dirty and nothing is left to drain.
    (loop []
      (try (recompute! ps) (catch Throwable e (set! (.-value ps) [::err e])))
      (let [drained (drain! ps)]
        (when (or drained (seq (.-dirty ps))) (recur))))
    (finally (set! (.-sampling ps) false)))
  (let [[tag v] (.-value ps)]
    (settle! ps)
    (if (= ::ok tag) v (throw v))))

(defn- cancel [^Process ps]
  (when (.-live ps)
    (set! (.-live ps) false)
    (doseq [^Choice ch (.-choices ps)]
      (when (.-live ch)
        (set! (.-live ch) false)
        (when-some [it (.-iterator ch)] (it))))
    ;; Settle after cancelling, or the owner fiber never learns the process is
    ;; over and its inbox is never closed -- one leaked fiber per cancelled cp,
    ;; which starves the carrier pool a few hundred tests later.
    (settle! ps))
  nil)

;; ------------------------------------------------------------------- the body

;; WHICH PROMPT IS IN SCOPE, keyed by executor.
;;
;; This was a single mutable cell, set around each step and restored after --
;; missionary's `fiber` global. That is unsound here. An extent returns through
;; whichever delimiter the prompt cell holds at the time, so when a resume comes
;; from a different frame than the one that entered, the entering frame is
;; ABANDONED and its restore never runs. The cell then names a dead prompt, and
;; the next ordinary `(m/? (m/sleep ...))` anywhere in the program is routed into
;; it -- surfacing, absurdly, as a fork-in-finally error on a task that forks
;; nothing. It made the suite alternate pass/fail.
;;
;; A process's body only ever runs on that process's own owner fiber (ADR-001),
;; so the association is not per-call at all: it is per-executor, fixed for the
;; process's lifetime. Register once, deregister once, nothing to restore.
(def ^:private prompts (atom {}))

(defn- register! [pr] (swap! prompts assoc (p/here) pr) nil)
(defn- deregister! [] (swap! prompts dissoc (p/here)) nil)
(defn- current [] (get @prompts (p/here)))

(defn in-cp? [] (some? (current)))

(defn switch [flow]
  (let [[tag v] (p/suspend (current) [::switch flow])]
    (if (= ::ok tag) v (throw v))))

;; A mutable cell, not a dynamic binding: bindings survive a continuation
;; capture but not a re-entry (see ebb.impl.prompt).
(defn- step! [^Process ps enter] (enter))

(defn run [body n t]
  (let [pr (p/prompt)
        ps (->Process n t pr body (server/spawn) true none [] #{} [] false false false)]
    (call! ps #(register! pr))
    ;; A continuous flow always has a value to give, so it notifies at once and
    ;; computes nothing until downstream actually samples.
    (set! (.-notified ps) true)
    (n)
    ps))
