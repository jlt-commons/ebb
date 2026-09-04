;; Derived from missionary/impl/Ambiguous.cljs (missionary, EPL 2.0).
;;
;; `ap` -- a discrete flow whose body may FORK. `?>` runs the continuation of
;; the fork point once per value of a flow, so that continuation has to be
;; re-enterable. Missionary gets this from cloroutine, whose coroutine object
;; has a clone arity: `backtrack` calls `((.-coroutine p) boot c)` to start a
;; fresh branch from the same suspension point.
;;
;; Ebb has no clone arity; it has a multi-shot delimited continuation instead
;; (ebb.impl.prompt). Resuming the SAME k twice IS the clone -- each resume runs
;; an independent branch and answers that branch's own result. So the substrate
;; swap is: where missionary steps a coroutine and reads a sentinel to know it
;; suspended, ebb resumes a continuation and reads a tagged result.
;;
;;   [::value x]              the branch finished, producing x
;;   [::suspend [::park t] k] the branch is waiting on task t
;;   [::suspend [::fork p f] k] the branch forked over flow f with parallelism p
;;
;; No fiber park happens inside an ap body: the prompt handles every suspension,
;; so an ap process needs no fiber of its own. It IS affine to whichever fiber
;; drives it -- see the note on marshalling at the bottom.
(ns ^:no-doc ebb.impl.ambiguous
  (:require [ebb.impl.prompt :as p]
            [ebb.impl.util :refer [nop cancelled]]
            [ebb.impl.server :as server]))

(declare pump! settle! cancel transfer step! handle! call! switch? register! deregister! current)

;; Two counters, each with one job:
;;
;;   branches  -- started and not yet produced a result. Governs TERMINATION.
;;                A branch that forks stops being a branch: it has become a
;;                Choice and will never produce a value itself.
;;   flight    -- per Choice: branches it started whose value has not yet been
;;                transferred downstream. Governs PARALLELISM, so it is `par`
;;                this is compared against, and it falls only on transfer.
;;
;; queue entries carry the Choice that produced them, so transfer knows whose
;; flight to decrement.
(deftype Process [notifier terminator prompt
                  owner
                  ^:unsynchronized-mutable live
                  ^:unsynchronized-mutable queue
                  ^:unsynchronized-mutable notified
                  ^:unsynchronized-mutable choices
                  ^:unsynchronized-mutable branches
                  ^:unsynchronized-mutable parks
                  ^:unsynchronized-mutable ended]
  clojure.lang.IFn
  (invoke [this] (call! this #(cancel this)) nil)
  clojure.lang.IDeref
  (deref [this] (call! this #(transfer this))))

(deftype Choice [k
                 parent                            ; the Choice whose branch forked, or nil
                 ^:unsynchronized-mutable iterator
                 par
                 ^:unsynchronized-mutable avail    ; upstream notifications not yet consumed
                 ^:unsynchronized-mutable flight
                 ^:unsynchronized-mutable done
                 ^:unsynchronized-mutable live
                 ;; An eager flow -- `seed` is the common one -- notifies from
                 ;; inside (flow n t), before the iterator has been assigned.
                 ;; Pumping then derefs nil. Notifications during construction
                 ;; only bump `avail`; the caller pumps once the choice is armed.
                 ^:unsynchronized-mutable armed
                 ^:unsynchronized-mutable reaped
                 ;; switch (par < 0) only: how to interrupt the branch currently
                 ;; in flight, so the next value can replace it
                 ^:unsynchronized-mutable interrupt])

;; --------------------------------------------------------------- branch steps

(defn- produced! [^Process ps owner tag v]
  (set! (.-branches ps) (dec (.-branches ps)))
  (set! (.-queue ps) (conj (.-queue ps) [tag v owner]))
  nil)

(defn- watch-park!
  "Remember a pending task's cancel fn so cancelling the process reaches it, and
  cancel it straight away if the process is already dead -- missionary's token
  rule, applied per suspended branch."
  [^Process ps c]
  (if (.-live ps)
    (set! (.-parks ps) (conj (.-parks ps) c))
    (c))
  nil)

(defn- handle!
  "Act on one result of starting or resuming a branch. `owner` is the Choice the
  branch came from, nil for the root branch."
  [^Process ps owner r]
  (cond
    (p/error? r) (produced! ps owner ::err (p/payload r))
    (p/value? r) (produced! ps owner ::ok (p/payload r))
    :else
    (let [[tag a b] (p/payload r)
          k         (p/continuation r)]
      (case tag
        ::park
        (let [fired (volatile! false)
              done! (fn [answer]
                      (call! ps (fn []
                                  (when-not @fired
                                    (vreset! fired true)
                                    (handle! ps owner
                                             (step! ps #(p/resume (.-prompt ps) k answer)))
                                    (pump! ps))))
                      nil)]
          (let [c (a (fn [x] (done! [::ok x])) (fn [e] (done! [::err e])))]
            (when (and owner (neg? (.-par ^Choice owner)))
              (set! (.-interrupt ^Choice owner) c))
            (watch-park! ps c)))

        ::fork
        ;; the branch becomes a Choice; it will not produce a value itself
        (do (set! (.-branches ps) (dec (.-branches ps)))
            (let [ch (->Choice k owner nil (if (number? a) a 1) 0 0 false true false false nil)]
              (set! (.-choices ps) (conj (.-choices ps) ch))
              (set! (.-iterator ch)
                    (b (fn [] (call! ps (fn []
                                          (set! (.-avail ch) (inc (.-avail ch)))
                                          (when (.-armed ch) (pump! ps)))) nil)
                       (fn [] (call! ps (fn []
                                          (set! (.-done ch) true)
                                          (when (.-armed ch) (pump! ps)))) nil)))
              (set! (.-armed ch) true)
              (when-not (.-live ps)
                (set! (.-live ch) false)
                ((.-iterator ch)))
              nil))))))

(defn- run-choice!
  "Consume one available upstream value and start a branch from it."
  [^Process ps ^Choice ch]
  (set! (.-avail ch) (dec (.-avail ch)))
  ;; `?<` interrupts the branch it is replacing. It cannot stop a running
  ;; continuation -- nothing can -- so it cancels whatever task that branch is
  ;; parked on, which surfaces as Cancelled at its `?`. That is exactly what
  ;; missionary's debounce relies on:
  ;;   (ap (let [x (?< flow)] (try (? (sleep d x)) (catch Cancelled _ (amb)))))
  (when (switch? ch)
    (when-some [c (.-interrupt ch)]
      (set! (.-interrupt ch) nil)
      (c)))
  (set! (.-flight ch) (inc (.-flight ch)))
  (set! (.-branches ps) (inc (.-branches ps)))
  (let [x (try [::ok @(.-iterator ch)] (catch Throwable e [::err e]))]
    (handle! ps ch (step! ps #(p/resume (.-prompt ps) (.-k ch) x)))))

;; ------------------------------------------------------------------- the pump

(defn- switch? [^Choice ch] (neg? (.-par ch)))

(defn- runnable? [^Choice ch]
  (and (not (.-reaped ch))
       (pos? (.-avail ch))
       ;; `?<` (par -1) never queues: a new value replaces the branch in flight
       (or (switch? ch) (< (.-flight ch) (.-par ch)))))

(defn- reap!
  "Retire every choice that is exhausted, releasing its parent's slot.

  A branch leaves its choice in one of two ways: it produces a value, and the
  slot is freed when that value is transferred; or it FORKS, becoming a child
  choice, and the slot stays held until that whole subtree is exhausted. Without
  this, a nested `?>` wedges the outer fork forever -- the outer slot is never
  returned, so it never pulls its next value and nothing terminates."
  [^Process ps]
  (loop []
    (let [spent (filter (fn [^Choice ch]
                          (and (not (.-reaped ch)) (.-done ch) (zero? (.-flight ch))))
                        (.-choices ps))]
      (when (seq spent)
        (doseq [^Choice ch spent]
          (set! (.-reaped ch) true)
          (when-some [^Choice up (.-parent ch)]
            (set! (.-flight up) (dec (.-flight up)))))
        (recur)))))

(defn- pump! [^Process ps]
  (loop []
    (reap! ps)
    (when-some [ch (first (filter runnable? (.-choices ps)))]
      (run-choice! ps ch)
      (recur)))
  (settle! ps))

(defn- finished? [^Process ps]
  (and (zero? (.-branches ps))
       (empty? (.-queue ps))
       (every? #(.-reaped ^Choice %) (.-choices ps))))

(defn- settle! [^Process ps]
  (cond
    (.-ended ps) nil
    (.-notified ps) nil
    (seq (.-queue ps)) (do (set! (.-notified ps) true) ((.-notifier ps)))
    (finished? ps) (do (set! (.-ended ps) true)
                       (call! ps deregister!)
                       (server/retire! (.-owner ps))
                       ((.-terminator ps)))
    :else nil))

;; ------------------------------------------------------- downstream interface

(defn- transfer [^Process ps]
  (set! (.-notified ps) false)
  (let [[tag v owner] (first (.-queue ps))]
    (set! (.-queue ps) (subvec (.-queue ps) 1))
    (when-some [^Choice ch owner]
      (set! (.-flight ch) (dec (.-flight ch))))
    (pump! ps)
    (if (= ::ok tag) v (throw v))))

(defn- cancel [^Process ps]
  (when (.-live ps)
    (set! (.-live ps) false)
    ;; NOT named `parks`: a local sharing a name with a mutable field reads the
    ;; FIELD, so this snapshot came back as the [] just assigned and every
    ;; park-cancel was silently skipped. See doc/conformance.md.
    (let [v-parks (.-parks ps)]
      (set! (.-parks ps) [])
      (doseq [c v-parks] (c)))
    (doseq [^Choice ch (.-choices ps)]
      (when (.-live ch)
        (set! (.-live ch) false)
        (when-some [it (.-iterator ch)] (it)))))
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

(defn in-ap?
  "True when the caller is inside an ap body, so `?` must suspend the branch
  rather than park a fiber."
  []
  (some? (current)))


;; A suspension answers [::ok v] or [::err e]; the body sees the value, or the
;; error thrown at the suspension point -- missionary's `resume`, which rethrows
;; when the choice is done. That is what lets an ap body catch a failing
;; upstream transfer with an ordinary try/catch.
(defn- answer [r]
  (let [[tag v] r]
    (if (= ::ok tag) v (throw v))))

(defn park [task]  (answer (p/suspend (current) [::park task])))
(defn fork [par f] (answer (p/suspend (current) [::fork par f])))

(defn- call! [^Process ps thunk] (server/call! (.-owner ps) thunk))


(defn- step!
  "Run one prompt entry. The prompt is already registered for this fiber, so
  there is nothing to set or restore."
  [^Process ps enter]
  (enter))

(defn run [body n t]
  (let [pr (p/prompt)
        ps (->Process n t pr (server/spawn) true [] false [] 1 [] false)]
    (call! ps (fn []
                (register! pr)
                (handle! ps nil (step! ps #(p/start pr body)))
                (pump! ps)))
    ps))
