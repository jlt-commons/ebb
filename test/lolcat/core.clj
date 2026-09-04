;; Derived from lolcat (https://github.com/leonoel/lolcat), vendored into
;; missionary as test/lolcat/core.cljc. EPL 2.0.
;;
;; Ported to jolt:
;;   * reader conditionals stripped; `catch :default` -> `catch Throwable`.
;;   * the context cell was a ThreadLocal. Jolt has no no-arg ThreadLocal ctor,
;;     and the obvious replacement -- a dynamic var -- is wrong here: jolt
;;     conveys bindings INTO a spawned fiber but a child's mutation cannot
;;     propagate back out, and lolcat's cell is read-modify-write. lolcat drives
;;     protocol callbacks synchronously from the test thread by design, so a
;;     plain atom is both correct and simpler. If a test ever needs to observe
;;     the context from another fiber, that is a real design question, not a
;;     cell-type question.
;;   * fn-name scraped the function's printed name. Jolt prints
;;     `clojure.lang.AFunction$fn__0@...` with no user name in it, so `defword`
;;     records the name in the function's metadata and fn-name reads that
;;     instead. Names are only used to label errors, but an unreadable label
;;     costs real debugging time in exactly the situations lolcat exists for.
;;   * a `call` WAITS for the events it declared. See below.
;;
;; ---------------------------------------------------------------------------
;; Events that arrive a moment late (ebb-8nq.29)
;;
;; lolcat is a synchronous DSL. `call` runs a function and requires every event
;; that function was declared to raise to have been raised BEFORE IT RETURNS --
;; `call-attempt` checks the declared list is empty and fails on the spot if it
;; is not. Missionary can hold to that, because its processes run inline on
;; whoever pokes them: a notifier IS the consumer's code, on the caller's own
;; thread.
;;
;; Ebb's `ap` and `cp` own a fiber (ADR-001 discipline 2), so every callback
;; they hand out is work for that fiber. Ebb used to make the handover
;; synchronous, which reproduced missionary's ordering exactly -- and violated
;; the protocol clause that a notifier, a terminator, a canceller and both task
;; continuations "must not block". Handing the work over and returning is what
;; the protocol asks for, and it makes the event arrive a moment after the call
;; that provoked it.
;;
;; So `call` now waits: each one carries a promise, delivered when the last of
;; its declared events has been consumed, and `call-attempt` blocks on that
;; promise for up to `*event-timeout*` before it checks. Nothing else changes --
;; the same events, in the same order, and a call with nothing outstanding does
;; not wait at all. What is relaxed is only WHEN: from "before the call
;; returned" to "before the next instruction runs".
;;
;; A test that fails now waits out the timeout before it says so. That is the
;; price of the ones that could not be expressed at all before.
(ns lolcat.core
  (:refer-clojure :exclude [drop])
  (:require [clojure.string :as s]))

(def ^:dynamic *event-timeout*
  "How long a `call` waits for an event raised from another executor. A
  deadline for a stall, not a budget for scheduling -- a passing test never
  reaches it."
  2000)

(defrecord Copy [offset])
(defrecord Drop [offset])
(defrecord Call [arity events])
(defrecord Word [f args])

(def ^:no-doc ^:private cell (atom nil))

(def ^:no-doc context
  (fn [f & args] (swap! cell #(apply f % args))))

(def ^:no-doc rethrow!
  #(when (contains? (context identity) :error)
     (let [e (:error (context identity))]
       (context dissoc :error) (throw e))))

(defn- fn-name [f]
  (or (some-> (meta f) :lolcat/name str)
      (let [n (str f)]
        (or (second (re-find #"\$([A-Za-z0-9_+?!*<>=-]+?)(?:__\d+)?@" n))
            n))))

(def ^:no-doc eval-inst!
  (letfn [(stack-copy [stack offset]
            (let [index (- (count stack) offset)]
              (conj stack (nth stack (dec index)))))
          (stack-drop [stack offset]
            (let [index (- (count stack) offset)]
              (into (subvec stack 0 (dec index))
                (subvec stack index))))
          (call-attempt [words stack arity gate]
            (let [f (dec (count stack))
                  r (try (constantly (apply (peek stack) (subvec stack (- f arity) f)))
                         (catch Throwable e
                           #(throw (ex-info (str "Call failure in:\n\n" (s/join " " words) "\n")
                                     {:arity arity
                                      :stack stack
                                      :words words} e))))
                  ;; the call may have handed its work to another executor, so
                  ;; give the events it declared a chance to land. Never touch a
                  ;; gate that is already open: a blocking deref on this thread
                  ;; is observable -- `overflow` in observe_test runs a call
                  ;; with the interrupt flag deliberately set.
                  _ (when-not (realized? gate) (deref gate *event-timeout* nil))
                  {:keys [words stack events]} (context identity)]
              (rethrow!)
              (when (some? events)
                (throw (ex-info "Missing events." {:words words :stack stack :events events
                                                   :waited *event-timeout*})))
              (conj stack (r))))]
    (fn [inst]
      (cond
        (instance? Copy inst)
        (context update :stack stack-copy (:offset inst))

        (instance? Drop inst)
        (context update :stack stack-drop (:offset inst))

        (instance? Call inst)
        (let [{:keys [events stack words drained]} (context identity)
              arity   (:arity inst)
              pending (seq (:events inst))
              gate    (promise)]
          ;; nothing declared, nothing to wait for
          (when (nil? pending) (deliver gate true))
          (context assoc
            :events pending
            :drained gate
            :stack (subvec stack 0 (- (count stack) (inc arity))))
          (context assoc
            :events events
            :drained drained
            :stack (call-attempt words stack arity gate)))

        (instance? Word inst)
        (let [{:keys [f args]} inst]
          (context update :words conj (fn-name f))
          (run! eval-inst! (apply f args))
          (context update :words pop))

        :push (context update :stack conj inst)))))

(def ^{:doc "
Return a pair containing the number of values respectively consumed and produced by the composition of given programs.
"} balance
  (letfn [(apply-stack-effect [r consumed produced]
            (let [d (- consumed (second r))]
              [(+ (first r) (max 0 d))
               (- produced (min 0 d))]))
          (events-stack-effect [r events]
            (reduce event-stack-effect r events))
          (event-stack-effect [r inst]
            (-> r
              (apply-stack-effect 0 1)
              (inst-stack-effect inst)
              (apply-stack-effect 1 0)))
          (insts-stack-effect [r insts]
            (reduce inst-stack-effect r insts))
          (inst-stack-effect [r inst]
            (cond
              (instance? Copy inst)
              (let [consumed (inc (:offset inst))]
                (apply-stack-effect r consumed (inc consumed)))

              (instance? Drop inst)
              (let [produced (:offset inst)]
                (apply-stack-effect r (inc produced) produced))

              (instance? Call inst)
              (-> r
                (apply-stack-effect (inc (:arity inst)) 0)
                (events-stack-effect (:events inst))
                (apply-stack-effect 0 1))

              (instance? Word inst)
              (insts-stack-effect r (apply (:f inst) (:args inst)))

              :push (apply-stack-effect r 0 1)))]
    (fn [& insts] (insts-stack-effect [0 0] insts))))

(def ^{:doc "
Must be called as a side effect of the evaluation of a `call` instruction providing the event definition as a program.
Produce given value, evaluate the program, consume a value and return it to the caller.
"} event
  (fn [x]
    (assert (context identity) "Undefined context.")
    ;; whose call is waiting on us, read before any nested call reassigns it
    (let [gate (:drained (context identity))]
      (try (rethrow!)
           (let [{:keys [words stack events]} (context identity)]
             (when (nil? events)
               (throw (ex-info (str "\n\nUnhandled event " x "\n") {:words words :stack stack :event x})))
             (let [rest-events (next events)]
               (context assoc :stack (conj stack x) :events rest-events)
               (eval-inst! (first events))
               (let [stack (:stack (context identity))]
                 (context assoc :stack (pop stack))
                 ;; the last one: release the call that was waiting for it,
                 ;; AFTER its program has run and the stack is settled
                 (when (nil? rest-events) (deliver gate true))
                 (peek stack))))
           (catch Throwable e
             (context assoc :error e)
             ;; do not make a failing test wait out the timeout. `gate` is nil
             ;; for an event raised with no call above it -- the "Unhandled
             ;; event" path -- and that error must not be masked by an NPE here.
             (some-> gate (deliver true))
             (throw (Error. "crashed")))))))

(def ^{:doc "
Evaluate given instructions.
"} run
  (fn [& insts]
    (let [[required] (apply balance insts)]
      (when (pos? required)
        (throw (ex-info (str "Insufficient data - required " required) {:insts insts})))
      (let [p (context identity)]
        (context {} {:words [] :stack []})
        (try (run! eval-inst! insts)
             (:stack (context identity))
             (finally (context {} p)))))))

(def ^{:doc "
Return a program consuming (inc n) values and producing this list appended with a copy of the head.
(= (run [x y z] (copy 2)) [x y z x])
"} copy
  (fn [n]
    (assert (nat-int? n))
    (->Copy n)))

(def ^{:doc "
Return a program consuming n values and producing the tail of this list.
(= (run [x y z] (drop 2)) [y z])
"} drop
  (fn [n]
    (assert (nat-int? n))
    (->Drop n)))

(def ^{:doc "
Return a program consuming (inc n) values and calling the last as a clojure function, passing the rest as arguments,
and producing the returned value. The function must call `event` as a side effect successively for each program
provided as extra arguments.
(= (run [inc 1] (call 1)) [2])
(= (run [event x] (call 1 (list (drop 0) (push y)))) [y])
"} call
  (fn [n & ps]
    (assert (nat-int? n))
    (->Call n ps)))

(defn word [f & args]
  (->Word f args))

(defmacro defword [sym args & body]
  `(def ~sym (partial word (with-meta (fn ~sym ~args ~@body)
                                      {:lolcat/name '~sym}))))
