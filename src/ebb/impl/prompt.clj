;; ebb.impl.prompt -- delimited continuations for ap and cp (ADR-001, ebb-8nq.13).
;;
;; `sp` needs no continuations at all: a fiber parks a real stack. `ap` and `cp`
;; do, because `?>` forks -- the continuation of the fork point runs once per
;; value of the flow, so it must be re-enterable. You cannot clone a fiber's
;; stack, so this is the one place ebb reaches below jolt.
;;
;; ---------------------------------------------------------------------------
;; Why a prompt, and why this implementation
;;
;; Raw `call/cc` captures the rest of the PROGRAM. Resuming such a continuation
;; re-runs whatever called us, which during development showed up as the driver
;; loop executing twice per fork. What `ap` wants is Guile's `call-with-prompt`:
;; capture only the extent between the prompt and the capture point, as a
;; COMPOSABLE continuation that returns a value to whoever invokes it, and may
;; be invoked more than once.
;;
;; Chez 10.4.1 has no native prompts -- checked: no call-with-prompt,
;; abort-to-prompt, shift, or continuation prompt tags. The principled
;; alternative is the Dybvig / Peyton Jones / Sabry delimited-control library,
;; which builds prompts on call/cc plus a metacontinuation cell. It is rejected
;; here on Oleg Kiselyov's own caveat: such a library "must be used with extreme
;; care in the presence of other effects: dynamic binding, exceptions, or
;; independent uses of call/cc". Ebb has all three at once -- jolt's fiber park
;; is itself a call/1cc capture, ebb binds dynamic vars per process, and the
;; bodies are ordinary Clojure with try/catch.
;;
;; So the prompt is built the narrow way instead: a cell holding the current
;; delimiter, in the shape Guile Fibers uses -- the driver re-enters the
;; continuation, the continuation never calls the driver.
;;
;;   start p thunk   ->  [::value v] | [::suspend tag k]
;;   resume p k v    ->  [::value v] | [::suspend tag k]
;;   suspend p tag   ->  called inside the extent; aborts to the driver
;;
;; TWO THINGS THAT LOOK LIKE DETAILS AND ARE NOT:
;;
;; 1. The NORMAL RETURN goes through the cell too, re-read at return time. If
;;    the extent returned through the `outer` captured by `start`, every later
;;    resume would deliver its value to the FIRST driver call, splicing the
;;    driver's own continuation into k. Re-reading is what delimits k.
;;
;; 2. That read must happen AFTER the thunk runs. `(@p [::value (thunk)])`
;;    evaluates the operator `@p` first, capturing the stale delimiter, and the
;;    driver runs twice per fork. Hence the explicit `let`.
;;
;; A dynamic `binding` cannot be used for the cell: bindings survive a capture
;; but NOT a re-entry -- invoking a continuation escapes the binding's extent,
;; so a resumed extent sees the outer value. An atom is not a shortcut here, it
;; is the only thing that works.
(ns ^:no-doc ebb.impl.prompt
  (:require [jolt.scheme :as scheme]
            [jolt.fibers :as fib]))

(def ^:private callcc (scheme/proc "call-with-current-continuation"))

;; --- the finally guard -------------------------------------------------------
;; Re-entering a continuation re-runs any dynamic-wind after-thunk between the
;; prompt and the capture, and jolt compiles `finally` to exactly that. Measured:
;; a `finally` whose `try` lexically contains a fork point runs N+1 times for N
;; branches, where the correct answer is N. `catch` is unaffected (no winder),
;; and fiber parks are unaffected (jolt strips finally winders on a park), so
;; this is narrow -- but it is silent, so it is refused rather than tolerated.
;;
;; Recognising a finally winder is the same test the fiber scheduler uses in
;; host/chez/fibers.ss: the winder's `in` thunk is the one shared marker.
(scheme/eval-string "
(define (ebb-finally-depth)
  (let ([rtd (guard (e (#t #f)) sa-winder-rtd)]
        [ws  (guard (e (#t '())) (sa-current-winders))])
    (if (not rtd) 0
      (let ([in-ref (record-accessor rtd 0)])
        (let loop ([w ws] [n 0])
          (cond
            [(null? w) n]
            [(and (record? (car w))
                  (eq? (record-rtd (car w)) rtd)
                  (eq? (in-ref (car w)) jolt-finally-marker))
             (loop (cdr w) (+ n 1))]
            [else (loop (cdr w) n)]))))))
")

(def ^:private finally-depth* (scheme/proc "ebb-finally-depth"))

(defn finally-depth
  "How many `finally` winders the current dynamic extent is inside."
  []
  (finally-depth*))

;; --- the prompt --------------------------------------------------------------

(deftype Prompt [cell         ; atom: the delimiter to return through, re-read
                 base])       ; atom: finally-depth at the last (re)entry

(deftype K [k owner depth])

(defn here
  "Who is executing: the carrier thread AND the fiber on it.

  The fiber alone is not an identity. `current-fiber` is nil off a fiber, so two
  different OS threads both read nil and a same-fiber check passes vacuously --
  which is how a timer thread came to resume a main-thread continuation
  unnoticed. A continuation belongs to a Scheme thread's stack whether or not a
  fiber is riding it."
  []
  [(Thread/currentThread) (fib/current-fiber)])

(defn prompt [] (->Prompt (atom nil) (atom 0)))

(defn- enter! [^Prompt p outer]
  (reset! (.-cell p) outer)
  nil)

(defn start
  "Run (thunk) under prompt p. Answers [::value v] when it returns,
  [::error e] when it throws, or [::suspend tag k] when it suspends."
  [^Prompt p thunk]
  (callcc (fn [outer]
             ;; The base is recorded HERE and never again. Resuming reinstates
             ;; the winder chain as captured, so a suspension's depth is always
             ;; measured against the prompt's original base. Re-recording it on
             ;; resume compares the extent's chain against the RESUMER's, and a
             ;; resumer at a shallower depth -- a timer callback, say, while the
             ;; prompt started inside clojure.test's try/finally -- reads as a
             ;; fork inside four finallys that do not exist.
             (reset! (.-base p) (finally-depth))
             (enter! p outer)
             ;; The catch is part of the EXTENT, so it is reinstated with every
             ;; resume and reports that resume's throw to that resume's caller.
             ;; Without it an exception unwinds along the reinstated stack and
             ;; lands in whatever try/catch was below the ORIGINAL start --
             ;; which then carries on as though the first entry had returned,
             ;; running the driver a second time. It is a `catch`, never a
             ;; `finally`: a finally here would be a winder inside the prompt.
             ;;
             ;; The let is load-bearing too: read the delimiter AFTER the thunk.
             (let [r (try [::value (thunk)]
                          (catch Throwable e [::error e]))]
               (@(.-cell p) r)))))

(defn resume
  "Re-enter k with v. Answers [::value v] or [::suspend tag k], like start.
  k may be resumed more than once; each call answers that branch's own result."
  [^Prompt p ^K k v]
  (when-not (= (.-owner k) (here))
    ;; Not pedantry: resuming a continuation captured on another fiber transfers
    ;; control into a stack segment this carrier is not running, and jolt
    ;; documents that as a HANG with no error at all (jolt.continuations). A
    ;; thrown error is the only acceptable failure here.
    (throw (ex-info "Continuation resumed on a different fiber than it was captured on."
                    {:type ::wrong-fiber
                     :captured-on (.-owner k)
                     :resumed-on  (here)})))
  (callcc (fn [outer]
             (enter! p outer)
             ((.-k k) v))))

(defn suspend
  "Abort to p's driver, handing it tag and a resumable continuation of the rest
  of this extent. Refuses to capture from inside a `finally`."
  [^Prompt p tag]
  (let [depth (- (finally-depth) @(.-base p))]
    (when (pos? depth)
      (throw (ex-info (str "Cannot fork inside a try/finally: jolt compiles finally to a "
                           "dynamic-wind after-thunk, which would run once per re-entry "
                           "(N+1 times for N branches). Move the fork outside the try, or "
                           "use catch, which is unaffected.")
                      {:type ::fork-in-finally :depth depth})))
    (let [owner (here)]
      (callcc (fn [k]
                (@(.-cell p) [::suspend tag (->K k owner depth)]))))))

(defn value?   [r] (= ::value   (nth r 0)))
(defn error?   [r] (= ::error   (nth r 0)))
(defn suspend? [r] (= ::suspend (nth r 0)))
(defn payload  [r] (nth r 1))
(defn continuation [r] (nth r 2))
