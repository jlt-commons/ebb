;; Derived from missionary/impl/Pub.java (missionary, EPL 2.0).
;;
;; A discrete flow seen as a Reactive Streams publisher: each subscription runs
;; the flow once and pushes values as the subscriber requests them. Where the
;; Java implements org.reactivestreams.Subscription, this implements
;; ebb.rs/Subscription -- the same contract, stated as a protocol because jolt
;; has no Java interfaces (see ebb.rs).
;;
;; Per ADR-001 rule 1 the Java synchronises on the process, so this uses
;; `locking`, and every subscriber callback fires only AFTER leaving the
;; critical section -- onNext can re-enter through request.
;;
;; `requested` counts outstanding demand and goes to -1 to mean "a value is
;; parked in `current` waiting for demand", which is why the Java tests
;; `requested < 0` rather than `== 0`.
;;
;; EVERY local here is prefixed `v-`. A local sharing a name with a deftype
;; field reads the FIELD rather than the binding in some shapes, so a snapshot
;; taken before a `set!` comes back mutated. That silently dropped a value per
;; stream in the Sub port before it was found. See doc/conformance.md.
(ns ^:no-doc ebb.impl.pub
  (:require [ebb.rs :as rs]))

(declare ready)

(deftype Process [^:unsynchronized-mutable subscriber
                  ^:unsynchronized-mutable iterator
                  ^:unsynchronized-mutable current
                  ^:unsynchronized-mutable requested
                  ^:unsynchronized-mutable busy
                  ^:unsynchronized-mutable done])

(defn- kill [^Process ps e]
  (let [v-snap (locking ps
                 (let [v-req (.-requested ps)
                       v-sub (.-subscriber ps)]
                   (set! (.-subscriber ps) nil)
                   (when (neg? v-req) (set! (.-current ps) nil))
                   [v-req v-sub]))
        v-req  (nth v-snap 0)
        v-sub  (nth v-snap 1)]
    (when (some? v-sub)
      ((.-iterator ps))
      (when (neg? v-req) (ready ps))
      (when (some? e) (rs/on-error v-sub e)))))

(defn- more [^Process ps n]
  (let [v-snap (locking ps
                 (let [v-sub (.-subscriber ps)
                       v-cur (.-current ps)
                       v-req (.-requested ps)
                       v-sum (+ v-req n)]
                   ;; saturate rather than wrap: unbounded demand is the RS convention
                   (set! (.-requested ps) (if (neg? v-sum) Long/MAX_VALUE v-sum))
                   (when (and (some? v-sub) (neg? v-req)) (set! (.-current ps) nil))
                   [v-req v-cur v-sub]))
        v-req  (nth v-snap 0)
        v-cur  (nth v-snap 1)
        v-sub  (nth v-snap 2)]
    (when (and (some? v-sub) (neg? v-req))
      (rs/on-next v-sub v-cur)
      (ready ps))))

(defn- ready
  "Drain the iterator into the subscriber for as long as there is demand.

  `busy` is the Java's re-entrancy flip: a notification arriving while we are
  already draining flips it and returns, and the running drain loops again."
  [^Process ps]
  (loop []
    (let [v-snap (locking ps
                   (let [v-sub (.-subscriber ps)]
                     (set! (.-busy ps) (not (.-busy ps)))
                     (if (.-busy ps)
                       (cond
                         (nil? v-sub)
                         ;; nobody is listening: drop the pending value on the
                         ;; floor. One deref, throw swallowed -- missionary's
                         ;; Util.discard.
                         (do (when-not (.-done ps)
                               (try @(.-iterator ps) (catch Throwable _ nil)))
                             [false nil nil true])

                         (.-done ps)
                         (let [v-cur (.-current ps)]
                           (set! (.-current ps) nil)
                           (set! (.-subscriber ps) nil)
                           [true v-cur v-sub true])

                         :else
                         (try
                           (let [v-cur (deref (.-iterator ps))
                                 v-req (.-requested ps)]
                             (set! (.-requested ps) (dec v-req))
                             (if (zero? v-req)
                               (do (set! (.-current ps) v-cur) [false nil v-sub false])
                               [false v-cur v-sub true]))
                           (catch Throwable e
                             (set! (.-requested ps) Long/MAX_VALUE)
                             (set! (.-current ps) e)
                             [false nil v-sub true])))
                       [false nil v-sub false])))
          v-term (nth v-snap 0)
          v-cur  (nth v-snap 1)
          v-sub  (nth v-snap 2)
          v-more (nth v-snap 3)]
      (when v-more
        (cond
          v-term (if (nil? v-cur) (rs/on-complete v-sub) (rs/on-error v-sub v-cur))
          (nil? v-cur) nil
          :else (rs/on-next v-sub v-cur))
        (recur)))))

(extend-type Process
  rs/Subscription
  (request [this n]
    (if (pos? n)
      (more this n)
      (kill this (ex-info "Negative subscription request (3.9)" {})))
    nil)
  (cancel [this] (kill this nil) nil))

(defn run
  "Subscribe sub to one run of discrete flow f."
  [f sub]
  (let [ps (->Process sub nil nil 0 true false)]
    (set! (.-iterator ps)
          (f (fn [] (ready ps) nil)
             (fn [] (set! (.-done ps) true) (ready ps) nil)))
    (rs/on-subscribe sub ps)
    (ready ps)
    ps))
