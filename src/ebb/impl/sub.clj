;; Derived from missionary/impl/Sub.java (missionary, EPL 2.0).
;;
;; A Reactive Streams publisher seen as a discrete flow: the process IS the
;; subscriber, and also the flow's iterator (IFn to cancel, IDeref to transfer).
;; Where the Java implements org.reactivestreams.Subscriber, this implements
;; ebb.rs/Subscriber -- same contract, stated as a protocol (see ebb.rs).
;;
;; Per ADR-001 rule 1 the Java synchronises on the process, so this uses
;; `locking`, and notifier/terminator fire only after leaving it.
;;
;; `result` is a three-state cell, as in the Java:
;;   nil   running
;;   ps    completed normally (self-reference as sentinel)
;;   e     failed, or cancelled (a Cancelled ex-info)
;;
;; EVERY local here is prefixed `v-`. Locals that share a name with a deftype
;; field read the FIELD rather than the binding in some shapes -- a snapshot
;; taken before a `set!` then came back mutated, which silently dropped a value
;; per stream. Prefixing makes the collision unrepresentable rather than
;; relying on shadowing that does not always hold. See doc/conformance.md.
(ns ^:no-doc ebb.impl.sub
  (:require [ebb.rs :as rs]
            [ebb.impl.util :refer [cancelled]]))

(declare transfer)

(deftype Process [^:unsynchronized-mutable notifier
                  ^:unsynchronized-mutable terminator
                  ^:unsynchronized-mutable sub
                  ^:unsynchronized-mutable current
                  ^:unsynchronized-mutable result]
  clojure.lang.IFn
  (invoke [this]
    (let [v-snap (locking this
                   (let [v-sub (.-sub this)
                         v-res (.-result this)
                         v-cur (.-current this)]
                     (when (nil? v-res)
                       (set! (.-result this) (cancelled "Subscription cancelled.")))
                     [v-sub v-cur v-res]))
          v-sub  (nth v-snap 0)
          v-cur  (nth v-snap 1)
          v-res  (nth v-snap 2)]
      (when (nil? v-res)
        (when (some? v-sub) (rs/cancel v-sub))
        (when (nil? v-cur) ((.-notifier this)))))
    nil)

  clojure.lang.IDeref
  (deref [this] (transfer this)))

(defn- transfer [^Process ps]
  (let [v-snap (locking ps
                 (let [v-res (.-result ps)
                       v-cur (.-current ps)]
                   (set! (.-current ps) nil)
                   [v-res v-cur]))
        v-res  (nth v-snap 0)
        v-cur  (nth v-snap 1)]
    (if (nil? v-cur)
      (do ((.-terminator ps)) (throw v-res))
      (do (if (nil? v-res)
            (rs/request (.-sub ps) 1)
            ((if (identical? v-res ps) (.-terminator ps) (.-notifier ps))))
          v-cur))))

(extend-type Process
  rs/Subscriber
  (on-subscribe [this s]
    (when (nil? s) (throw (ex-info "on-subscribe called with nil" {})))
    (let [v-snap (locking this
                   (let [v-prev (.-sub this)
                         v-res  (.-result this)]
                     (when (and (nil? v-prev) (nil? v-res)) (set! (.-sub this) s))
                     [v-prev v-res]))]
      (if (and (nil? (nth v-snap 0)) (nil? (nth v-snap 1)))
        (rs/request s 1)
        (rs/cancel s)))
    nil)

  (on-next [this x]
    (when (nil? x) (throw (ex-info "on-next called with nil" {})))
    (let [v-res (locking this
                  (let [v-res (.-result this)]
                    (when (nil? v-res) (set! (.-current this) x))
                    v-res))]
      (when (nil? v-res) ((.-notifier this))))
    nil)

  (on-error [this e]
    (when (nil? e) (throw (ex-info "on-error called with nil" {})))
    (let [v-snap (locking this
                   (let [v-cur (.-current this)
                         v-res (.-result this)]
                     (when (nil? v-res) (set! (.-result this) e))
                     [v-cur v-res]))]
      (when (and (nil? (nth v-snap 1)) (nil? (nth v-snap 0))) ((.-notifier this))))
    nil)

  (on-complete [this]
    (let [v-snap (locking this
                   (let [v-cur (.-current this)
                         v-res (.-result this)]
                     (when (nil? v-res) (set! (.-result this) this))
                     [v-cur v-res]))]
      (when (nil? (nth v-snap 1))
        ((if (nil? (nth v-snap 0)) (.-terminator this) (.-notifier this)))))
    nil))

(defn run
  "Subscribe to pub, presenting it as a flow driven by n (notifier) and
  t (terminator)."
  [pub n t]
  (let [ps (->Process n t nil nil nil)]
    (rs/subscribe pub ps)
    ps))
