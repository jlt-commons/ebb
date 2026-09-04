;; Derived from missionary/impl/Event.java (missionary, EPL 2.0).
;;
;; A pending waiter: the emitter that owns it plus the success/failure pair to
;; resume it with. Calling it cancels it through its emitter.
;;
;; The .cljs impls use a bare closure in a set for this. That is fine
;; single-threaded, but ebb's ports retry their CAS loops (ADR-001), and a
;; closure gives no stable identity to remove on a retry -- so ebb follows the
;; Java shape instead.
(ns ^:no-doc ebb.impl.event)

(defprotocol Emitter
  (-cancel [emitter event] "Remove event from emitter's waiter set and fail it."))

(deftype Event [emitter success failure]
  clojure.lang.IFn
  (invoke [this] (-cancel emitter this) nil))

(defn event [emitter success failure] (->Event emitter success failure))
