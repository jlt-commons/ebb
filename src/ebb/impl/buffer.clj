;; Derived from missionary (EPL 2.0):
;;   logic           <- missionary/impl/Buffer.cljs
;;   synchronisation <- missionary/impl/Buffer.java  (the process monitor)
;;
;; ADR-001 rule 1 via `ebb.impl.arbiter`; see that header for the protocol.
;;
;; Buffer needs `locking` on top of the claim for the same reason Relieve does,
;; and one more besides. `more` hands the notifier out as soon as the ring goes
;; from empty to one, then KEEPS PULLING until the ring is full -- so the
;; consumer is inside `transfer` while the loop is still filling. The two share
;; the ring: `more` writes at `push` and grows `size`, `transfer` reads at
;; `pull` and shrinks it.
;;
;; The lock covers those field updates only; the input deref sits outside it,
;; because for an ap or cp input it parks (rule 5). `done` is therefore
;; snapshotted once per round rather than read twice as the Java does -- the
;; flow protocol puts the terminator after the last outstanding notification has
;; been answered, so within one round the two readings cannot disagree.
;;
;; `transfer` refills outside the lock too, and reads the slot it is about to
;; hand out next after that refill. That read needs no lock: it only happens
;; when the ring held two or more, and with two or more the refill's `push` slot
;; is never the next `pull` slot.
(ns ^:no-doc ebb.impl.buffer
  (:require [ebb.impl.arbiter :as arb]))

(declare kill transfer)
(deftype Process
  [notifier terminator ^:unsynchronized-mutable iterator buffer ^:unsynchronized-mutable failed ^:unsynchronized-mutable size ^:unsynchronized-mutable push ^:unsynchronized-mutable pull state ^:unsynchronized-mutable done]
  clojure.lang.IFn
  (invoke [ps] (kill ps))
  clojure.lang.IDeref
  (deref [ps] (transfer ps)))

(defn- rounds
  "Fill the ring while the claim is held. Answers the callback to fire outside,
  or nil. A FULL ring breaks out keeping the claim -- the Java's `break` -- and
  `transfer` is what gives it back."
  [^Process ps]
  (let [buffer (.-buffer ps)
        len    (alength buffer)]
    (loop [cb nil]
      (let [d   (.-done ps)
            res (when-not d (try [::ok @(.-iterator ps)]
                                 (catch Throwable e [::err e])))
            [cb full?]
            (locking ps
              (let [i (.-push ps)
                    s (.-size ps)]
                (set! (.-push ps) (mod (inc i) len))
                (let [cb (if (zero? s)
                           (if d (.-terminator ps) (.-notifier ps))
                           cb)]
                  (cond d                             (aset buffer i ps)
                        (identical? ::ok (nth res 0)) (aset buffer i (nth res 1))
                        :else (do (set! (.-failed ps) i)
                                  (aset buffer i (nth res 1))))
                  (set! (.-size ps) (inc s))
                  [cb (= (inc s) len)])))]
        (if full?
          cb
          (if (arb/release! (.-state ps)) (recur cb) cb))))))

(defn more [^Process ps]
  (when (arb/claim! (.-state ps)) (rounds ps)))

(defn transfer [^Process ps]
  (let [buffer (.-buffer ps)
        len    (alength buffer)
        snap   (locking ps
                 (let [i (.-pull ps)
                       s (.-size ps)
                       n (mod (inc i) len)
                       f (= i (.-failed ps))
                       x (aget buffer i)]
                   (aset buffer i nil)
                   (set! (.-pull ps) n)
                   (set! (.-size ps) (dec s))
                   [f x s n]))
        [f x s n] snap
        ;; the ring was full, so `rounds` broke out holding the claim: this is
        ;; a release, and it refills outside the lock.
        cb     (when (= s len)
                 (when (arb/release! (.-state ps)) (rounds ps)))
        cb     (if (= s 1)
                 cb
                 (if (identical? (aget buffer n) ps)
                   (.-terminator ps) (.-notifier ps)))]
    (when cb (cb))
    (if f (throw x) x)))

(defn kill [^Process ps]
  ((.-iterator ps)))

(deftype Flow [capacity input]
  clojure.lang.IFn
  (invoke [_ n t]
    (let [ps (->Process n t nil (object-array capacity) -1 0 0 0 (arb/arbiter) false)]
      (set! (.-iterator ps)
        (input #(when-some [cb (more ps)] (cb))
          #(do (set! (.-done ps) true)
               (when-some [cb (more ps)] (cb)))))
      ;; the flow came in holding the claim, so this is a release.
      (when-some [cb (when (arb/release! (.-state ps)) (rounds ps))] (cb))
      ps)))

(def flow ->Flow)
