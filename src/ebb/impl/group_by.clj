;; Derived from missionary (EPL 2.0):
;;   logic           <- missionary/impl/GroupBy.cljs
;;   synchronisation <- missionary/impl/GroupBy.java  (the process monitor)
;;
;; ADR-001 rule 1 via `ebb.impl.arbiter`; see that header for the protocol.
;;
;; The pull loop breaks every time it hands a callback out, so `sample` and
;; `consume` run with the claim already held and give it back -- the loop and
;; the transfers never overlap. What the claim does NOT cover is the open
;; addressing table: `group` and `cancel` are the downstream consumer's calls,
;; they arrive on whatever executor that consumer runs on, and they insert into
;; and compact the same array the loop probes.
;;
;; So the table gets `locking`. It fits: the table work neither derefs nor
;; invokes anybody -- the Java already returns its callbacks and fires them
;; after the monitor -- so nothing inside the region can park (rule 5). The
;; input deref, the `kill` on the error path and every notifier and terminator
;; stay outside it.
(ns ^:no-doc ebb.impl.group-by
  (:require [ebb.impl.arbiter :as arb]
            [ebb.impl.util :refer [cancelled]]))

(declare kill group sample cancel consume)

(deftype Process [keyfn notifier terminator ^:unsynchronized-mutable key ^:unsynchronized-mutable value ^:unsynchronized-mutable input ^:unsynchronized-mutable table ^:unsynchronized-mutable load ^:unsynchronized-mutable live state ^:unsynchronized-mutable done]
  clojure.lang.IFn
  (invoke [p] (kill p) nil)
  (invoke [p n t] (group p n t))
  clojure.lang.IDeref
  (deref [p] (sample p)))

(deftype Group [process ^:unsynchronized-mutable key notifier terminator]
  clojure.lang.IFn
  (invoke [g] (cancel g) nil)
  clojure.lang.IDeref
  (deref [g] (consume g)))

(defn kill [^Process p]
  (when (.-live p)
    (set! (.-live p) false)
    ((.-input p))))

(defn step [i m]
  (bit-and (inc i) m))

(defn group [^Process p n t]
  (let [g (->Group p nil n t)]
    (locking p
      (let [k     (.-key p)
            table (.-table p)]
        (set! (.-key g) k)
        (when-not (identical? k p)
          (set! (.-key p) p)
          (let [s (alength table)
                m (dec s)]
            (loop [i (bit-and (hash k) m)]
              (case (aget table i)
                nil (aset table i g)
                (recur (step i m))))
            (let [ss (bit-shift-left s 1)]
              (when (<= ss (* 3 (set! (.-load p) (inc (.-load p)))))
                (let [mm (dec ss)
                      larger (object-array ss)]
                  (set! (.-table p) larger)
                  (dotimes [i s]
                    (when-some [h (aget table i)]
                      (loop [j (bit-and (hash (.-key h)) mm)]
                        (case (aget larger j)
                          nil (aset larger j h)
                          (recur (step j mm)))))))))))))
    (n) g))

(defn cancel [^Group g]
  (let [^Process p (.-process g)
        cb (locking p
             (let [k (.-key g)]
               (when (and (.-live p) (not (identical? k p)))
                 (set! (.-key g) p)
                 (let [table (.-table p)
                       m (dec (alength table))
                       i (loop [i (bit-and (hash k) m)]
                           (if (identical? g (aget table i))
                             i (recur (step i m))))]
                   (aset table i nil)
                   (set! (.-load p) (dec (.-load p)))
                   (loop [i (step i m)]
                     (when-some [h (aget table i)]
                       (let [j (bit-and (hash (.-key h)) m)]
                         (when-not (= i j)
                           (aset table i nil)
                           (loop [j j]
                             (if (nil? (aget table j))
                               (aset table j h)
                               (recur (step j m))))))
                       (recur (step i m))))
                   (if (= k (.-key p))
                     (.-notifier p)
                     (.-notifier g))))))]
    (when cb (cb))
    nil))

(defn- terminate-groups!
  "Fire every live group's terminator. The table is detached under the lock
  first, so nobody is compacting it while we walk it."
  [v-table]
  (when v-table
    (dotimes [i (alength v-table)]
      (when-some [g (aget v-table i)]
        ((.-terminator g))))))

(defn- rounds
  "Run rounds while the claim is held. Every branch but the discard breaks --
  the claim stays with whoever we just notified, and comes back through
  `sample` or `consume`."
  [^Process p]
  (loop []
    (cond
      (.-done p)
      (do (set! (.-live p) false)
          ;; NOT named `table` -- see doc/conformance.md.
          (terminate-groups! (locking p (let [v-table (.-table p)]
                                          (set! (.-table p) nil)
                                          v-table)))
          ((.-terminator p)))

      (identical? p (.-value p))
      (let [e (try (set! (.-value p) @(.-input p))
                   (set! (.-key p) ((.-keyfn p) (.-value p)))
                   nil
                   (catch Throwable e e))]
        (if (nil? e)
          ;; the probe is the only part that touches the table.
          ((locking p
             (let [table (.-table p)
                   k     (.-key p)
                   m     (dec (alength table))]
               (loop [i (bit-and (hash k) m)]
                 (if-some [h (aget table i)]
                   (if (= k (.-key h))
                     (.-notifier h)
                     (recur (step i m)))
                   (.-notifier p))))))
          (let [v-table (locking p (let [t (.-table p)]
                                     (set! (.-table p) nil)
                                     t))]
            (set! (.-value p) e)
            (kill p)
            (terminate-groups! v-table)
            ((.-notifier p)))))

      :else
      (do (try @(.-input p) (catch Throwable _ nil))
          (when (arb/release! (.-state p)) (recur))))))

(defn transfer
  "Entry from the input's notifier and terminator: take the claim, or leave a
  round due."
  [^Process p]
  (when (arb/claim! (.-state p)) (rounds p)))

(defn- pump!
  "Entry from a consumer that already holds the claim -- `sample`, `consume`,
  and `run`'s tail."
  [^Process p]
  (when (arb/release! (.-state p)) (rounds p)))

(defn sample [^Process p]
  (let [k (.-key p)]
    (if (identical? k p)
      (do (pump! p) (throw (.-value p)))
      (clojure.lang.MapEntry. k p))))

(defn consume [^Group g]
  (let [^Process p (.-process g)]
    (if (identical? p (.-key g))
      (do ((.-terminator g))
          (throw (cancelled "Group consumer cancelled.")))
      (let [x (.-value p)]
        (set! (.-value p) p)
        (set! (.-key p) p)
        (pump! p) x))))

(defn run [k f n t]
  ;; the claim starts held, as GroupBy.java's `busy = true` does.
  (let [p (->Process k n t nil nil nil (object-array 8) 0 true (arb/arbiter) false)]
    (set! (.-key p) p)
    (set! (.-value p) p)
    (set! (.-input p)
      (f #(transfer p)
        #(do (set! (.-done p) true)
             (transfer p))))
    (pump! p) p))
