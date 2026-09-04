;; Derived from missionary/impl/GroupBy.cljs (missionary, EPL 2.0).
;; See GroupBy.java for the synchronisation strategy (ADR-001 rule 1).
(ns ^:no-doc ebb.impl.group-by
  (:require [ebb.impl.util :refer [cancelled]]))

(declare kill group sample cancel consume)

(deftype Process [keyfn notifier terminator ^:unsynchronized-mutable key ^:unsynchronized-mutable value ^:unsynchronized-mutable input ^:unsynchronized-mutable table ^:unsynchronized-mutable load ^:unsynchronized-mutable live ^:unsynchronized-mutable busy ^:unsynchronized-mutable done]
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
  (let [k (.-key p)
        g (->Group p k n t)
        table (.-table p)]
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
                      (recur (step j mm)))))))))))
    (n) g))

(defn cancel [^Group g]
  (let [^Process p (.-process g)
        k (.-key g)]
    (when (.-live p)
      (when-not (identical? k p)
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
          ((if (= k (.-key p))
             (.-notifier p)
             (.-notifier g))))))))

(defn transfer [^Process p]
  (loop []
    (when (set! (.-busy p) (not (.-busy p)))
      (if (.-done p)
        (do (set! (.-live p) false)
            ;; NOT named `table` -- see doc/conformance.md.
            (when-some [v-table (.-table p)]
              (set! (.-table p) nil)
              (dotimes [i (alength v-table)]
                (when-some [g (aget v-table i)]
                  ((.-terminator g)))))
            ((.-terminator p)))
        (if (identical? p (.-value p))
          (let [table (.-table p)]
            (try
              (let [k (set! (.-key p) ((.-keyfn p) (set! (.-value p) @(.-input p))))
                    m (dec (alength table))]
                (loop [i (bit-and (hash k) m)]
                  (if-some [h (aget table i)]
                    (if (= k (.-key h))
                      ((.-notifier h))
                      (recur (step i m)))
                    ((.-notifier p)))))
              (catch Throwable e
                (set! (.-value p) e)
                (set! (.-table p) nil)
                (kill p)
                (dotimes [i (alength table)]
                  (when-some [g (aget table i)]
                    ((.-terminator g))))
                ((.-notifier p)))))
          (do (try @(.-input p) (catch Throwable _ nil))
              (recur)))))))

(defn sample [^Process p]
  (let [k (.-key p)]
    (if (identical? k p)
      (do (transfer p) (throw (.-value p)))
      (clojure.lang.MapEntry. k p))))

(defn consume [^Group g]
  (let [^Process p (.-process g)]
    (if (identical? p (.-key g))
      (do ((.-terminator g))
          (throw (cancelled "Group consumer cancelled.")))
      (let [x (.-value p)]
        (set! (.-value p) p)
        (set! (.-key p) p)
        (transfer p) x))))

(defn run [k f n t]
  (let [p (->Process k n t nil nil nil (object-array 8) 0 true true false)]
    (set! (.-key p) p)
    (set! (.-value p) p)
    (set! (.-input p)
      (f #(transfer p)
        #(do (set! (.-done p) true)
             (transfer p))))
    (transfer p) p))
