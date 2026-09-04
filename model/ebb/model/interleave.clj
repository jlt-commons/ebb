(ns ebb.model.interleave
  "Exhaustive interleaving search, relationally.

  A concurrency bug that only shows up on 8+ carriers is a bug the test suite
  SAMPLES. Running it more times explores more schedules, but never tells you
  you have seen them all. These models enumerate every interleaving of a small
  number of threads over a shared state and report the bad ones, which is a
  proof over the model rather than evidence about a run."
  (:refer-clojure :exclude [==])
  (:require [clojure.core.logic :refer :all]))

(defne interleaveo
  "zs is an interleaving of xs and ys, preserving each one's own order."
  [xs ys zs]
  ([() _ ys])
  ([_ () xs])
  ([[x . xr] [y . _] [x . zr]] (interleaveo xr ys zr))
  ([[x . _] [y . yr] [y . zr]] (interleaveo xs yr zr)))

(defn runo
  "sn is the state reached by applying every action in as to s0, in order,
  through the step relation stepo."
  [stepo as s0 sn]
  (conde
    [(== as ()) (== s0 sn)]
    [(fresh [a ar s1]
       (conso a ar as)
       (stepo a s0 s1)
       (runo stepo ar s1 sn))]))

(defn schedules
  "Every interleaving of the given thread programs (a seq of action seqs)."
  [progs]
  (reduce (fn [acc prog] (mapcat (fn [a] (run* [z] (interleaveo a prog z))) acc))
          [(first progs)]
          (rest progs)))
