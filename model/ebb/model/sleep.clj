(ns ebb.model.sleep
  "Does ebb.impl.sleep guarantee exactly one of success/failure?

  Sleep is the one task in ebb that is completed by two DIFFERENT threads: the
  runtime timer fires it, and whoever cancels the process cancels it. Missionary's
  Sleep.java arbitrates the two under the scheduler lock -- the timer nulls
  success/failure while holding S.L, and Process.invoke reads-and-nulls failure
  while holding the same lock -- so exactly one side wins.

  Ebb ported the .cljs logic, where a flag is enough because there is one thread.
  This model asks whether that still holds when there are two."
  (:refer-clojure :exclude [==])
  (:require [clojure.core.logic :refer :all]
            [ebb.model.interleave :as il]))

;; state: [pending timer-saw canceller-saw fired]
;;   pending  the flag both sides test and clear
;;   *-saw    what each side read, i.e. the value its `when` branched on
;;   fired    callbacks actually invoked, newest first

(defne stepo [action s0 s1]
  ;; --- the shipped version: read, clear, fire as three separate steps -------
  ([:t/read   [p _ c f]      [p p c f]])
  ([:t/clear  [_ t c f]      [false t c f]])
  ([:t/fire   [p true c f]   [p true c [:success . f]]])
  ([:t/fire   [p false c f]  [p false c f]])

  ([:c/read   [p t _ f]      [p t p f]])
  ([:c/clear  [p t c f]      [false t c f]])
  ([:c/fire   [p t true f]   [p t true [:failure . f]]])
  ([:c/fire   [p t false f]  [p t false f]])

  ;; --- the fix: claim the completion in ONE indivisible step, then fire ----
  ;; Whether that step is a compare-and-set or a test-and-clear under a lock
  ;; does not matter to the model, only that it is atomic. The shipped code
  ;; uses `locking`, which is also what missionary's Sleep.java does (the
  ;; scheduler lock), and avoids allocating an atom per sleep on a hot path.
  ([:t/claim  [true t c f]   [false true c f]])
  ([:t/claim  [false t c f]  [false false c f]])
  ([:c/claim  [true t c f]   [false t true f]])
  ([:c/claim  [false t c f]  [false t false f]]))

(def timer-shipped  [:t/read :t/clear :t/fire])
(def cancel-shipped [:c/read :c/clear :c/fire])
(def timer-cas      [:t/claim :t/fire])
(def cancel-cas     [:c/claim :c/fire])

(defn outcomes
  "Every interleaving of the two programs, with the callbacks each one fires."
  [timer canceller]
  (for [sched (il/schedules [timer canceller])]
    (let [fired (run* [f]
                  (fresh [p t c]
                    (il/runo stepo sched [true nil nil ()] [p t c f])))]
      [(vec sched) (reverse (first fired))])))

(defn report [label timer canceller]
  (let [os   (outcomes timer canceller)
        bad  (remove #(= 1 (count (second %))) os)]
    (println (format "%-28s %3d interleavings, %d violate exactly-once"
                     label (count os) (count bad)))
    (doseq [[sched fired] (take 3 bad)]
      (println "     " (pr-str sched) "->" (pr-str (vec fired))))
    (count bad)))

(defn -main [& _]
  (println "\nebb.impl.sleep -- is exactly one of success/failure guaranteed?\n")
  (let [shipped (report "shipped (flag, 3 steps)" timer-shipped cancel-shipped)
        fixed   (report "with an atomic claim"    timer-cas     cancel-cas)]
    (println)
    (if (and (pos? shipped) (zero? fixed))
      (println "The flag admits double completion; an atomic claim does not.")
      (println "Inconclusive -- the model did not separate the two."))
    (System/exit 0)))
