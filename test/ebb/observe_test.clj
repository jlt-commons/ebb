;; Ported from missionary test/missionary/observe_test.cljc (EPL 2.0).
;; Mechanical port: missionary.core -> ebb.core, and the
;; missionary.Cancelled import/predicate -> ebb's m/cancelled?.
(ns ebb.observe-test
  (:require [lolcat.core :as lc]
            [lolcat.lib :as l]
            [ebb.core :as m]
            [clojure.test :as t]))

(t/deftest a-subject-that-is-immediately-ready
  (t/is (= []
          (lc/run
            (l/store
              (m/observe (fn [f] (f nil) #(do)))
              (l/spawn :main
                (l/notified :main))
              (l/transfer :main)
              (l/check nil?)
              (l/cancel :main
                (l/notified :main))
              (l/crash :main
                (l/terminated :main))
              (l/check m/cancelled?))))))

(defn interrupt-current!
  "PORT DIVERGENCE. Missionary interrupts the calling thread here, because its
  JVM Observe consults the interrupt flag. Ebb's Observe is ported from the
  .cljs impl and detects overflow structurally -- the consumer either holds an
  untransferred value or it does not -- so the interrupt is not part of what is
  under test.

  It is also actively harmful on jolt: the flag survives the test and the next
  `promise` deref on this thread, in an unrelated namespace, dies with
  InterruptedException. That cost a bisect. Left as a no-op so the lolcat
  program keeps its shape."
  []
  nil)

(t/deftest overflow
  (t/is (= []
          (lc/run
            (l/store
              (m/observe lc/event)
              (l/spawn :main
                (l/compose
                  (l/insert :f)
                  #(do)))
              (l/signal :f :x (l/notified :main))
              interrupt-current!
              (lc/call 0)
              (lc/drop 0)
              (l/signal-error :f :x))))))

;; unsubscribe fn is called after cancellation
;; we don't know when exactly should the unsubscription happen
(t/deftest cancellation
  (t/is (= []
          (lc/run
            (l/store
              (m/observe lc/event)
              (l/spawn :main
                (l/compose
                  (lc/drop 0)
                  #(lc/event :unsub)))
              (l/cancel :main
                (l/compose
                  (l/check #{:unsub})
                  nil)
                (l/notified :main))
              (l/crash :main
                (l/terminated :main))
              (l/check m/cancelled?))))))

;; subject throws exception after callback
(t/deftest subject-callback-throw
  (t/is (= []
          (lc/run
            (l/store
              (m/observe (fn [!] (! :foo) (assert false)))
              (l/spawn :main
                (l/notified :main))
              (l/crash :main
                (l/terminated :main))
              (l/check #(instance? #?(:clj Error :cljs js/Error) %)))))))