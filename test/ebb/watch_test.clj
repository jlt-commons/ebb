;; Ported from missionary test/missionary/watch_test.cljc (EPL 2.0).
;; Mechanical port: missionary.core -> ebb.core, and the
;; missionary.Cancelled import/predicate -> ebb's m/cancelled?.
(ns ebb.watch-test
  (:require [lolcat.core :as lc]
            [lolcat.lib :as l]
            [ebb.core :as m]
            [clojure.test :as t]))

(lc/defword init [flow] [flow (l/spawn :main (l/notified :main))])

(t/deftest watch
  (let [!a (atom 0)]
    (t/is (= []
            (lc/run
              (l/store
                (init (m/watch !a))
                (l/transfer :main)
                (l/check #{0})
                #(swap! !a inc)
                (lc/call 0
                  (l/notified :main))
                (lc/drop 0)
                (l/transfer :main)
                (l/check #{1})
                (l/cancel :main
                  (l/notified :main))
                (l/crash :main
                  (l/terminated :main))
                (l/check m/cancelled?)))))))

(t/deftest cancel-before-transfer
  (let [!a (atom 0)]
    (t/is (= []
            (lc/run
              (l/store
                (init (m/watch !a))
                (l/cancel :main)
                (l/crash :main
                  (l/terminated :main))
                (l/check m/cancelled?)))))))
