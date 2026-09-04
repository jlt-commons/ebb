;; Ported from missionary test/missionary/seed_test.cljc (EPL 2.0).
;; Mechanical port: missionary.core -> ebb.core, and the
;; missionary.Cancelled import/predicate -> ebb's m/cancelled?.
(ns ebb.seed-test
  (:require [lolcat.core :as lc]
            [lolcat.lib :as l]
            [ebb.core :as m]
            [clojure.test :as t]))

(t/deftest seed
  (t/is (= []
          (lc/run
            (l/store
              (m/seed [1 2 3])
              (l/spawn :main
                (l/notified :main))
              (l/transfer :main
                (l/notified :main))
              (l/check #{1})
              (l/cancel :main)
              (l/crash :main
                (l/terminated :main))
              (l/check m/cancelled?))))))
