;; Ported from missionary test/missionary/never_test.cljc (EPL 2.0).
;; Mechanical port: missionary.core -> ebb.core, and the
;; missionary.Cancelled import/predicate -> ebb's m/cancelled?.
(ns ebb.never-test
  (:require [lolcat.core :as lc]
            [lolcat.lib :as l]
            [ebb.core :as m]
            [clojure.test :as t]))

(t/deftest simple
  (t/is (= []
          (lc/run
            (l/store
              m/never
              (l/start :main)
              (l/cancel :main
                (l/failed :main m/cancelled?)))))))
