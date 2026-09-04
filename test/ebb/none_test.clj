;; Ported from missionary test/missionary/none_test.cljc (EPL 2.0).
;; Mechanical port: missionary.core -> ebb.core, and the
;; missionary.Cancelled import/predicate -> ebb's m/cancelled?.
(ns ebb.none-test
  (:require [lolcat.core :as lc]
            [lolcat.lib :as l]
            [ebb.core :as m]
            [clojure.test :as t]))

(t/deftest none
  (t/is (= []
          (lc/run
            (l/store
              m/none
              (l/spawn :main
                (l/terminated :main))
              (l/cancel :main))))))
