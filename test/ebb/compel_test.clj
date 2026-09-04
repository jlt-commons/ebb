;; Ported from missionary test/missionary/compel_test.cljc (EPL 2.0).
;; Mechanical port: missionary.core -> ebb.core, and the
;; missionary.Cancelled import/predicate -> ebb's m/cancelled?.
(ns ebb.compel-test
  (:require [lolcat.core :as lc]
            [lolcat.lib :as l]
            [ebb.core :as m]
            [clojure.test :as t]))

(t/deftest success
  (t/is (= []
          (lc/run
            (l/store
              (m/compel (l/task :input))
              (l/start :main (l/started :input))
              (l/cancel :main)          ; didn't cancel :input
              (l/succeed :input 1
                (l/succeeded :main #{1})))))))

(t/deftest failure
  (t/is (= []
          (lc/run
            (l/store
              (m/compel (l/task :input))
              (l/start :main (l/started :input))
              (l/cancel :main)          ; didn't cancel :input
              (l/fail :input 1
                (l/failed :main #{1})))))))
