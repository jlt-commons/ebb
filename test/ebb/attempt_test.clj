;; Ported from missionary test/missionary/attempt_test.cljc (EPL 2.0).
;; Mechanical port: missionary.core -> ebb.core, and the
;; missionary.Cancelled import/predicate -> ebb's m/cancelled?.
(ns ebb.attempt-test
  (:require [lolcat.core :as lc]
            [lolcat.lib :as l]
            [ebb.core :as m]
            [clojure.test :as t])
  #?(:clj (:import [clojure.lang ExceptionInfo])))

(t/deftest success
  (t/is (= []
          (lc/run
            (l/store
              (m/attempt (l/task :input))
              (l/start :main (l/started :input))
              (l/succeed :input 1
                (l/succeeded :main #(= 1 (%)))))))))

(def err (ex-info "" {}))
(t/deftest failure
  (t/is (= []
          (lc/run
            (l/store
              (m/attempt (l/task :input))
              (l/start :main (l/started :input))
              (l/fail :input err
                (l/succeeded :main #(= err (try (%) (catch ExceptionInfo e e))))))))))

(t/deftest cancel
  (t/is (= []
          (lc/run
            (l/store
              (m/attempt (l/task :input))
              (l/start :main (l/started :input))
              (l/cancel :main
                (l/cancelled :input)))))))
