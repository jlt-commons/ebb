;; Ported from missionary test/missionary/dfv_test.cljc (EPL 2.0).
;; Only change: missionary throws a `missionary.Cancelled` instance, ebb an
;; ex-info carrying ::cancelled, so `(partial instance? Cancelled)` becomes
;; `m/cancelled?`.
(ns ebb.dfv-test
  (:require [lolcat.core :as lc]
            [lolcat.lib :as l]
            [ebb.core :as m]
            [clojure.test :as t]))

(lc/defword assign [v & events]
  [v (l/swap) (apply lc/call 1 events) (l/lose)])

(t/deftest success
  (t/testing "value ready"
    (t/is (= []
            (lc/run
              (l/store
                (m/dfv)
                (l/dup) (assign :fine)
                (l/start :main
                  (l/succeeded :main #{:fine})))))))
  (t/testing "value not ready"
    (t/is (= []
            (lc/run
              (l/store
                (m/dfv) (l/dup) (l/-rot) ; dfv store dfv
                (l/start :main)
                (l/swap) (assign :fine (l/succeeded :main #{:fine})))))))
  (t/testing "only first assignment counts"
    (t/is (= []
            (lc/run
              (l/store
                (m/dfv)
                (l/dup) (l/dup) (assign :first) (assign :second)
                (l/start :main
                  (l/succeeded :main #{:first}))))))))

(t/deftest cancel
  (t/is (= []
          (lc/run
            (l/store
              (m/dfv)
              (l/start :main)
              (l/cancel :main
                (l/failed :main m/cancelled?)))))))
