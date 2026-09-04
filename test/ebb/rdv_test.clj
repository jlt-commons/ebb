;; Ported from missionary test/missionary/rdv_test.cljc (EPL 2.0).
;; Only change: `(partial instance? Cancelled)` -> `m/cancelled?`.
(ns ebb.rdv-test
  (:require [lolcat.core :as lc]
            [lolcat.lib :as l]
            [ebb.core :as m]
            [clojure.test :as t]))

(lc/defword give [v & events] [v (l/swap) (lc/call 1) (apply l/start :give events)])

(t/deftest success
  (t/testing "give then take"
    (t/is (= []
            (lc/run
              (l/store
                (m/rdv) (l/dup) (l/-rot) ; rdv store rdv
                (give 1) (l/swap)    ; store rdv
                (l/start :take
                  (l/succeeded :give nil?)
                  (l/succeeded :take #{1})))))))
  (t/testing "take then give"
    (t/is (= []
            (lc/run
              (l/store
                (m/rdv) (l/dup) (l/-rot) ; rdv store rdv
                (l/start :take) (l/swap) ; store rdv
                (give 1
                  (l/succeeded :take #{1})
                  (l/succeeded :give nil?))))))))

(t/deftest cancel
  (t/testing "give"
    (t/is (= []
            (lc/run
              (l/store
                (m/rdv)
                (give 1)
                (l/cancel :give
                  (l/failed :give m/cancelled?)))))))
  (t/testing "take"
    (t/is (= []
            (lc/run
              (l/store
                (m/rdv)
                (l/start :take)
                (l/cancel :take
                  (l/failed :take m/cancelled?))))))))
