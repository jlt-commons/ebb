;; Ported from missionary test/missionary/pub_test.clj (EPL 2.0).
;; The org.reactivestreams interfaces become ebb.rs protocols -- same roles,
;; same contract, stated in Clojure because jolt has no Java interfaces. The
;; lolcat programs below are missionary's, unchanged.
(ns ebb.pub-test
  (:require [lolcat.core :as lc]
            [lolcat.lib :as l]
            [ebb.core :as m]
            [ebb.rs :as rs]
            [clojure.test :as t]))

(defn subscriber [id]
  (reify rs/Subscriber
    (on-subscribe [_ s] (lc/event [:on-subscribe id s]))
    (on-next [_ x] (lc/event [:on-next id x]))
    (on-complete [_] (lc/event [:on-complete id]))
    (on-error [_ e] (lc/event [:on-error id e]))))

(defn subscribe! [publisher subscriber]
  (rs/subscribe publisher subscriber))

(defn request! [subscription n]
  (rs/request subscription (long n)))

(defn cancel! [subscription]
  (rs/cancel subscription))

(lc/defword subscribe [id & events]
  [(subscriber id)
   subscribe!
   (apply lc/call 2 events)
   (lc/drop 0)])

(lc/defword subscribed [id & insts]
  (-> [(l/bi peek pop)
       (l/check #{[:on-subscribe id]})
       (l/insert id)]
    (into insts)
    (conj nil)))

(lc/defword nexted [id & insts]
  (-> [(l/bi peek pop)
       (l/check #{[:on-next id]})]
    (into insts)
    (conj nil)))

(lc/defword completed [id & insts]
  (-> [(l/check #{[:on-complete id]})]
    (into insts)
    (conj nil)))

(lc/defword errored [id & insts]
  (-> [(l/bi peek pop)
       (l/check #{[:on-error id]})]
    (into insts)
    (conj nil)))

(lc/defword cancel [id & events]
  [(lc/copy 0)
   (l/change get id)
   cancel!
   (apply lc/call 1 events)
   (lc/drop 0)])

(lc/defword request [id n & events]
  [(lc/copy 0)
   (l/change get id)
   n request!
   (apply lc/call 2 events)
   (lc/drop 0)])

(t/deftest empty-flow
  (t/is (= []
          (lc/run
            (l/store
              (m/publisher (l/flow :input))
              (subscribe :main
                (l/spawned :input)
                (subscribed :main))
              (l/terminate :input
                (completed :main)))))))

(t/deftest request-before-ready
  (t/is (= []
          (lc/run
            (l/store
              (m/publisher (l/flow :input))
              (subscribe :main
                (l/spawned :input)
                (subscribed :main))
              (request :main 1)
              (l/notify :input
                (l/transferred :input :foo)
                (nexted :main
                  (l/check #{:foo})))
              (l/terminate :input
                (completed :main)))))))

(t/deftest request-after-ready
  (t/is (= []
          (lc/run
            (l/store
              (m/publisher (l/flow :input))
              (subscribe :main
                (l/spawned :input)
                (subscribed :main))
              (l/notify :input
                (l/transferred :input
                  (l/notify :input)
                  :foo))
              (request :main 1
                (nexted :main
                  (l/check #{:foo}))
                (l/transferred :input
                  (l/terminate :input)
                  :bar))
              (request :main 1
                (nexted :main
                  (l/check #{:bar}))
                (completed :main)))))))

(def err (ex-info "Publisher failure." {}))

(t/deftest failure
  (t/is (= []
          (lc/run
            (l/store
              (m/publisher (l/flow :input))
              (subscribe :main
                (l/spawned :input)
                (subscribed :main))
              (l/notify :input
                (l/crashed :input err))
              (l/terminate :input
                (errored :main
                  (l/check #{err}))))))))

(t/deftest cancel-waiting
  (t/is (= []
          (lc/run
            (l/store
              (m/publisher (l/flow :input))
              (subscribe :main
                (l/spawned :input)
                (subscribed :main))
              (cancel :main
                (l/cancelled :input
                  (l/notify :input
                    (l/crashed :input
                      (l/terminate :input)
                      err)))))))))

(t/deftest cancel-ready
  (t/is (= []
          (lc/run
            (l/store
              (m/publisher (l/flow :input))
              (subscribe :main
                (l/spawned :input)
                (subscribed :main))
              (l/notify :input
                (l/transferred :input
                  (l/notify :input)
                  :foo))
              (cancel :main
                (l/cancelled :input)
                (l/crashed :input
                  (l/terminate :input)
                  err)))))))

(t/deftest cancel-crashed
  (t/is (= []
          (lc/run
            (l/store
              (m/publisher (l/flow :input))
              (subscribe :main
                (l/spawned :input)
                (subscribed :main))
              (l/notify :input
                (l/crashed :input
                  err))
              (cancel :main
                (l/cancelled :input))
              (l/terminate :input))))))