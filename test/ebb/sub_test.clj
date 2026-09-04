;; Ported from missionary test/missionary/sub_test.clj (EPL 2.0).
;; The org.reactivestreams interfaces become ebb.rs protocols -- same roles,
;; same contract, stated in Clojure because jolt has no Java interfaces. The
;; lolcat programs below are missionary's, unchanged; `Cancelled` becomes
;; ebb's m/cancelled? predicate.
(ns ebb.sub-test
  (:require [lolcat.core :as lc]
            [lolcat.lib :as l]
            [ebb.core :as m]
            [ebb.rs :as rs]
            [clojure.test :as t]))

(defn publisher [id]
  (reify rs/Publisher
    (subscribe [_ s]
      (lc/event [:subscribed id s]))))

(defn subscription [id]
  (reify rs/Subscription
    (request [_ n] (lc/event [:requested id n]))
    (cancel [_] (lc/event [:cancelled id]))))

(defn subscribe! [subscriber subscription]
  (rs/on-subscribe subscriber subscription))

(defn complete! [subscriber]
  (rs/on-complete subscriber))

(defn error! [subscriber error]
  (rs/on-error subscriber error))

(defn next! [subscriber value]
  (rs/on-next subscriber value))

(lc/defword subscribed [id]
  [(l/bi peek pop)
   (l/check #{[:subscribed id]})
   (l/insert id)
   nil])

(lc/defword requested [id n]
  [(l/check #{[:requested id n]}) nil])

(lc/defword cancelled [id]
  [(l/check #{[:cancelled id]}) nil])

(lc/defword on-subscribe [id & events]
  [(lc/copy 0)
   (l/change get id)
   (subscription id)
   subscribe!
   (apply lc/call 2 events)
   (lc/drop 0)])

(lc/defword on-next [id & events]
  [(lc/copy 1)
   (l/change get id)
   (l/swap)
   next!
   (apply lc/call 2 events)
   (lc/drop 0)])

(lc/defword on-error [id & events]
  [(lc/copy 1)
   (l/change get id)
   (l/swap)
   error!
   (apply lc/call 2 events)
   (lc/drop 0)])

(lc/defword on-complete [id & events]
  [(lc/copy 0)
   (l/change get id)
   complete!
   (apply lc/call 1 events)
   (lc/drop 0)])

(t/deftest cancel-initializing
  (t/is (= []
          (lc/run
            (l/store
              (m/subscribe (publisher :input))
              (l/spawn :main
                (subscribed :input))
              (l/cancel :main
                (l/notified :main))
              (l/crash :main
                (l/terminated :main))
              (l/check m/cancelled?)
              (on-subscribe :input
                (cancelled :input)))))))

(t/deftest cancel-waiting
  (t/is (= []
          (lc/run
            (l/store
              (m/subscribe (publisher :input))
              (l/spawn :main
                (subscribed :input))
              (on-subscribe :input
                (requested :input 1))
              (l/cancel :main
                (cancelled :input)
                (l/notified :main))
              :ignored (on-next :input)
              (l/crash :main
                (l/terminated :main))
              (l/check m/cancelled?))))))

(t/deftest cancel-ready
  (t/is (= []
          (lc/run
            (l/store
              (m/subscribe (publisher :input))
              (l/spawn :main
                (subscribed :input))
              (on-subscribe :input
                (requested :input 1))
              :foo (on-next :input
                     (l/notified :main))
              (l/cancel :main
                (cancelled :input))
              (l/transfer :main
                (l/notified :main))
              (l/check #{:foo})
              (l/crash :main
                (l/terminated :main))
              (l/check m/cancelled?))))))

(t/deftest cancel-done
  (t/is (= []
          (lc/run
            (l/store
              (m/subscribe (publisher :input))
              (l/spawn :main
                (subscribed :input))
              (on-subscribe :input
                (requested :input 1))
              (on-complete :input
                (l/terminated :main))
              (l/cancel :main))))))

(def err (ex-info "Publisher failure." {}))

(t/deftest failure-waiting
  (t/is (= []
          (lc/run
            (l/store
              (m/subscribe (publisher :input))
              (l/spawn :main
                (subscribed :input))
              (on-subscribe :input
                (requested :input 1))
              err (on-error :input
                    (l/notified :main))
              (l/crash :main
                (l/terminated :main))
              (l/check #{err}))))))

(t/deftest failure-ready
  (t/is (= []
          (lc/run
            (l/store
              (m/subscribe (publisher :input))
              (l/spawn :main
                (subscribed :input))
              (on-subscribe :input
                (requested :input 1))
              :foo (on-next :input
                     (l/notified :main))
              err (on-error :input)
              (l/transfer :main
                (l/notified :main))
              (l/check #{:foo})
              (l/crash :main
                (l/terminated :main))
              (l/check #{err}))))))