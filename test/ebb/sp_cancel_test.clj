;; Cancellation semantics for sp, derived from missionary/impl/Sequential.java.
;; Missionary's own suite barely exercises these -- sp_test covers success and
;; failure only -- so the spec is the Java:
;;
;;   token starts at NOP and means "the thing to cancel right now".
;;   kill      if token non-nil, null it and invoke it. Idempotent.
;;   suspend   if token is null the process is already dead, so cancel the new
;;             task at once; otherwise the new task's cancel becomes the token.
;;   check     throw Cancelled when token is null.
;;
;; Cancellation is cooperative: it fails the pending task, it does not stop the
;; body. That is missionary's model and Erlang's, and on fibers it is the only
;; model available -- there is no safe way to unwind another fiber's stack.
(ns ebb.sp-cancel-test
  (:require [lolcat.core :as lc]
            [lolcat.lib :as l]
            [ebb.core :as m]
            [clojure.test :as t]))

(def err (ex-info "boom" {}))

(t/deftest cancel-reaches-the-parked-task
  (t/is (= []
          (lc/run
            (l/store
              (m/sp (m/? (l/task :input)))
              (l/start :main (l/started :input))
              (l/cancel :main (l/cancelled :input))
              (l/fail :input err (l/failed :main #{err})))))))

(t/deftest body-can-catch-cancellation-and-succeed
  (t/is (= []
          (lc/run
            (l/store
              (m/sp (try (m/? (l/task :input)) (catch Throwable _ :caught)))
              (l/start :main (l/started :input))
              (l/cancel :main (l/cancelled :input))
              (l/fail :input err (l/succeeded :main #{:caught})))))))

(t/deftest park-after-cancel-is-cancelled-at-once
  (t/testing "token is nil, so suspend cancels the new task without waiting"
    (t/is (= []
            (lc/run
              (l/store
                (m/sp (try (m/? (l/task :first)) (catch Throwable _ nil))
                      (m/? (l/task :second)))
                (l/start :main (l/started :first))
                (l/cancel :main (l/cancelled :first))
                (l/fail :first err
                  (l/started :second)
                  (l/cancelled :second))
                (l/fail :second err (l/failed :main #{err}))))))))

(t/deftest cancel-is-idempotent
  (t/is (= []
          (lc/run
            (l/store
              (m/sp (m/? (l/task :input)))
              (l/start :main (l/started :input))
              (l/cancel :main (l/cancelled :input))
              (l/cancel :main)                    ; no second [:cancelled :input]
              (l/cancel :main)
              (l/fail :input err (l/failed :main #{err})))))))

(t/deftest check-throws-once-cancelled
  (t/is (= []
          (lc/run
            (l/store
              (m/sp (try (m/? (l/task :input)) (catch Throwable _ nil))
                    (m/!)                          ; observes the cancellation
                    :not-reached)
              (l/start :main (l/started :input))
              (l/cancel :main (l/cancelled :input))
              (l/fail :input err (l/failed :main m/cancelled?)))))))

(t/deftest check-is-nil-while-live
  (t/is (= []
          (lc/run
            (l/store
              (m/sp (m/!) (m/? (l/task :input)))
              (l/start :main (l/started :input))
              (l/succeed :input :v (l/succeeded :main #{:v})))))))

(t/deftest a-body-that-never-parks-ignores-cancellation
  (t/testing "cooperative: nothing interrupts a running body"
    (let [t (m/sp :done)
          out (atom nil)]
      ((fn [] nil))
      (let [cancel (t (fn [v] (reset! out [:ok v])) (fn [e] (reset! out [:err e])))]
        (cancel)
        (t/is (= [:ok :done] @out))))))
