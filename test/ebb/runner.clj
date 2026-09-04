(ns ebb.runner
  "Discovers and runs every ebb test namespace.

  Test namespaces are the files under test/ebb whose name ends in `_test.clj`.
  Discovery is by directory scan so adding a test file is enough -- nothing to
  register, which is what makes the TDD loop in the epic cheap."
  (:require [clojure.test :as t]
            [clojure.string :as str]
            [jolt.fs :as fs]))

(defn- path->ns
  "test/ebb/foo_test.clj -> ebb.foo-test. Accepts absolute or relative paths."
  [p]
  (-> (str p)
      (str/replace #"^.*?\btest/" "")
      (str/replace #"\.clj$" "")
      (str/replace "_" "-")
      (str/replace "/" ".")
      symbol))

(defn test-namespaces
  "Every ebb.*-test namespace symbol, sorted."
  []
  (->> (fs/list-dir "test/ebb" "*_test.clj")
       (map path->ns)
       sort))

(defn run
  "Run the given namespaces (default: all discovered). Returns the summary.

  Namespaces run ONE AT A TIME with a progress line, rather than through a
  single (apply run-tests nses). The suite has hung outright often enough that
  knowing which namespace was in flight is worth more than tidy output -- with
  the batch call a hang produces nothing at all, because the summary is the only
  thing that prints on a clean run."
  ([] (run (test-namespaces)))
  ([nses]
   (when (empty? nses)
     (println "No test namespaces found under test/ebb."))
   (doseq [n nses] (require n))
   (reduce (fn [acc n]
             (print (str "  " n " ... ")) (flush)
             (let [t0 (System/currentTimeMillis)
                   s  (t/run-tests n)]
               (println (str (- (System/currentTimeMillis) t0) "ms"
                             (when (pos? (+ (:fail s 0) (:error s 0)))
                               (str "  <-- " (:fail s 0) " fail " (:error s 0) " error"))))
               (merge-with + (dissoc acc :type) (dissoc s :type))))
           {:test 0 :pass 0 :fail 0 :error 0}
           nses)))

(defn -main [& args]
  (let [nses (if (seq args) (map symbol args) (test-namespaces))
        {:keys [test pass fail error] :or {test 0 pass 0 fail 0 error 0}} (run nses)]
    (println (str "\nRan " test " tests. " pass " assertions passed, "
                  fail " failures, " error " errors."))
    (flush)
    (System/exit (if (pos? (+ fail error)) 1 0))))
