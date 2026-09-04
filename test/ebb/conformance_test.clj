;; The gate on doc/conformance.md (ebb-8nq.22).
;;
;; jolt's test/conformance/known-divergences.edn is checked by `make certify`,
;; and ebb's registry needs the same or it rots: prose drifts from code, a
;; divergence's test gets renamed, an entry outlives the behaviour it describes.
;; doc/divergences.edn is the machine-readable half; this namespace is the check.
;;
;; It cannot detect an UNLISTED divergence. That needs missionary as an oracle,
;; and missionary needs a JVM -- which is the one thing ebb does without, so it
;; can never be a condition of `bin/test`. What it enforces is that nothing in
;; the registry can go quietly stale.
(ns ebb.conformance-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]))

(def ^:private registry-path "doc/divergences.edn")
(def ^:private prose-path    "doc/conformance.md")

(defn- registry [] (read-string (slurp registry-path)))
(defn- prose []    (slurp prose-path))

(defn- marker
  "How an id is spelled in the prose. An HTML comment, so it is invisible when
  the markdown is rendered and impossible to reword by accident."
  [id]
  (str "<!-- divergence: " (name id) " -->"))

(deftest every-entry-is-well-formed
  (doseq [e (registry)]
    (testing (str (:id e))
      (is (keyword? (:id e)))
      (is (#{:api :behaviour :host :harness} (:kind e)) "kind is one of the four")
      (is (string? (:summary e)))
      (is (not (str/blank? (:summary e))))
      (is (or (symbol? (:pinned-by e))
              (and (string? (:unpinned e)) (not (str/blank? (:unpinned e)))))
          "either a test pins it, or :unpinned says why one cannot"))))

(deftest ids-are-unique
  (let [ids (map :id (registry))]
    (is (= (count ids) (count (distinct ids))))))

(deftest every-pinning-test-still-exists
  ;; the rot this catches: a divergence's test renamed or deleted, leaving the
  ;; registry claiming a guarantee nothing checks any more
  (doseq [e (registry)
          :let [sym (:pinned-by e)]
          :when sym]
    (testing (str (:id e) " -> " sym)
      (let [ns-sym (symbol (namespace sym))]
        (is (nil? (require ns-sym)) "the namespace loads")
        (is (some? (ns-resolve (find-ns ns-sym) (symbol (name sym))))
            "the test var exists")
        (is (:test (meta (ns-resolve (find-ns ns-sym) (symbol (name sym)))))
            "and it is a test, not just any var")))))

(deftest every-entry-is-described-in-the-prose
  ;; the rot this catches: the prose deleted or rewritten past the entry
  (let [text (prose)]
    (doseq [e (registry)]
      (testing (str (:id e))
        (is (str/includes? text (marker (:id e)))
            (str "doc/conformance.md must carry " (marker (:id e))))))))

(deftest the-prose-describes-nothing-the-registry-has-not-heard-of
  ;; the rot this catches: a divergence written up in prose and never registered,
  ;; so no test is ever asked for
  (let [ids  (set (map (comp name :id) (registry)))
        seen (map second (re-seq #"<!-- divergence: ([a-z0-9-]+) -->" (prose)))]
    (doseq [m seen]
      (testing m
        (is (contains? ids m) "every marked id must be in doc/divergences.edn")))
    (is (= (count seen) (count (distinct seen))) "no id is marked twice")))
