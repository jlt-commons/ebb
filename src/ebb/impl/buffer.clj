;; Derived from missionary/impl/Buffer.cljs (missionary, EPL 2.0).
;; See Buffer.java for the synchronisation strategy (ADR-001 rule 1).
(ns ^:no-doc ebb.impl.buffer)

(declare kill transfer)
(deftype Process
  [notifier terminator ^:unsynchronized-mutable iterator buffer ^:unsynchronized-mutable failed ^:unsynchronized-mutable size ^:unsynchronized-mutable push ^:unsynchronized-mutable pull ^:unsynchronized-mutable busy ^:unsynchronized-mutable done]
  clojure.lang.IFn
  (invoke [ps] (kill ps))
  clojure.lang.IDeref
  (deref [ps] (transfer ps)))

(defn more [^Process ps]
  (let [buffer (.-buffer ps)]
    (loop [cb nil]
      (if (set! (.-busy ps) (not (.-busy ps)))
        (let [i (.-push ps)
              s (.-size ps)]
          (set! (.-push ps) (mod (inc i) (alength buffer)))
          (let [cb (if (zero? s) (if (.-done ps) (.-terminator ps) (.-notifier ps)) cb)]
            (if (.-done ps)
              (aset buffer i ps)
              (try (aset buffer i @(.-iterator ps))
                   (catch Throwable e
                     (set! (.-failed ps) i)
                     (aset buffer i e))))
            (if (= (set! (.-size ps) (inc s)) (alength buffer))
              cb (recur cb)))) cb))))

(defn transfer [^Process ps]
  (let [buffer (.-buffer ps)
        i (.-pull ps)
        s (.-size ps)
        n (mod (inc i) (alength buffer))
        f (= i (.-failed ps))
        x (aget buffer i)]
    (aset buffer i nil)
    (set! (.-pull ps) n)
    (set! (.-size ps) (dec s))
    (let [cb (when (= s (alength buffer)) (more ps))]
      (when-some [cb (if (= s 1)
                       cb (if (identical? (aget buffer n) ps)
                            (.-terminator ps) (.-notifier ps)))]
        (cb))) (if f (throw x) x)))

(defn kill [^Process ps]
  ((.-iterator ps)))

(deftype Flow [capacity input]
  clojure.lang.IFn
  (invoke [_ n t]
    (let [ps (->Process n t nil (object-array capacity) -1 0 0 0 true false)]
      (set! (.-iterator ps)
        (input #(when-some [cb (more ps)] (cb))
          #(do (set! (.-done ps) true)
               (when-some [cb (more ps)] (cb)))))
      (when-some [cb (more ps)] (cb)) ps)))

(def flow ->Flow)