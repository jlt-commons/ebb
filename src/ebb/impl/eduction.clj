;; Derived from missionary (EPL 2.0):
;;   logic           <- missionary/impl/Eduction.cljs
;;   synchronisation <- missionary/impl/Eduction.java  (the PRESSURE counter)
;;
;; ADR-001 rule 1. Eduction is the one operator whose JVM impl does NOT use the
;; process monitor: it arbitrates with an atomic int, `PRESSURE`, and the .cljs
;; `busy` toggle is that counter collapsed onto a single thread. So the port is
;; the Java's counter, verbatim, on an atom -- no claim protocol, no lock.
;;
;;   pressure  0  the loop is running, nothing pending
;;            -1  the loop is idle
;;             1  the loop is running and a round is pending, or the loop has
;;                handed a terminal batch out and is waiting for the transfer
;;
;; and the rule is: whoever moves the counter TO ZERO runs the loop. A notifier
;; increments, a completed transfer decrements, and neither waits -- which is
;; also why the buffer needs no lock of its own. `pull` returns as soon as it
;; has handed a batch to the consumer and does not resume until the consumer's
;; last transfer brings the counter back to zero, so a fill and a drain are
;; never in flight together.
;;
;; NOTE the asymmetry ebb's earlier port had lost by folding both into one
;; toggle: the notifier and the terminator INCREMENT, while `run`'s tail and
;; `transfer` DECREMENT.
(ns ^:no-doc ebb.impl.eduction)

(declare cancel transfer)
(deftype Process
  [^:unsynchronized-mutable reducer ^:unsynchronized-mutable iterator notifier terminator ^:unsynchronized-mutable buffer ^:unsynchronized-mutable offset ^:unsynchronized-mutable length ^:unsynchronized-mutable error pressure ^:unsynchronized-mutable done]
  clojure.lang.IFn
  (invoke [t] (cancel t))
  clojure.lang.IDeref
  (deref [t] (transfer t)))


;; The cljs impl keeps a growable JS array and calls .push. A jolt array has a
;; fixed length, so the buffer grows by doubling instead -- a transducer such as
;; `cat` emits more values than it consumes, which is exactly what overruns it.
(defn feed
  ([^Process t] t)
  ([^Process t x]
   (let [b (.-buffer t)
         n (.-length t)]
     (if (= n (alength b))
       (let [b2 (object-array (max 8 (* 2 (alength b))))]
         (dotimes [i n] (aset b2 i (aget b i)))
         (aset b2 n x)
         (set! (.-buffer t) b2))
       (aset b n x)))
   (set! (.-length t) (inc (.-length t))) t))

(defn- rise!
  "A round has come due. True when that makes us the one to run the loop."
  [^Process t]
  (zero? (swap! (.-pressure t) inc)))

(defn- fall!
  "A round is over. True when another one is already due."
  [^Process t]
  (zero? (swap! (.-pressure t) dec)))

(defn pull [^Process t]
  (loop []
    (if (.-done t)
      (if-some [rf (.-reducer t)]
        (do (set! (.-offset t) 0)
            (set! (.-length t) 0)
            (try (rf t)
                 (catch Throwable e
                   (set! (.-error t) (.-length t))
                   (feed t e)))
            (set! (.-reducer t) nil)
            (if (zero? (.-length t))
              (recur)
              (do ((.-notifier t))
                  ;; the batch is out; the consumer's last transfer brings the
                  ;; counter back and finishes the loop off at the terminator.
                  (when (rise! t) (recur)))))
        ((.-terminator t)))
      (if-some [rf (.-reducer t)]
        (do (set! (.-offset t) 0)
            (set! (.-length t) 0)
            (try (when (reduced? (rf t @(.-iterator t)))
                   (rf t)
                   (set! (.-reducer t) nil)
                   (cancel t))
                 (catch Throwable e
                   (set! (.-error t) (.-length t))
                   (feed t e)
                   (set! (.-reducer t) nil)
                   (cancel t)))
            (if (pos? (.-length t))
              ((.-notifier t))
              (when (fall! t) (recur))))
        (do (try @(.-iterator t) (catch Throwable _ nil))
            (when (fall! t) (recur)))))))

(defn cancel [^Process t]
  ((.-iterator t)))

(defn transfer [^Process t]
  (let [o (.-offset t)
        x (aget (.-buffer t) o)]
    (aset (.-buffer t) o nil)
    (set! (.-offset t) (inc o))
    (if (= (.-offset t) (.-length t))
      (when (fall! t) (pull t))
      ((.-notifier t)))
    (if (= o (.-error t)) (throw x) x)))

(defn run [xf flow n t]
  (let [t    (->Process (xf feed) nil n t (object-array 1) 0 0 -1 (atom 0) false)
        wake #(when (rise! t) (pull t))]
    (set! (.-iterator t) (flow wake #(do (set! (.-done t) true) (wake))))
    (when (fall! t) (pull t))
    t))
