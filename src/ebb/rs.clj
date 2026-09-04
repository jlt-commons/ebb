(ns ebb.rs
  "The Reactive Streams protocol, expressed in jolt.

  Missionary's `publisher`/`subscribe` sit on the `org.reactivestreams`
  interfaces. Jolt has no Java interfaces to implement, so ebb states the same
  three roles as protocols. The CONTRACT is unchanged -- demand is requested,
  never pushed; `on-next` is called at most `request`ed times; exactly one of
  `on-error`/`on-complete` terminates a subscription -- so anything already
  speaking Reactive Streams can be bridged by extending these.

  Ebb's own two implementations are `ebb.impl.pub` (a flow seen as a publisher)
  and `ebb.impl.sub` (a publisher seen as a flow); they are ports of
  missionary's Pub.java and Sub.java, which are the normative versions."
  (:refer-clojure :exclude [subscribe]))

(defprotocol Publisher
  (subscribe [pub sub]
    "Begin a subscription: call `on-subscribe` on sub with a Subscription."))

(defprotocol Subscriber
  (on-subscribe [sub s] "Called once, with the Subscription to drive.")
  (on-next      [sub x] "One value. Never more than were requested.")
  (on-error     [sub e] "Terminal: the stream failed.")
  (on-complete  [sub]   "Terminal: the stream ended."))

(defprotocol Subscription
  (request [s n] "Signal demand for n more values. n must be positive.")
  (cancel  [s]   "Stop; no further values will be delivered."))
