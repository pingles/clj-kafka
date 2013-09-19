(ns clj-kafka.producer
  (:import [kafka.javaapi.producer Producer]
           [kafka.producer ProducerConfig KeyedMessage]
           [java.util List])
  (:use [clj-kafka.core :only (as-properties)]))

(defn producer
  "Creates a Producer. m is the configuration
   metadata.broker.list : \"server:port,server:port\""
  [m]
  (Producer. (ProducerConfig. (as-properties m))))

(defn message
  ([topic value] (message topic nil value))
  ([topic key value] (KeyedMessage. topic key value)))

(defn send-message
  [^Producer producer ^KeyedMessage message]
  (.send producer message))

(defn send-messages
  [^Producer producer ^List messages]
  (.send producer messages))
