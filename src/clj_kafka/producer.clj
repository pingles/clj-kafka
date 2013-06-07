(ns clj-kafka.producer
  (:import [kafka.javaapi.producer Producer]
           [kafka.producer ProducerConfig KeyedMessage]
           [kafka.message Message])
  (:use [clj-kafka.core :only (as-properties)]))

(defn producer
  "Creates a Producer. m is the configuration
   metadata.broker.list : \"server:port,server:port\""
  [m]
  ^Producer (Producer. (ProducerConfig. (as-properties m))))

(defn keyed-message
  ([topic value] (keyed-message topic nil value))
  ([topic key value] (KeyedMessage. topic key value)))

(defn send-message
  [^Producer producer ^String topic value]
  (println "Sending message ...")
  (.send producer (keyed-message topic value))
  (println "Sent message!"))


