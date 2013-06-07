(ns clj-kafka.producer
  (:import [kafka.javaapi.producer Producer]
           [kafka.producer ProducerConfig KeyedMessage]
           [kafka.message Message])
  (:use [clj-kafka.core :only (as-properties)]))

(defprotocol MessagePayload
  "Converts message payloads to bytes"
  (message-payload [x]))

(extend-protocol MessagePayload
  (Class/forName "[B") (message-payload [bytes] bytes)
  String (message-payload [s] (.getBytes ^String s)))

(defn producer
  "Creates a Producer. m is the configuration
   metadata.broker.list : \"server:port,server:port\""
  [m]
  (Producer. (ProducerConfig. (as-properties m))))

(defn keyed-message
  ([topic value] (keyed-message topic nil value))
  ([topic key value] (KeyedMessage. topic key value)))

(defn send-message
  [^Producer producer ^String topic value]
  (.send producer ^KeyedMessage (keyed-message topic (message-payload value))))
