(ns clj-kafka.producer
  (:import [kafka.javaapi.producer Producer ProducerData]
           [kafka.producer ProducerConfig]
           [kafka.message Message])
  (:use [clj-kafka.core :only (as-properties as-bytes)]))

(defn producer
  "Creates a Producer. m is the configuration
   serializer.class : default is kafka.serializer.DefaultEncoder
   zk.connect       : Zookeeper connection. e.g. localhost:2181 "
  [m]
  (Producer. (ProducerConfig. (as-properties m))))

(defn message
  "Creates a message with the specified payload.
   payload : bytes for the message payload. e.g. (.getBytes \"hello, world\")"
  [payload]
  (Message. payload))

(defn send-messages
  "Sends a message.
   topic   : a string
   msgs    : a single message, or sequence of messages to send"
  [producer topic msgs]
  (.send producer (ProducerData. topic msgs)))