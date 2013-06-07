(ns clj-kafka.producer
  (:import [kafka.javaapi.producer Producer]
           [kafka.producer ProducerConfig KeyedMessage]
           [kafka.message Message])
  (:use [clj-kafka.core :only (as-properties)]))

(defn producer
  "Creates a Producer. m is the configuration
   metadata.broker.list: host1:port1,host1:port2"
  [m]
  ^Producer (Producer. (ProducerConfig. (as-properties m))))

(defn message
  "Creates a message with the specified payload.
   payload : bytes for the message payload. e.g. (.getBytes \"hello, world\")"
  [#^bytes payload]
  (Message. payload))

(defn- keyed-message
  [^String topic ^Message message]
  (KeyedMessage. topic nil message))

(defn send-message
  "Sends a message.
   topic   : a string
   msgs    : a single message, or sequence of messages to send"
  [^Producer producer ^String topic ^Message message]
  (.send producer (keyed-message topic message)))


(defprotocol ToMessage
  "Protocol to be extended to convert types to encoded Message objects"
  (to-message [x] "Creates a Message instance"))

(extend-protocol ToMessage
  String
  (to-message [x] (message (.getBytes x))))