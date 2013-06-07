(ns clj-kafka.producer
  (:import [kafka.javaapi.producer Producer]
           [kafka.producer ProducerConfig KeyedMessage]
           [kafka.message Message]
           [org.I0Itec.zkclient ZkClient]
           [kafka.utils ZkUtils])
  (:use [clj-kafka.core :only (as-properties with-resource)]
        [clojure.data.json :only (read-str)])
  (:require [zookeeper :as zk]))

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
  (.send producer ^KeyedMessage (keyed-message topic (message-payload value)))))

(defn brokers
  "Get brokers from zookeeper"
  [m]
  (with-resource [z (zk/connect (get m "zookeeper.connect"))]
    zk/close
    (doall (map (comp #(read-str % :key-fn keyword)
                      #(String. %)
                      :data
                      #(zk/data z (str "/brokers/ids/" %)))
                (zk/children z "/brokers/ids")))))
