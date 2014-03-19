(ns clj-kafka.test.consumer.zk
  (:use [expectations]
        [clj-kafka.core :only (with-resource to-clojure)]
        [clj-kafka.producer :only (producer send-messages message)]
        [clj-kafka.test.utils :only (with-test-broker)])
  (:require [clj-kafka.consumer.zk :as zk]))

(def producer-config {"metadata.broker.list" "localhost:9999"
                      "serializer.class" "kafka.serializer.DefaultEncoder"
                      "partitioner.class" "kafka.producer.DefaultPartitioner"})

(def test-broker-config {:zookeeper-port 2182
                         :kafka-port 9999
                         :topic "test"})

(def consumer-config {"zookeeper.connect" "localhost:2182"
                      "group.id" "clj-kafka.test.consumer"
                      "auto.offset.reset" "smallest"
                      "auto.commit.enable" "false"})

(defn string-value
  [k]
  (fn [m]
    (String. (k m) "UTF-8")))

(defn test-message
  []
  (message "test" (.getBytes "Hello, world")))

(defn send-and-receive
  [messages]
  (with-test-broker test-broker-config
    (with-resource [c (zk/consumer consumer-config)]
      zk/shutdown
      (let [p (producer producer-config)]
        (send-messages p messages)
        (first (zk/messages c "test"))))))

(given (send-and-receive [(test-message)])
       (expect :topic "test"
               :offset 0
               :partition 0
               (string-value :value) "Hello, world"))
