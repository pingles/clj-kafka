(ns clj-kafka.test.consumer
  (:use [clojure.test]
        [clj-kafka.core :only (with-resource)]
        [clj-kafka.producer :only (producer send-message)]
        [clj-kafka.test.utils :only (with-broker static-partitioner)])
  (:require [clj-kafka.consumer.zk :as zk]))

(deftest testing-something
  (with-broker
    (let [p (producer {"metadata.broker.list" "localhost:9999"
                       "serializer.class" "kafka.serializer.StringEncoder"
                       "partitioner.class" "kafka.producer.DefaultPartitioner"})] 
      (with-resource [c (zk/consumer {"zookeeper.connect" "localhost:2182"
                                      "group.id" "clj-kafka.test.consumer"
                                      "auto.offset.reset" "smallest"
                                      "auto.commit.enable" "false"})]
        zk/shutdown
        (println "Creating consumer sequence")
        (let [msg (first (zk/messages c "test"))]
          (println "Setup consumer, about to send a message...")
          (send-message p "test" "Hello, world")
          (println "Message SENT")
          (is (= "" (String. msg "UTF-8"))))))))
