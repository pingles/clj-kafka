(ns clj-kafka.test.consumer
  (:use [clojure.test]
        [clj-kafka.test.utils :only (with-broker)]
        [clj-kafka.core :only (with-resource)]
        [clj-kafka.producer :only (producer to-message send-messages)])
  (:require [clj-kafka.consumer.zk :as zk]))

(deftest zookeeper-consumer
  (with-broker
    (let [p (producer {"metadata.broker.list" "localhost:9999"})]
      (send-messages p "test" (to-message "Hello, world"))
      (with-resource [c (zk/consumer {"zookeeper.connect" "127.0.0.1:2182"
                                      "group.id" "clj-kafka.test.consumer"
                                      "auto.offset.reset" "smallest"
                                      "auto.commit.enable" "false"})]
        zk/shutdown
        (is (= 1 1))))))