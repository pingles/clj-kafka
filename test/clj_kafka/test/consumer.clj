(ns clj-kafka.test.consumer
  (:use [clojure.test]
        [clj-kafka.core :only (with-resource to-clojure)]
        [clj-kafka.producer :only (producer send-message message message-value)]
        [clj-kafka.test.utils :only (with-test-broker)])
  (:require [clj-kafka.consumer.zk :as zk]
            [clj-kafka.consumer.simple :as simp]))

(def producer-config {"metadata.broker.list" "localhost:9999"
                      "serializer.class" "kafka.serializer.DefaultEncoder"
                      "partitioner.class" "kafka.producer.DefaultPartitioner"})

(def test-broker-config {:zookeeper-port 2182
                         :kafka-port 9999
                         :topic "test"})

(deftest test-zookeeper-consumption
  (let [consumer-config {"zookeeper.connect" "localhost:2182"
                         "group.id" "clj-kafka.test.consumer"
                         "auto.offset.reset" "smallest"
                         "auto.commit.enable" "false"}]
    (testing "Sending single message"
      (with-test-broker test-broker-config
        (let [p (producer producer-config)] 
          (with-resource [c (zk/consumer consumer-config)]
            zk/shutdown
            (send-message p (message "test" (message-value "Hello, world")))
            (let [{:keys [topic offset partition key value]} (first (zk/messages c ["test"]))]
              (is (= "test" topic))
              (is (= 0 offset))
              (is (= 0 partition))
              (is (= "Hello, world" (String. value "UTF-8"))))))))
    (testing "Sending multiple messages"
      (with-test-broker test-broker-config
        (let [p (producer producer-config)] 
          (with-resource [c (zk/consumer consumer-config)]
            zk/shutdown
            (send-message p (message "test" (message-value "Hello, world")))
            (let [{:keys [topic offset partition key value]} (first (zk/messages c ["test"]))]
              (is (= "test" topic))
              (is (= 0 offset))
              (is (= 0 partition))
              (is (= "Hello, world" (String. value "UTF-8"))))))))))


(deftest test-simple-consumer
  (with-test-broker test-broker-config
    (let [p (producer producer-config)
          c (simp/consumer "localhost" 9999 "simple-consumer")]
      (send-message p (message "test" (message-value "Hello, world")))
      (let [msgs (simp/messages c
                                "clj-kafka.test.simple-consumer"
                                "test"
                                0
                                0
                                1024)
            msg (to-clojure (first msgs))]
        (let [{:keys [topic offset partition key value]} msg]
          (is (= nil topic))
          (is (= 0 offset))
          (is (= nil partition))
          (is (= nil key))
          (is (= "Hello, world" (String. value))))))))
