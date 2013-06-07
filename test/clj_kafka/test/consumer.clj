(ns clj-kafka.test.consumer
  (:use [clojure.test]
        [clj-kafka.core :only (with-resource to-clojure)]
        [clj-kafka.producer :only (producer send-message)]
        [clj-kafka.test.utils :only (with-test-broker static-partitioner)])
  (:require [clj-kafka.consumer.zk :as zk]
            [clj-kafka.consumer.simple :as simp]))

(def producer-config {"metadata.broker.list" "localhost:9999"
                      "serializer.class" "kafka.serializer.DefaultEncoder"
                      "partitioner.class" "kafka.producer.DefaultPartitioner"})

;; TODO
;; Figure out why zk/messages can return the iterator sequence but
;; can't map the contents for us. Instead, use to-clojure directly
;; here for now
(deftest test-zookeeper-consumption
  (with-test-broker
    (let [p (producer producer-config)] 
      (with-resource [c (zk/consumer {"zookeeper.connect" "localhost:2182"
                                      "group.id" "clj-kafka.test.consumer"
                                      "auto.offset.reset" "smallest"
                                      "auto.commit.enable" "false"})]
        zk/shutdown
        (send-message p "test" "Hello, world")
        (let [msgs (zk/messages c "test")
              msg (first msgs)]
          (let [{:keys [topic offset partition key value]} msg]
            (is (= "test" topic))
            (is (= 0 offset))
            (is (= 0 partition))
            (is (= "Hello, world" (String. value "UTF-8")))))))))


(deftest test-simple-consumer
  (with-test-broker
    (let [p (producer producer-config)
          c (simp/consumer "localhost" 9999 "simple-consumer")]
      (send-message p "test" "Hello, world")
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
