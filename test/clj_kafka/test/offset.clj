(ns clj-kafka.test.offset
  (:use [expectations]
        [clj-kafka.core :only (with-resource)]
        [clj-kafka.producer :only (producer send-messages message)]
        [clj-kafka.test.utils :only (with-test-broker)]
        [clj-kafka.offset :only (fetch-consumer-offsets reset-consumer-offsets)])
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
                      "offsets.storage" "kafka"
                      "auto.commit.interval.ms" "100"
                      "auto.commit.enable" "true"})

(defn test-message
  [body]
  (message "test" (.getBytes body)))

(defn send-consume-fetch
  [messages]
  (with-test-broker test-broker-config
    (with-resource [c (zk/consumer consumer-config)]
      zk/shutdown
      (let [p (producer producer-config)]
        (send-messages p messages)

        (let [msgs (take 3 (zk/messages c "test"))]
          (str "Received msgs: " msgs))
        (zk/shutdown c)
        (Thread/sleep 100)

        (let [offset-response (fetch-consumer-offsets "localhost:9999,localhost:9999" consumer-config "test" "clj-kafka.test.consumer")
              ignore (str offset-response)
              offset (.offset (get offset-response "test:0"))]
          offset)))))

(given (send-consume-fetch [(test-message "msg 1")  (test-message "msg 2")  (test-message "msg 3")])
       (expect identity 3))

(defn send-consume-reset-fetch
  [messages]
  (with-test-broker test-broker-config
    (with-resource [c (zk/consumer consumer-config)]
      zk/shutdown
      (let [p (producer producer-config)]
        (send-messages p messages)

        (let [msgs (take 2 (zk/messages c "test"))]
          (str "Received msgs: " msgs))
        (zk/shutdown c)
        (Thread/sleep 100)

        (let [offset-reset-response (reset-consumer-offsets "localhost:9999,localhost:9999" consumer-config "test" "clj-kafka.test.consumer", :earliest)
              ignore (str offset-reset-response)]
          offset-reset-response)
        (Thread/sleep 100)

        (let [offset-response (fetch-consumer-offsets "localhost:9999,localhost:9999" consumer-config "test" "clj-kafka.test.consumer")
              ignore (str offset-response)
              offset (.offset (get offset-response "test:0"))]
          offset)))))

(given (send-consume-reset-fetch [(test-message "msg 1") (test-message "msg 2")])
       (expect identity 0))