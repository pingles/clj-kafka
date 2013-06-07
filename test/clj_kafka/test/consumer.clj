(ns clj-kafka.test.consumer
  (:import [kafka.server KafkaConfig])
  (:use [clojure.test]
        [clj-kafka.core :only (as-properties)]))

(defn create-broker
  []
  (let [config (KafkaConfig. (as-properties {"broker.id" "0"
                                             "port" "9999"
                                             "host.name" "localhost"
                                             "zookeeper.connect" "127.0.0.1:2182"}))]))

(defmacro with-broker
  "Creates an in-process broker that can be used to test against"
  [& body]
  `(let [broker# (create-broker)]
     ~@body))

(deftest testing-something
  (with-broker (is (= 1 1))))