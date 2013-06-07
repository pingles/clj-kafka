(ns clj-kafka.test.producer
  (:use [clojure.test]
        [clj-kafka.core]
        [clj-kafka.producer] :reload
        [clj-kafka.test.utils :only (with-broker)])
  (:import [kafka.message Message]
           [kafka.producer KeyedMessage]))

(deftest keyed-messages
  (is (instance? KeyedMessage
                 (keyed-message "topic" "value"))))

(deftest brokers-test
  (with-broker
    (is (= [{:host "localhost", :jmx_port -1, :port 9999, :version 1}]
           (brokers {"zookeeper.connect" "localhost:2182"})))))
