(ns clj-kafka.test.producer
  (:use [expectations]
        [clj-kafka.core]
        [clj-kafka.producer]
        [clj-kafka.test.utils :only (with-test-broker)])
  (:import [kafka.message Message]
           [kafka.producer KeyedMessage]))

(expect KeyedMessage (message "topic" "value"))

(given (with-test-broker {:zookeeper-port 2182
                          :kafka-port 9999
                          :topic "test"}
         (brokers {"zookeeper.connect" "127.0.0.1:2182"}))
       (expect count 1
               first {:host "localhost", :jmx_port -1, :port 9999, :version 1}))
