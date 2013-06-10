(ns clj-kafka.test.zk
  (:use expectations
        clj-kafka.zk
        [clj-kafka.test.utils :only (with-test-broker)]))

(def config {:zookeeper-port 2182
             :kafka-port 9999
             :topic "test"})

(given (with-test-broker config
         (brokers {"zookeeper.connect" "127.0.0.1:2182"}))
       (expect count 1
               first {:host "localhost", :jmx_port -1, :port 9999, :version 1}))

(given (with-test-broker config
         (controller {"zookeeper.connect" "127.0.0.1:2182"}))
       (expect identity 0))
