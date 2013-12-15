(ns clj-kafka.test.zk
  (:use expectations
        clj-kafka.zk
        [clj-kafka.test.utils :only (with-test-broker)]))

(def config {:zookeeper-port 2182
             :kafka-port 9999
             :topic "test"})

(def zk-connect {"zookeeper.connect" "127.0.0.1:2182"})
(given (with-test-broker config
         (brokers zk-connect))
       (expect count 1))

(given (with-test-broker config
         (first (brokers zk-connect)))
       (expect :host "localhost"
               :jmx_port -1
               :port 9999
               :version 1))

(given (with-test-broker config
         (controller zk-connect))
       (expect identity 0))

(given (with-test-broker config
         (topics zk-connect))
       (expect count 1
               first (:topic config)))
