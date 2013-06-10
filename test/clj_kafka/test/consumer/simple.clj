(ns clj-kafka.test.consumer.simple
  (:use clj-kafka.consumer.simple
        expectations
        [clj-kafka.test.utils :only (with-test-broker)]))

(def test-broker-config {:zookeeper-port 2182
                         :kafka-port 9999
                         :topic "test"})

(given (with-test-broker test-broker-config
         (let [c (consumer "127.0.0.1" 9999 "simple-consumer")]
           (topic-meta-data c ["test"])))
       (expect count 1
               first {:topic "test",
                      :partition-metadata [{:partition-id 0,
                                            :leader {:connect "localhost:9999",
                                                     :host "localhost",
                                                     :port 9999
                                                     :broker-id 0},
                                            :replicas [{:connect "localhost:9999",
                                                        :host "localhost",
                                                        :port 9999
                                                        :broker-id 0}],
                                            :in-sync-replicas [{:connect "localhost:9999",
                                                                :host "localhost",
                                                                :port 9999
                                                                :broker-id 0}],
                                            :error-code 0}]}))

(given (with-test-broker test-broker-config
         (let [c (consumer "127.0.0.1" 9999 "simple-consumer")]
           (latest-topic-offset c "test" 0)))
       (expect identity 0))
