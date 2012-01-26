(ns clj-kafka.consumer.zk
  (:import [kafka.consumer ConsumerConfig Consumer])
  (:use [clj-kafka.core :only (as-properties to-clojure)]))

(defmacro with-resource
  [binding close-fn & body]
  `(let ~binding
     (try
       (do ~@body)
       (finally
        (~close-fn ~(binding 0))))))

(defn consumer
  "Uses information in Zookeeper to connect to Kafka. More info on settings
   is available here: http://incubator.apache.org/kafka/configuration.html

   Recommended for using with with-resource:
   (with-resource [c (consumer m)]
     shutdown
     (take 5 (messages c \"test\")))
  
   Keys:
   zk.connect             : host:port for Zookeeper. e.g: 127.0.0.1:2181
   groupid                : consumer group. e.g. group1
   zk.sessiontimeout.ms   : session timeout. e.g. 400
   zk.synctime.ms         : Max time for how far a ZK follower can be behind a ZK leader. 200 ms
   autocommit.interval.ms : the frequency that the consumed offsets are committed to zookeeper.
   autocommit.enable      : if set to true, the consumer periodically commits to zookeeper the latest consumed offset of each partition"
  [m]
  (let [config (ConsumerConfig. (as-properties m))]
    (Consumer/createJavaConsumerConnector config)))

(defn shutdown
  "Closes the connection to Zookeeper and stops consuming messages."
  [consumer]
  (.shutdown consumer))

(defn messages
  "Creates a sequence of messages"
  [consumer topic]
  (let [topic-map {topic (Integer/valueOf 1)}
        streams (.createMessageStreams consumer topic-map)
        stream (first (.get streams topic))]
    (map to-clojure (iterator-seq (.iterator stream)))))