(ns clj-kafka.consumer.zk
  (:import [kafka.consumer ConsumerConfig Consumer KafkaStream]
           [kafka.javaapi.consumer ConsumerConnector])
  (:use [clj-kafka.core :only (as-properties to-clojure with-resource pipe)])
  (:require [zookeeper :as zk]))

(defn consumer
  "Uses information in Zookeeper to connect to Kafka. More info on settings
   is available here: http://incubator.apache.org/kafka/configuration.html

   Recommended for using with with-resource:
   (with-resource [c (consumer m)]
     shutdown
     (take 5 (messages c \"test\")))

   Keys:
   zookeeper.connect             : host:port for Zookeeper. e.g: 127.0.0.1:2181
   group.id                      : consumer group. e.g. group1
   auto.offset.reset             : what to do if an offset is out of range, e.g. smallest, largest
   auto.commit.interval.ms       : the frequency that the consumed offsets are committed to zookeeper.
   auto.commit.enable            : if set to true, the consumer periodically commits to zookeeper the latest consumed offset of each partition"
  [m]
  (let [config (ConsumerConfig. (as-properties m))]
    (Consumer/createJavaConsumerConnector config)))

(defn shutdown
  "Closes the connection to Zookeeper and stops consuming messages."
  [^ConsumerConnector consumer]
  (.shutdown consumer))

(defn- topic-map
  [topics]
  (apply hash-map (interleave topics
                              (repeat (Integer/valueOf 1)))))

(defn messages
  "Creates a sequence of KafkaMessage messages from the given topics. Consumes
   messages from a single stream."
  [^ConsumerConnector consumer & topics]
  (let [[queue-seq queue-put] (pipe)]
    (doseq [[topic streams] (.createMessageStreams consumer (topic-map topics))]
      (future (doseq [msg (iterator-seq (.iterator ^KafkaStream (first streams)))]
                (queue-put (to-clojure msg)))))
    queue-seq))

