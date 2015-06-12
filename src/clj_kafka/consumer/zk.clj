(ns clj-kafka.consumer.zk
  (:import [kafka.consumer ConsumerConfig Consumer KafkaStream]
           [kafka.javaapi.consumer ConsumerConnector]
           [kafka.serializer DefaultDecoder])
  (:use [clj-kafka.core :only (as-properties to-clojure with-resource)])
  (:require [zookeeper :as zk]))

(defn consumer
  "Uses information in Zookeeper to connect to Kafka. More info on settings
   is available here: https://kafka.apache.org/08/configuration.html

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

(defn- lazy-iterate
  [^java.util.Iterator it]
  (lazy-seq
   (when (.hasNext it)
     (cons (.next it) (lazy-iterate it)))))

(defn default-decoder
  "Creates the default decoder that reads message keys and values as byte arrays."
  []
  (DefaultDecoder. nil))

(defn- vals->ints
  [m]
  (reduce-kv (fn [res k v] (assoc res k (int v))) {} m))

(defn create-message-streams
  "Creates multiple message streams for consuming multiple topics, or
  a single topic cusing multiple threads. topic-count-map is a map
  from a topic name to the number of streams desired for that topic."
  ([^ConsumerConnector consumer topic-count-map]
   (.createMessageStreams consumer (vals->ints topic-count-map)))
  ([^ConsumerConnector consumer topic-count-map key-decoder value-decoder]
   (.createMessageStreams consumer (vals->ints topic-count-map) key-decoder value-decoder)))

(defn create-message-stream
  "Creates a single message stream for given topic."
  ([^ConsumerConnector consumer topic]
   (let [topic-streams (.createMessageStreams consumer {topic (int 1)})]
     (first (get topic-streams topic))))
  ([^ConsumerConnector consumer topic key-decoder value-decoder]
   (let [topic-streams (.createMessageStreams consumer {topic (int 1)} key-decoder value-decoder)]
     (first (get topic-streams topic)))))

(defn stream-seq
  "Returns a lazy sequence of KafkaMessage messages from the stream."
  [^KafkaStream stream]
  (map to-clojure
       (lazy-iterate (.iterator ^KafkaStream stream))))

(defn messages
  "Provides an easy way to consume a sequence of KafkaMessage messages from the
  named topic. Consumes on a single thread and returns a lazy sequence."
  [^ConsumerConnector consumer topic & {:keys [key-decoder
                                               value-decoder]
                                        :or   {key-decoder (default-decoder)
                                               value-decoder (default-decoder)}}]
  (stream-seq (create-message-stream consumer topic key-decoder value-decoder)))
