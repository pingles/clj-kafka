(ns clj-kafka.consumer.simple
  (:use [clj-kafka.core :only (to-clojure)])
  (:import [kafka.javaapi.consumer SimpleConsumer]
           [kafka.api FetchRequestBuilder PartitionOffsetRequestInfo]
           [kafka.javaapi OffsetRequest]
           [kafka.javaapi TopicMetadataRequest]
           [kafka.common TopicAndPartition]))

(defn consumer
  "Create a consumer to connect to host and port. Port will
   normally be 9092."
  [host ^Long port client-id & {:keys [^Long timeout ^Long buffer-size] :or {timeout 100000 buffer-size 10000}}]
  (SimpleConsumer. host
                   (Integer/valueOf port)
                   (Integer/valueOf timeout)
                   (Integer/valueOf buffer-size)
                   client-id))

(defn fetch-request
  [client-id topic ^Long partition offset fetch-size]
  (.build (doto (FetchRequestBuilder. )
            (.clientId client-id)
            (.addFetch topic (Integer/valueOf partition) offset fetch-size))))

(defn messages
  [^SimpleConsumer consumer client-id topic partition offset fetch-size]
  (let [fetch (fetch-request client-id topic partition offset fetch-size)]
    (iterator-seq (.iterator (.messageSet ^kafka.javaapi.FetchResponse (.fetch consumer ^kafka.api.FetchRequest fetch)
                                          topic
                                          partition)))))

(defn topic-meta-data [consumer topics]
  (to-clojure (.send consumer (TopicMetadataRequest. topics))))

(defn latest-topic-offset [consumer topic partition]
  (let [tp   (TopicAndPartition. topic partition)
        pori (PartitionOffsetRequestInfo. -1 1)
        hm    (java.util.HashMap. {tp pori})]
    (let [response  (.getOffsetsBefore consumer (OffsetRequest. hm (kafka.api.OffsetRequest/CurrentVersion) "clj-kafka-id"))]
      (first (.offsets response topic partition)))))
