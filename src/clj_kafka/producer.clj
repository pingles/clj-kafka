(ns ^{:doc "Clojure interface for Kafka Producer API. For
  complete JavaDocs, see:
  http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/producer/package-summary.html"}
  clj-kafka.producer
  (:refer-clojure :exclude [send flush])
  (:import [java.util.concurrent Future TimeUnit TimeoutException]
           [org.apache.kafka.clients.producer Callback KafkaProducer ProducerRecord RecordMetadata]
           [org.apache.kafka.common Metric MetricName]
           [org.apache.kafka.common.serialization Serializer ByteArraySerializer StringSerializer]))

#_(set! *warn-on-reflection* true)

(defn- RecordMetadata->map
  [^RecordMetadata m]
  {:topic (.topic m)
   :partition (.partition m)
   :offset (.offset m)})

(defn- map-future-val
  [^Future fut f]
  (reify
    java.util.concurrent.Future
    (cancel [_ interrupt?] (.cancel fut interrupt?))
    (get [_] (f (.get fut)))
    (get [_ timeout unit] (f (.get fut timeout unit)))
    (isCancelled [_] (.isCancelled fut))
    (isDone [_] (.isDone fut))))


;;--------------------------------------------------------------------
;; API

(defn string-serializer [] (StringSerializer.))
(defn byte-array-serializer [] (ByteArraySerializer.))

(defn producer
  "Return a `KafkaProducer` for publishing records to Kafka.
  `KafkaProducer` instances are thread-safe and should generally be
  shared for best performance.

  For details on usage and available config options, see: http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/producer/ProducerConfig.html"
  ([^java.util.Map config]
   (KafkaProducer. config))
  ([^java.util.Map config ^Serializer key-serializer ^Serializer value-serializer]
   (KafkaProducer. config key-serializer value-serializer)))

(defn record
  "Return a record that can be published to Kafka using [[send]]."
  ([topic value]
   (ProducerRecord. topic value))
  ([topic key value]
   (ProducerRecord. topic key value))
  ([topic partition key value]
   (ProducerRecord. topic partition key value)))

(defn send
  "Asynchronously send a record to Kafka. Returns a `Future` of a map
  with `:topic`, `:partition` and `:offset` keys. Optionally provide
  a callback fn that will be called when the operation completes.
  Callback should be a fn of two arguments, a map as above, and an
  exception. Exception will be nil if operation succeeded.

  See: http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#send(org.apache.kafka.clients.producer.ProducerRecord)"
  ([^KafkaProducer producer record]
   (let [fut (.send producer record)]
     (map-future-val fut RecordMetadata->map)))
  ([^KafkaProducer producer record callback]
   (let [fut (.send producer record (reify Callback
                                      (onCompletion [_ metadata exception]
                                        (callback (and metadata (RecordMetadata->map metadata)) exception))))]
     (map-future-val fut RecordMetadata->map))))

(defn flush
  "See: http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#flush()"
  [^KafkaProducer producer]
  (.flush producer))

(defn close
  "Like `.close`, but with a default time unit of ms for the arity with timeout.

  See: 

  - http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#close()
  - http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#close(long,%20java.util.concurrent.TimeUnit)"
  ([^KafkaProducer producer]
   (.close producer))
  ([^KafkaProducer producer timeout-ms]
   (.close producer timeout-ms TimeUnit/MILLISECONDS)))

(defn partitions
  [^KafkaProducer producer topic]
  (.partitionsFor producer topic))

;; TODO: Figure out best structure for this data
(defn metrics
  [^KafkaProducer producer]
  (persistent! (reduce (fn [ret [^MetricName metric-name ^Metric metric]]
                         (conj! ret
                                {:group (.group metric-name)
                                 :name (.name metric-name)
                                 :tags (.tags metric-name)
                                 :description (.description metric-name)
                                 :value (.value metric)}))
                          (transient [])
                          (.metrics producer))))



