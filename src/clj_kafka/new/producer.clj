(ns ^{:doc "Clojure interface to the \"new\" Kafka Producer API. For
  complete JavaDocs, see:
  http://kafka.apache.org/082/javadoc/org/apache/kafka/clients/producer/package-summary.html"}
  clj-kafka.new.producer
  (:refer-clojure :exclude [send])
  (:import [org.apache.kafka.clients.producer Callback KafkaProducer ProducerRecord RecordMetadata]
           [org.apache.kafka.common.serialization Serializer ByteArraySerializer StringSerializer]))

(defn string-serializer [] (StringSerializer.))
(defn byte-array-serializer [] (ByteArraySerializer.))

(defn producer
  "Return a `KafkaProducer` for publishing records to Kafka.
  `KafkaProducer` instances are thread-safe and should generally be
  shared for best performance.

  Implements `Closeable`, so suitable for use with `with-open`.

  For available config options, see:
  http://kafka.apache.org/documentation.html#newproducerconfigs "
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

(defn- record-metadata->map
  [^RecordMetadata m]
  {:topic (.topic m)
   :partition (.partition m)
   :offfset (.offset m)})

(defn send
  "Asynchronously send a record to Kafka. Returns a map with `:topic`,
  `:partition` and `:offset` keys, wrapped in a delay. Derefing delay
  will block until the underlying future completes. Optionally provide
  a callback fn that will be called when the operation completes.
  Callback should be a fn of two arguments, a map as above, and an
  exception. Exception will be nil if operation succeeded.

  For details on behaviour, see http://kafka.apache.org/082/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#send(org.apache.kafka.clients.producer.ProducerRecord,org.apache.kafka.clients.producer.Callback)"
  ([^KafkaProducer producer record]
   (let [fut (.send producer record)]
     (delay (record-metadata->map @fut))))
  ([^KafkaProducer producer record callback]
   (let [fut (.send producer record (reify Callback
                                      (onCompletion [_ metadata exception]
                                        (callback (and metadata (record-metadata->map metadata)) exception))))]
     (delay (record-metadata->map @fut)))))
