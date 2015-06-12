(ns clj-kafka.new.producer
  (:refer-clojure :exclude [send])
  (:import [org.apache.kafka.clients.producer Callback KafkaProducer ProducerRecord]
           [org.apache.kafka.common.serialization Serializer ByteArraySerializer StringSerializer]))

(defn string-serializer [] (StringSerializer.))
(defn byte-array-serializer [] (ByteArraySerializer.))

(defn producer
  ([^java.util.Map config]
   (KafkaProducer. config))
  ([^java.util.Map config ^Serializer key-serializer ^Serializer value-serializer]
   (KafkaProducer. config key-serializer value-serializer)))

(defn record
  ([topic value]
   (ProducerRecord. topic value))
  ([topic key value]
   (ProducerRecord. topic key value))
  ([topic partition key value]
   (ProducerRecord. topic partition key value)))

(defn send
  ([^KafkaProducer producer record]
   (.send producer record))
  ([^KafkaProducer producer record callback]
   (.send producer record (reify Callback
                            (onCompletion [_ metadata exception]
                              (callback metadata exception))))))
