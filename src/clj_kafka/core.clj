(ns clj-kafka.core
  (:import [java.nio ByteBuffer]
           [java.util Properties]
           [kafka.message MessageAndMetadata MessageAndOffset]
           [java.util.concurrent LinkedBlockingQueue]
           [kafka.javaapi OffsetResponse PartitionMetadata TopicMetadata TopicMetadataResponse]
           [kafka.cluster Broker]))

(defrecord KafkaMessage [topic offset partition key value])

(defn as-properties
  [m]
  (let [props (Properties. )]
    (doseq [[n v] m] (.setProperty props n v))
    props))

(defmacro with-resource
  [binding close-fn & body]
  `(let ~binding
     (try
       (do ~@body)
       (finally
        (~close-fn ~(binding 0))))))

(defprotocol ToClojure
  (to-clojure [x] "Converts type to Clojure structure"))

(extend-protocol ToClojure
  nil
  (to-clojure [x] nil)

  MessageAndMetadata
  (to-clojure [x] (KafkaMessage. (.topic x) (.offset x) (.partition x) (.key x) (.message x)))

  MessageAndOffset
  (to-clojure [x]
    (letfn [(byte-buffer-bytes [^ByteBuffer bb] (let [b (byte-array (.remaining bb))]
                                      (.get bb b)
                                      b))]
      (let [offset (.offset x)
            msg (.message x)]
        (KafkaMessage. nil offset nil (.key msg) (byte-buffer-bytes (.payload msg))))))

  Broker
  (to-clojure [x]
    {:connect (.getConnectionString x)
     :host (.host x)
     :port (.port x)
     :broker-id (.id x)})

  PartitionMetadata
  (to-clojure [x]
    {:partition-id (.partitionId x)
     :leader (to-clojure (.leader x))
     :replicas (map to-clojure (.replicas x))
     :in-sync-replicas (map to-clojure (.isr x))
     :error-code (.errorCode x)})

  TopicMetadata
  (to-clojure [x]
    {:topic (.topic x)
     :partition-metadata (map to-clojure (.partitionsMetadata x))})

  TopicMetadataResponse
  (to-clojure [x]
    (map to-clojure (.topicsMetadata x))))
