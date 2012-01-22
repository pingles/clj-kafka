(ns clj-kafka.core
  (:import [kafka.javaapi.consumer SimpleConsumer]
           [kafka.api FetchRequest]
           [kafka.message MessageAndOffset Message]))

(defprotocol ToClojure
  (to-clojure [_] "Converts type to Clojure structure"))

(extend-protocol ToClojure
  MessageAndOffset
  (to-clojure [x] {:message (to-clojure (.message x))
                   :offset (.offset x)})
  Message
  (to-clojure [x] {:crc (.checksum x)
                   :payload (.array (.payload x))
                   :size (.size x)}))

(defn create-consumer
  "Create a consumer to connect to host and port. Port will
   normally be 9092."
  [host port & {:keys [timeout buffer-size] :or {timeout 100000 buffer-size 10000}}]
  (SimpleConsumer. host port timeout buffer-size))

(defn fetch
  "Creates a request to retrieve a set of messages from the
   specified topic.

   Arguments:
   partition: as specified when producing messages
   offset: offset to start retrieval
   max-size: number of bytes to retrieve"
  [^String topic ^Integer partition ^Long offset ^Integer max-size]
  (FetchRequest. topic partition offset max-size))

(defn messages
  "Creates a sequence of messages from the given request."
  [consumer request]
  (map to-clojure (iterator-seq (.iterator (.fetch consumer request)))))