(ns clj-kafka.consumer.simple
  (:use [clj-kafka.core :only (to-clojure)])
  (:import [kafka.javaapi.consumer SimpleConsumer]
           [kafka.api FetchRequestBuilder]))

(defn consumer
  "Create a consumer to connect to host and port. Port will
   normally be 9092."
  [host port client-id & {:keys [timeout buffer-size] :or {timeout 100000 buffer-size 10000}}]
  (SimpleConsumer. host
                   (Integer/valueOf port)
                   (Integer/valueOf timeout)
                   (Integer/valueOf buffer-size)
                   client-id))

(defn fetch-request
  [client-id topic partition offset fetch-size]
  (.build (doto (FetchRequestBuilder. )
            (.clientId client-id)
            (.addFetch topic (Integer/valueOf partition) offset fetch-size))))

(defn messages
  [consumer client-id topic partition offset fetch-size]
  (let [fetch (fetch-request client-id topic partition offset fetch-size)]
    (iterator-seq (.iterator (.messageSet (.fetch consumer fetch)
                                          topic
                                          partition)))))
