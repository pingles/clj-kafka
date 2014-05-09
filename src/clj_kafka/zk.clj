(ns clj-kafka.zk
  (:use [clojure.data.json :only (read-str)]
        [clj-kafka.core :only (with-resource)])
  (:require [zookeeper :as zk]
            [clojure.string :as s])
  (:import [org.apache.zookeeper KeeperException$NoNodeException]))

(defn brokers
  "Get brokers from zookeeper"
  [m]
  (with-resource [z (zk/connect (get m "zookeeper.connect"))]
    zk/close
    (if-let [broker-ids (zk/children z "/brokers/ids")]
      (doall (map (comp #(read-str % :key-fn keyword)
                        #(String. ^bytes %)
                        :data
                        #(zk/data z (str "/brokers/ids/" %)))
                  broker-ids))
      '())))

(defn broker-list
  "Returns a comma separated list of Kafka brokers, as returned from clj-kafka.zk/brokers.
   e.g.: (broker-list (brokers {\"zookeeper.connect\" \"127.0.0.1:2181\"})) "
  [brokers]
  (when (seq brokers)
    (s/join "," (map (fn [{:keys [host port]}] (str host ":" port)) brokers))))

(defn- controller-broker-id
  [^String zk-data]
  (get (read-str zk-data) "brokerid"))

(defn controller
  "Get leader node"
  [m]
  (with-resource [z (zk/connect (get m "zookeeper.connect"))]
    zk/close
    (-> (zk/data z "/controller")
        :data
        String.
        controller-broker-id)))

(defn topics
  "Get topics"
  [m]
  (with-resource [z (zk/connect (get m "zookeeper.connect"))]
    zk/close
    (zk/children z "/brokers/topics")))

(defn partitions
  "Returns a map of partitions and replica ids. e.g. {\"1\" [0], \"0\" [0]}"
  [m topic]
  (with-resource [z (zk/connect (get m "zookeeper.connect"))]
    zk/close
    (-> (zk/data z (str "/brokers/topics/" topic))
        :data
        String.
        read-str
        (get "partitions"))))

(defn offset-path
  [consumer-group topic partition]
  (str "/consumers/" consumer-group "/offsets/" topic "/" partition))

(defn committed-offset
  [m consumer-group topic partition]
  (with-resource [z (zk/connect (get m "zookeeper.connect"))]
    zk/close
    (try (-> (zk/data z (offset-path consumer-group topic partition))
             :data
             (String. )
             (Long/valueOf))
         (catch KeeperException$NoNodeException e
           nil))))

(defn set-offset!
  [m consumer-group topic partition offset]
  {:pre [(or (nil? offset) (number? offset))]}
  (let [path (offset-path consumer-group topic partition)]
    (with-resource [z (zk/connect (get m "zookeeper.connect"))]
      zk/close
      (if (nil? offset)
        (zk/delete z path)
        (if-let [{:keys [version] :as m} (zk/exists z path)]
          (zk/set-data z path (.getBytes (str offset)) version)
          (zk/create z path :persistent? true :data (.getBytes (str offset))))))))
