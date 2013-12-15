(ns clj-kafka.zk
  (:use [clojure.data.json :only (read-str)]
        [clj-kafka.core :only (with-resource)])
  (:require [zookeeper :as zk]))

(defn brokers
  "Get brokers from zookeeper"
  [m]
  (with-resource [z (zk/connect (get m "zookeeper.connect"))]
    zk/close
    (doall (map (comp #(read-str % :key-fn keyword)
                      #(String. ^bytes %)
                      :data
                      #(zk/data z (str "/brokers/ids/" %)))
                (zk/children z "/brokers/ids")))))
                
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
