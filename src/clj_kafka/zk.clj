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

(defn controller
  "Get leader node"
  [m]
  (with-resource [z (zk/connect (get m "zookeeper.connect"))]
    zk/close
    (-> (zk/data z "/controller")
        :data
        String.
        Integer/valueOf)))
