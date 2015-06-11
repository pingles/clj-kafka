(ns ^{:doc "Administration operations."}
  clj-kafka.admin
  (:require [clj-kafka.core :refer [as-properties]])
  (:import [kafka.admin AdminUtils]
           [kafka.utils ZKStringSerializer$]
           [org.I0Itec.zkclient ZkClient]))

(defn- zk-string-serializer [] (ZKStringSerializer$/MODULE$))

(defn zk-client
  "Create a `ZkClient` for use with the functions in this namespace.
  Servers is a Zookeeper connection string."
  ([servers]
   (zk-client servers nil))
  ([servers {:keys [session-timeout-ms connection-timeout-ms] :as opts
             :or {session-timeout-ms 10000
                  connection-timeout-ms 10000}}]
   (ZkClient. servers session-timeout-ms connection-timeout-ms (zk-string-serializer))))

(defn topic-exists?
  [zk topic]
  (AdminUtils/topicExists zk topic))

(defn create-topic
  ([zk topic]
   (create-topic zk topic nil))
  ([zk topic {:keys [partitions replication-factor config]
              :or {partitions 1
                   replication-factor 1
                   config nil}}]
   (AdminUtils/createTopic zk topic (int partitions) (int replication-factor) (as-properties config))))

(defn delete-topic
  [zk topic]
  (AdminUtils/deleteTopic zk topic))

(defn topic-config
  [zk topic]
  (AdminUtils/fetchTopicConfig zk topic))

(defn change-topic-config
  [zk topic config]
  (AdminUtils/changeTopicConfig zk topic (as-properties config)))
