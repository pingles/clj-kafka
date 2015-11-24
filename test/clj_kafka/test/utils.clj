(ns clj-kafka.test.utils
  (:import
    [kafka.admin AdminUtils]
    [kafka.server KafkaConfig KafkaServer]
    [java.net InetSocketAddress]
    [org.apache.zookeeper.server ZooKeeperServer NIOServerCnxnFactory]
    [org.apache.commons.io FileUtils]
    [org.I0Itec.zkclient ZkClient]
    [org.I0Itec.zkclient.serialize ZkSerializer]
    [kafka.utils Time]
    [java.util Properties])
  (:require [clj-kafka.admin :as admin])
  (:use [clojure.java.io :only (file)]
        [clj-kafka.core :only (as-properties)]))

(defn tmp-dir
  [& parts]
  (.getPath (apply file (System/getProperty "java.io.tmpdir") "clj-kafka" parts)))

(def system-time (proxy [Time] []
                   (milliseconds [] (System/currentTimeMillis))
                   (nanoseconds [] (System/nanoTime))
                   (sleep [ms] (Thread/sleep ms))))

;; enable.zookeeper doesn't seem to be used- check it actually has an effect
(defn create-broker
  [{:keys [kafka-port zookeeper-port]}]
  (let [base-config {"broker.id"                   "0"
                     "port"                        "9999"
                     "host.name"                   "localhost"
                     "zookeeper.connect"           (str "127.0.0.1:" zookeeper-port)
                     ;"enable.zookeeper"            "true"
                     "log.flush.interval.messages" "1"
                     "auto.create.topics.enable"   "true"
                     "log.dir"                     (.getAbsolutePath (file (tmp-dir "kafka-log")))}]
    (KafkaServer. (KafkaConfig. (as-properties (assoc base-config "port" (str kafka-port))))
                  system-time)))

(defn create-zookeeper
  [{:keys [zookeeper-port]}]
  (let [tick-time 500
        zk (ZooKeeperServer. (file (tmp-dir "zookeeper-snapshot")) (file (tmp-dir "zookeeper-log")) tick-time)]
    (doto (NIOServerCnxnFactory.)
      (.configure (InetSocketAddress. "127.0.0.1" zookeeper-port) 100)
      (.startup zk))))

(defn wait-until-initialised
  [^KafkaServer kafka-server topic]
  (let [cache (.. kafka-server apis metadataCache)
        topics (scala.collection.JavaConversions/asScalaSet #{topic})]
    (while (< (.. cache (getTopicMetadata  topics) size) 1)
      (Thread/sleep 500))))


(defmacro with-test-broker
  "Creates an in-process broker that can be used to test against"
  [config & body]
  `(do (FileUtils/deleteDirectory (file (tmp-dir)))
       (let [zk# (create-zookeeper ~config)
             kafka# (create-broker ~config)
             topic# (:topic ~config)]
         (try
           (.startup kafka#)
           (let [zk-client# (admin/zk-client (str "127.0.0.1:" (:zookeeper-port ~config))
                                             {:session-timeout-ms 500
                                              :connection-timeout-ms 500})]
             (admin/create-topic zk-client# topic#)
             (wait-until-initialised kafka# topic#)
             ~@body)
           (finally (do (.shutdown kafka#)
                        (.awaitShutdown kafka#)
                        (.shutdown zk#)
                        (FileUtils/deleteDirectory (file (tmp-dir)))))))))
