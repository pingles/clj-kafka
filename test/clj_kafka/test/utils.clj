(ns clj-kafka.test.utils
  (:import [kafka.server KafkaConfig KafkaServer]
           [kafka.admin AdminUtils]
           [kafka.common TopicAndPartition]
           [java.net InetSocketAddress]
           [org.apache.zookeeper.server ZooKeeperServer NIOServerCnxn$Factory]
           [org.apache.commons.io FileUtils]
           [org.I0Itec.zkclient ZkClient]
           [org.I0Itec.zkclient.serialize ZkSerializer]
           [java.util Properties])
  (:use [clojure.java.io :only (file)]
        [clj-kafka.core :only (as-properties)]))

(defn tmp-dir
  [& parts]
  (.getPath (apply file (System/getProperty "java.io.tmpdir") "clj-kafka" parts)))

(def system-time (proxy [kafka.utils.Time] []
                   (milliseconds [] (System/currentTimeMillis))
                   (nanoseconds [] (System/nanoTime))
                   (sleep [ms] (Thread/sleep ms))))

;; enable.zookeeper doesn't seem to be used- check it actually has an effect
(defn create-broker
  [{:keys [kafka-port zookeeper-port]}]
  (let [base-config {"broker.id" "0"
                     "port" "9999"
                     "host.name" "localhost"
                     "zookeeper.connect" (str "127.0.0.1:" zookeeper-port)
                     "enable.zookeeper" "true"
                     "log.flush.interval.messages" "1"
                     "auto.create.topics.enable" "true"
                     "log.dir" (.getAbsolutePath (file (tmp-dir "kafka-log")))}]
    (KafkaServer. (KafkaConfig. (as-properties (assoc base-config "port" (str kafka-port))))
                  system-time)))

(defn create-zookeeper
  [{:keys [zookeeper-port]}]
  (let [tick-time 500
        zk (ZooKeeperServer. (file (tmp-dir "zookeeper-snapshot")) (file (tmp-dir "zookeeper-log")) tick-time)]
    (doto (NIOServerCnxn$Factory. (InetSocketAddress. "127.0.0.1" zookeeper-port))
      (.startup zk))))

(defn wait-until-initialised
  [kafka-server topic]
  (let [topic-and-partition (TopicAndPartition. topic 0)]
    (while (not (.. kafka-server apis metadataCache keySet (contains topic-and-partition)))
      (Thread/sleep 50))))

(defn create-topic
  [zk-client topic & {:keys [partitions replicas]
                      :or   {partitions 1 replicas 1}}]
  (AdminUtils/createTopic zk-client topic partitions replicas (Properties.)))

(def string-serializer (proxy [ZkSerializer] []
                         (serialize [data] (.getBytes data "UTF-8"))
                         (deserialize [bytes] (when bytes
                                                (String. bytes "UTF-8")))))

(defmacro with-test-broker
  "Creates an in-process broker that can be used to test against"
  [config & body]
  `(do (FileUtils/deleteDirectory (file (tmp-dir)))
       (let [zk# (create-zookeeper ~config)
             kafka# (create-broker ~config)
             topic# (:topic ~config)]
         (try
           (.startup kafka#)
           (let [zk-client# (ZkClient. (str "127.0.0.1:" (:zookeeper-port ~config)) 500 500 string-serializer)]
             (create-topic zk-client# topic#)
             (wait-until-initialised kafka# topic#)
             ~@body)
           (finally (do (.shutdown kafka#)
                        (.awaitShutdown kafka#)
                        (.shutdown zk#)
                        (FileUtils/deleteDirectory (file (tmp-dir)))))))))
