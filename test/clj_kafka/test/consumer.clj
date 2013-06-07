(ns clj-kafka.test.consumer
  (:import [kafka.server KafkaConfig KafkaServer]
           [java.net InetSocketAddress]
           [org.apache.zookeeper.server ZooKeeperServer NIOServerCnxn$Factory]
           [org.apache.commons.io FileUtils])
  (:use [clojure.test]
        [clojure.java.io :only (file)]
        [clj-kafka.core :only (as-properties)]))

(defn tmp-dir
  [path]
  (.getPath (file (System/getProperty "java.io.tmpdir") path)))

(def zk-config {:host "127.0.0.1"
                :port 2182
                :snapshot-dir (tmp-dir "zookeeper-snapshot")
                :log-dir (tmp-dir "zookeeper-log")})

(def broker-config (let [{:keys [host port]} zk-config]
                     {"broker.id" "0"
                      "port" "9999"
                      "host.name" "localhost"
                      "zookeeper.connect" (str host ":" port)
                      "enable.zookeeper" "false"
                      "log.flush.interval.messages" "1"
                      "auto.create.topics.enabled" "true"
                      "log.dir" (.getAbsolutePath (file (tmp-dir "kafka-log")))}))

(def system-time (proxy [kafka.utils.Time] []
                   (milliseconds [] (System/currentTimeMillis))
                   (nanoseconds [] (System/nanoTime))
                   (sleep [ms] (Thread/sleep ms))))

;; enable.zookeeper doesn't seem to be used- check it actually has an effect
(defn create-broker
  []
  (let [config (KafkaConfig. (as-properties broker-config))]
    (KafkaServer. config system-time)))

(defn- start
  [broker]
  (.startup broker))

(defn- stop
  [broker]
  (.shutdown broker))

(defn create-zookeeper
  [{:keys [port host snapshot-dir log-dir]}]
  (let [tick-time 500
        zk (ZooKeeperServer. (file snapshot-dir) (file log-dir) tick-time)]
    (doto (NIOServerCnxn$Factory. (InetSocketAddress. host port))
      (.startup zk))))

(defn shutdown-zookeeper
  [zookeeper]
  (.shutdown zookeeper))

(defn- clean-broker-data
  []
  (FileUtils/deleteDirectory (file (broker-config "log.dir"))))

(defn- clean-zk
  []
  (let [{:keys [snapshot-dir log-dir]} zk-config]
    (FileUtils/deleteDirectory (file snapshot-dir))
    (FileUtils/deleteDirectory (file log-dir))))

(defn- cleanup
  []
  (clean-broker-data)
  (clean-zk))

(defmacro with-broker
  "Creates an in-process broker that can be used to test against"
  [& body]
  `(let [broker# (create-broker)
         zookeeper# (create-zookeeper zk-config)]
     (try (do (start broker#) 
              ~@body)
          (catch Exception e#
            (throw e#))
          (finally (do (stop broker#)
                       (shutdown-zookeeper zookeeper#)
                       (cleanup))))))

(deftest testing-something
  (with-broker (is (= 1 1))))