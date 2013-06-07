(ns clj-kafka.test.utils
  (:import [kafka.server KafkaConfig KafkaServer]
           [java.net InetSocketAddress]
           [org.apache.zookeeper.server ZooKeeperServer NIOServerCnxn$Factory]
           [org.apache.commons.io FileUtils])
  (:use [clojure.java.io :only (file)]
        [clj-kafka.core :only (as-properties)]))

(defn tmp-dir
  [& parts]
  (.getPath (apply file (System/getProperty "java.io.tmpdir") "clj-kafka" (str (System/currentTimeMillis)) parts)))

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

(defn create-zookeeper
  [{:keys [port host snapshot-dir log-dir]}]
  (let [tick-time 500
        zk (ZooKeeperServer. (file snapshot-dir) (file log-dir) tick-time)]
    (doto (NIOServerCnxn$Factory. (InetSocketAddress. host port))
      (.startup zk))))

(defmacro with-broker
  "Creates an in-process broker that can be used to test against"
  [& body]
  `(let [zk# (create-zookeeper ~zk-config)
         kafka# (create-broker)]
     (.startup kafka#)
     (try ~@body
          (finally (do (.shutdown kafka#)
                       (.shutdown zk#))))))
