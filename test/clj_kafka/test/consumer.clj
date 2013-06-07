(ns clj-kafka.test.consumer
  (:import [kafka.server KafkaConfig KafkaServer])
  (:use [clojure.test]
        [clojure.java.io :only (file)]
        [clj-kafka.core :only (as-properties)]))

(def system-time (proxy [kafka.utils.Time] []
                   (milliseconds [] (System/currentTimeMillis))
                   (nanoseconds [] (System/nanoTime))
                   (sleep [ms] (Thread/sleep ms))))

;; enable.zookeeper doesn't seem to be used- check it actually has an effect
(defn create-broker
  []
  (let [config (KafkaConfig. (as-properties {"broker.id" "0"
                                             "port" "9999"
                                             "host.name" "localhost"
                                             "zookeeper.connect" "127.0.0.1:2182"
                                             "enable.zookeeper" "false"
                                             "log.flush.interval.messages" "1"
                                             "log.dir" (.getAbsolutePath (clojure.java.io/file "tmp/log"))}))]
    (KafkaServer. config system-time)))

(defmacro with-broker
  "Creates an in-process broker that can be used to test against"
  [& body]
  `(let [broker# (create-broker)]
     ~@body))

(deftest testing-something
  (with-broker (is (= 1 1))))