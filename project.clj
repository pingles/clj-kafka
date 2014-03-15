(defproject clj-kafka/clj-kafka "0.2.0-0.8.1"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [zookeeper-clj "0.9.3"]
                 [org.clojure/data.json "0.2.2"]

                 ;; kafka and its related deps
                 [org.apache.kafka/kafka_2.10 "0.8.1"]
                 [org.apache.zookeeper/zookeeper "3.3.4"]
                 [com.101tec/zkclient "0.3"]
                 [com.yammer.metrics/metrics-core "2.2.0"]
                 [org.scala-lang/scala-library "2.10.1"]
                 [net.sf.jopt-simple/jopt-simple "3.2"]]
  :exclusions [javax.mail/mail
               javax.jms/jms
               com.sun.jdmk/jmxtools
               com.sun.jmx/jmxri
               jline/jline]
  :plugins [[lein-expectations "0.0.8"]]
  :profiles {:dev {:dependencies [[commons-io/commons-io "2.4"]
                                  [expectations "1.4.56"]
                                  [org.slf4j/slf4j-simple "1.6.4"]]}}
  :description "Clojure wrapper for Kafka's Java API")
