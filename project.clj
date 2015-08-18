(defproject clj-kafka/clj-kafka "0.3.3"
  :min-lein-version "2.0.0"
  :url "https://github.com/pingles/clj-kafka"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/data.json "0.2.2"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.apache.kafka/kafka_2.10 "0.8.2.1"]
                 [org.apache.kafka/kafka-clients "0.8.2.1"]
                 [zookeeper-clj "0.9.3"]
                 [com.101tec/zkclient "0.5"]
                 [org.scala-lang/scala-library "2.10.5"]
                 [com.yammer.metrics/metrics-core "2.2.0"]]
  :exclusions [javax.mail/mail
               javax.jms/jms
               com.sun.jdmk/jmxtools
               com.sun.jmx/jmxri
               jline/jline]
  :plugins [[lein-expectations "0.0.8"]
            [codox "0.8.12"]]
  :codox {:src-dir-uri "http://github.com/pingles/clj-kafka/blob/master/"
          :src-linenum-anchor-prefix "L"          
          :defaults {:doc/format :markdown}}
  :profiles {:dev {:resource-paths ["dev-resources"]
                   :dependencies [[commons-io/commons-io "2.4"]
                                  [expectations "1.4.45"]]}}
  :description "Clojure wrapper for Kafka's Java API")
