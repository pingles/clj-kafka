(defproject clj-kafka/clj-kafka "0.1.0-0.8-SNAPSHOT"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.uswitch/kafka_2.9.2 "0.8.0-SNAPSHOT"]
                 [zookeeper-clj "0.9.3"]
                 [org.clojure/data.json "0.2.2"]]
  :exclusions [javax.mail/mail
               javax.jms/jms
               com.sun.jdmk/jmxtools
               com.sun.jmx/jmxri
               jline/jline
               junit/junit
               log4j/log4j]
  :profiles {:dev {:dependencies [[org.slf4j/slf4j-simple "1.6.1"]
                                  [org.slf4j/log4j-over-slf4j "1.6.1"]
                                  [commons-io/commons-io "2.4"]]}}
  :description "Clojure wrapper for Kafka's Java API")
