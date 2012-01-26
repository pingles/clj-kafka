(defproject clj-kafka "0.0.2-0.7-SNAPSHOT"
  :description "Clojure wrapper for Kafka's Java API"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojars.paul/core-kafka_2.8.0 "0.7.0-1" :exclusions [javax.mail/mail
                                                                           javax.jms/jms
                                                                           com.sun.jdmk/jmxtools
                                                                           com.sun.jmx/jmxri
                                                                           jline/jline
                                                                           net.sf.jopt-simple/jopt-simple
                                                                           junit/junit]]
                 [org.scala-lang/scala-library "2.8.0"]])
