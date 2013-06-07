(ns clj-kafka.test.core
  (:import [java.nio ByteBuffer]
           [kafka.message Message])
  (:use [clojure.test])
  (:use [clj-kafka.core] :reload))

