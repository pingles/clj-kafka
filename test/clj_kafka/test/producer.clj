(ns clj-kafka.test.producer
  (:use [clojure.test]
        [clj-kafka.core]
        [clj-kafka.producer] :reload)
  (:import [kafka.message Message]
           [kafka.producer KeyedMessage]))

(deftest keyed-messages
  (is (instance? KeyedMessage
                 (keyed-message "topic" "value"))))