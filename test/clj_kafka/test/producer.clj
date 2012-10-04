(ns clj-kafka.test.producer
  (:use [clojure.test]
        [clj-kafka.core]
        [clj-kafka.producer] :reload)
  (:import [kafka.message Message]))

(deftest creates-message-with-string-bytes
  (is (instance? Message
                 (to-message "Hello, world")))
  (is (= "Hello, world"
         (String. (:payload (to-clojure (to-message "Hello, world")))))))