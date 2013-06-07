(ns clj-kafka.test.consumer
  (:use [clojure.test]
        [clj-kafka.test.utils :only (with-broker)]))

(deftest testing-something
  (with-broker
    (is (= 1 1))))