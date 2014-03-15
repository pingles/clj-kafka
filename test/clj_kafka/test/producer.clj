(ns clj-kafka.test.producer
  (:use [expectations :exclude [message]]
        [clj-kafka.producer])
  (:import  [kafka.producer KeyedMessage]))

(expect KeyedMessage (message "topic" "value"))
