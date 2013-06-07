(ns clj-kafka.test.core
  (:import [java.nio ByteBuffer]
           [kafka.message Message])
  (:use [clojure.test])
  (:use [clj-kafka.core] :reload))

(set! *warn-on-reflection* true)

(deftest convert-message-payload-to-bytes
  (let [bytes (byte-array (repeat 100 (byte 10)))
        msg (Message. bytes)]
    (is (= 100 (count (:payload (to-clojure msg)))))
    (is (= (seq bytes) (seq (:payload (to-clojure msg)))))
    (is (= :none (:compression (to-clojure msg))))))
