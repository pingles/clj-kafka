(ns clj-kafka.test.core
  (:import [java.nio ByteBuffer]
           [kafka.message Message])
  (:use [clojure.test])
  (:use [clj-kafka.core] :reload))

(deftest convert-message-payload-to-bytes
  (let [bytes (byte-array (repeat 989 (byte 10)))
        msg (Message. 100 bytes)]
    ;; 1 magic byte
    ;; 1 byte attribute identifier for annotations on message
    ;; 4 bytes w/ CRC
    ;; n bytes payload
    ;; total = n + 4 + 1 + 1 bytes
    (is (= 995 (:size (to-clojure msg))))

    (is (= 989 (count (:payload (to-clojure msg)))))
    (is (= (seq bytes) (seq (:payload (to-clojure msg)))))))
