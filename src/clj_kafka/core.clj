(ns clj-kafka.core
  (:import [java.util Properties]
           [kafka.message MessageAndOffset Message]))

(defn as-properties
  [m]
  (let [props (Properties. )]
    (doseq [[n v] m] (.setProperty props n v))
    props))

(defprotocol ToClojure
  (to-clojure [_] "Converts type to Clojure structure"))

(extend-protocol ToClojure
  MessageAndOffset
  (to-clojure [x] {:message (to-clojure (.message x))
                   :offset (.offset x)})
  Message
  (to-clojure [x] {:crc (.checksum x)
                   :payload (.array (.payload x))
                   :size (.size x)}))
