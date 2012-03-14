(ns clj-kafka.core
  (:import [java.nio ByteBuffer]
           [java.util Properties]
           [kafka.message MessageAndOffset Message]))

(defn as-properties
  [m]
  (let [props (Properties. )]
    (doseq [[n v] m] (.setProperty props n v))
    props))

(defmacro with-resource
  [binding close-fn & body]
  `(let ~binding
     (try
       (do ~@body)
       (finally
        (~close-fn ~(binding 0))))))

(defprotocol ToClojure
  (to-clojure [x] "Converts type to Clojure structure"))

(extend-protocol ToClojure
  MessageAndOffset
  (to-clojure [x] {:message (to-clojure (.message x))
                   :offset (.offset x)})
  ByteBuffer
  (to-clojure [x] (let [b (byte-array (.remaining x))]
                    (.get x b)
                    b))
  Message
  (to-clojure [x] {:crc (.checksum x)
                   :payload (to-clojure (.payload x))
                   :size (.size x)}))
