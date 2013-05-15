(ns clj-kafka.core
  (:import [java.nio ByteBuffer]
           [java.util Properties]
           [kafka.message MessageAndOffset Message]
           [java.util.concurrent LinkedBlockingQueue]))

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

(defn pipe
  "Returns a vector containing a sequence that will read from the
   queue, and a function that inserts items into the queue.

   Source: http://clj-me.cgrand.net/2010/04/02/pipe-dreams-are-not-necessarily-made-of-promises/"
  ([] (pipe 100))
  ([size]
   (let [q (java.util.concurrent.LinkedBlockingQueue. size)
         EOQ (Object.)
         NIL (Object.)
         s (fn queue-seq [] (lazy-seq
                              (let [x (.take q)]
                                (when-not (= EOQ x)
                                  (cons (when-not (= NIL x) x)
                                        (queue-seq))))))]
     [(s) (fn queue-put
            ([] (.put q EOQ))
            ([x] (.put q (or x NIL))))])))
