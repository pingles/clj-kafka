(ns clj-kafka.example.zk
  (:use [clj-kafka.core :only (with-resource)]
        [clj-kafka.consumer.zk :only (consumer shutdown messages)]))

(def config {"zk.connect" "kafka.uswitchinternal.com:2181"
             "autocommit.enable" "false"
             "groupid" "group1"})

(defn- string-payload
  [{:keys [payload] :as message}]
  (assoc message :parsed-payload (String. payload)))

(defn- upcase
  [message]
  (update-in message [:parsed-payload] #(.toUpperCase %)))

(defn -main []
  (with-resource [c (consumer config)]
    shutdown
    (doseq [m (messages c {"testing" string-payload
                           "testing2" (comp upcase string-payload)})]
      (println "Message: " (:parsed-payload m)))))
