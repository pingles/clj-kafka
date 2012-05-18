(ns clj-kafka.example.zk
  (:use [clj-kafka.core :only (with-resource)]
        [clj-kafka.consumer.zk :only (consumer shutdown messages)]))

(def config {"zk.connect" "kafka.uswitchinternal.com:2181"
             "autocommit.enable" "false"
             "groupid" "group1"})

(defn -main []
  (with-resource [c (consumer config)]
    shutdown
    (doseq [m (messages c "testing" "testing2")]
      (println "Message: " (String. (:payload m))))))
