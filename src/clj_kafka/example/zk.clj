(ns clj-kafka.example.zk
  (:use [clj-kafka.core :only (with-resource)]
        [clj-kafka.consumer.zk :only (consumer shutdown messages)]))

(def config {"zk.connect" "localhost:2181"
             "auto.commit.enable" "false"
             "group.id" "group1"})

(defn -main []
  (with-resource [c (consumer config)]
    shutdown
    (doseq [m (messages c "testing" "testing2")]
      (println (:topic m) ": " (String. (:payload m))))))
