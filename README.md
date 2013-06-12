# clj-kafka

Clojure library for [Kafka](http://incubator.apache.org/kafka/).

Current build status: [![Build Status](https://travis-ci.org/pingles/clj-kafka.png?branch=0.8)](https://travis-ci.org/pingles/clj-kafka)

Development is against the unreleased 0.8 branch of Kafka. The protocols for 0.7 and 0.8 are incompatible so this will only work when connecting to a 0.8 cluster.

## Installing

Given 0.8 is still unreleased we're pushing SNAPSHOT releases.

Add the following to your [Leiningen](http://github.com/technomancy/leiningen) `project.clj`:

```clj
[clj-kafka "0.1.0-0.8-SNAPSHOT"]
```

## Usage

### Producer

Discovery of Kafka brokers from Zookeeper:

```clj
(brokers {"zookeeper.connect" "127.0.0.1:2181"})
;; ({:host "localhost", :jmx_port -1, :port 9999, :version 1})
```

```clj
(use 'clj-kafka.producer)

(def p (producer {"metadata.broker.list" "localhost:9999"
                  "serializer.class" "kafka.serializer.DefaultEncoder"
                  "partitioner.class" "kafka.producer.DefaultPartitioner"}))

(send-message p (keyed-message "test" (.getBytes "this is my message")))
```

### Zookeeper Consumer

The Zookeeper consumer uses broker information contained within Zookeeper to consume messages. This consumer also allows the client to automatically commit consumed offsets so they're not retrieved again.

```clj
(use 'clj-kafka.consumer.zk)
(use 'clj-kafka.core)

(def config {"zookeeper.connect" "localhost:2182"
             "group.id" "clj-kafka.consumer"
             "auto.offset.reset" "smallest"
             "auto.commit.enable" "false"})

(with-resource [c (consumer config)]
  shutdown
  (take 2 (messages c ["test"])))
```

It's also now possible to consume messages from multiple topics at the same time. These are aggregated and returned as a single sequence:

```clojure
(take 5 (messages c ["test1" "test2"]))
```

## License

Copyright &copy; 2013 Paul Ingles

Distributed under the Eclipse Public License, the same as Clojure.
