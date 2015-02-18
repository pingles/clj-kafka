# clj-kafka

Clojure library for [Kafka](https://kafka.apache.org).

Current build status: [![Build Status](https://travis-ci.org/pingles/clj-kafka.png)](https://travis-ci.org/pingles/clj-kafka)

Development is against the 0.8 release of Kafka. The protocols for 0.7 and 0.8 are incompatible so this will only work when connecting to a 0.8 cluster. Earlier releases of clj-kafka support the earlier 0.7 release if you need it.

## Installing

Add the following to your [Leiningen](http://github.com/technomancy/leiningen) `project.clj`:

![latest clj-kafka version](https://clojars.org/clj-kafka/latest-version.svg)

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

(send-message p (message "test" (.getBytes "this is my message")))
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
  (take 2 (messages c "test")))
```

## License

Copyright &copy; 2013 Paul Ingles

Distributed under the Eclipse Public License, the same as Clojure.

## Thanks

YourKit is kindly supporting this open source project with its full-featured Java Profiler. YourKit, LLC is the creator of innovative and intelligent tools for profiling Java and .NET applications. Take a look at YourKit's leading software products:
[YourKit Java Profiler](http://www.yourkit.com/java/profiler/index.jsp) and
[YourKit .NET Profiler](http://www.yourkit.com/.net/profiler/index.jsp).
