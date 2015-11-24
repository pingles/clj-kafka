# clj-kafka

Clojure library for [Kafka](https://kafka.apache.org).

Current build status: [![Build Status](https://travis-ci.org/pingles/clj-kafka.png)](https://travis-ci.org/pingles/clj-kafka)

Development is against the 0.8 release of Kafka.

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

See: [clj-kafka.producer](https://pingles.github.io/clj-kafka/clj-kafka.producer.html)


### New Producer

As of 0.3.1 we also support the "new" pure-Java producer. The
interface is superficially similar but we've chosen to keep names
close to their Java equivalent.

```clj
(use 'clj-kafka.new.producer)

(with-open [p (producer {"bootstrap.servers" "127.0.0.1:9092"} (byte-array-serializer) (byte-array-serializer))]
  (send p (record "test-topic" (.getBytes "hello world!"))))
```

One key difference is that sending is asynchronous by default. `send`
returns a `Future` immediately. If you want synchronous behaviour
you can deref it right away:

```clj
(with-open [p (producer {"bootstrap.servers" "127.0.0.1:9092"} (byte-array-serializer) (byte-array-serializer))]
  @(send p (record "test-topic" (.getBytes "hello world!"))))
```

See: [clj-kafka.new.producer](https://pingles.github.io/clj-kafka/clj-kafka.new.producer.html)


### Zookeeper Consumer

The Zookeeper consumer uses broker information contained within
Zookeeper to consume messages. This consumer also allows the client to
automatically commit consumed offsets so they're not retrieved again.

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

The `messages` function provides the easy-case of single topic and single thread consumption. This
is a stricter form of the same API that was in earlier releases. `messages` is built on two key
other functions: `create-message-streams` and `stream-seq` that create the underlying streams and
turn them into lazy sequences respectively; this change makes it easier to consume across multiple
partitions and threads.

See: [clj-kafka.consumer.zk](https://pingles.github.io/clj-kafka/clj-kafka.consumer.zk.html)

#### Usage with transducers

An alternate way of consuming is using `create-message-stream` or
`create-message-streams` to obtain `KafkaStream` instances. These are
`Iterable` which means, amongst other things, that they work nicely
with transducers.

Continuing previous example:

```clj
;; hypothetical transformation
(def xform (comp (map deserialize-message)
                 (filter production-traffic)
                 (map parse-user-agent-string)))

(with-resource [c (consumer config)]
  shutdown
  (let [stream (create-message-stream c "test-topic")]
    (run! write-to-database! (eduction xform stream))))
```


### Administration Operations

There is support the following simple administration operations:

- checking if a topic exists
- creating a topic
- deleting a topic (requires that the Kafka cluster supports deletion
and has `delete.topic.enable` set to `true`)
- retrieving topic configuration
- changing topic configuration

```clj
(require '[clj-kafka.admin :as admin])

(with-open [zk (admin/zk-client "127.0.0.1:2181")]
  (if-not (admin/topic-exists? zk "test-topic")
    (admin/create-topic zk "test-topic"
                        {:partitions 3
                         :replication-factor 1
                         :config {"cleanup.policy" "compact"}})))
```

See: [clj-kafka.admin](https://pingles.github.io/clj-kafka/clj-kafka.admin.html)


### Kafka Offset Manager Operations

There is support the following simple Kafka offset management operations:

- fetch the current offsets of a consumer group
- reset the current offsets of a consumer group

```clj
(require '[clj-kafka.offset :as offset])

(fetch-consumer-offsets "broker1:9092,broker1:9092" {"zookeeper.connect" "zkhost:2182"} "my-topic" "my-consumer")
(reset-consumer-offsets "broker1:9092,broker1:9092" {"zookeeper.connect" "zkhost:2182"} "my-topic" "my-consumer" :earliest)
(reset-consumer-offsets "broker1:9092,broker1:9092" {"zookeeper.connect" "zkhost:2182"} "my-topic" "my-consumer" :latest)
```

See: [clj-kafka.admin](https://pingles.github.io/clj-kafka/clj-kafka.offset.html)


## License

Copyright &copy; 2013 Paul Ingles

Distributed under the Eclipse Public License, the same as Clojure.

## Thanks

YourKit is kindly supporting this open source project with its full-featured Java Profiler. YourKit, LLC is the creator of innovative and intelligent tools for profiling Java and .NET applications. Take a look at YourKit's leading software products:
[YourKit Java Profiler](http://www.yourkit.com/java/profiler/index.jsp) and
[YourKit .NET Profiler](http://www.yourkit.com/.net/profiler/index.jsp).
