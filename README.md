# clj-kafka

Simple Clojure interface to [Kafka](http://incubator.apache.org/kafka/).

It's currently a snapshot only until things flesh out a little more. [API Documentation is also available](http://pingles.github.com/clj-kafka/).

Note: Kafka binaries are not currently published to any public repositories. Additionally, the 0.8 release was [published as source](http://incubator.apache.org/kafka/downloads.html). This library uses [a build of the 0.8 incubator release published on Clojars](https://clojars.org/com.uswitch/kafka_2.9.2).

Current build status: ![Build status](https://secure.travis-ci.org/pingles/clj-kafka.png)

## Installing

Add the following to your [Leiningen](http://github.com/technomancy/leiningen) `project.clj`:

```clj
[clj-kafka "0.1.0-0.8-SNAPSHOT"]
```

## Usage

clj-kafka currently only supports Kafka 0.8.

### Producer

Allows batching of messages:

```clj
(use 'clj-kafka.producer)

(def p (producer {"zk.connect" "localhost:2181"}))
(send-messages p "test" (->> ["message payload 1" "message payload 2"]
                             (map #(.getBytes %))
                             (map message)))
```

Or sending a single message:

```clj
(def p (producer {"zk.connect" "localhost:2181"}))
(send-messages p "test" (message (.getBytes "payload")))
```

### SimpleConsumer

```clj
(use 'clj-kafka.consumer.simple)

(def c (consumer "localhost" 9092))
(def f (fetch "test" 0 0 4096))

(messages c f)

({:message {:crc 1513777821, :payload #<byte[] [B@3088890d>, :size 1089}, :offset 1093} {:message {:crc 4119364266, :payload #<byte[] [B@3088890d>, :size 968}, :offset 2065} {:message {:crc 3827222527, :payload #<byte[] [B@3088890d>, :size 1137}, :offset 3206})
```

### Zookeeper Consumer

The Zookeeper consumer uses broker information contained within Zookeeper to consume messages. This consumer also allows the client to automatically commit consumed offsets so they're not retrieved again.

```clj
(use 'clj-kafka.consumer.zk)
(use 'clj-kafka.core)

(def config {"zookeeper.connect" "localhost:2181" 
             "group.id"    "my-task-group"
             "auto.offset.reset" "smallest"})

(with-resource [c (consumer config)]
  shutdown
  (take 2 (messages c "test")))

({:crc 3417370184, :payload #<byte[] [B@698b41da>, :size 22} {:crc 3417370184, :payload #<byte[] [B@698b41da>, :size 22} {:crc 960674935, :payload #<byte[] [B@698b41da>, :size 86} {:crc 3651343620, :payload #<byte[] [B@698b41da>, :size 20} {:crc 2012604996, :payload #<byte[] [B@698b41da>, :size 20})
```

It's also now possible to consume messages from multiple topics at the same time. These are aggregated and returned as a single sequence:

```clojure
(take 5 (messages c "test1" "test2"))
```

## License

Copyright &copy; 2013 Paul Ingles

Distributed under the Eclipse Public License, the same as Clojure.
