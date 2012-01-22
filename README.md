# clj-kafka

Simple Clojure interface to [Kafka](http://incubator.apache.org/kafka/).

## Usage

clj-kafka currently only supports consuming Kafka data against Kafka 0.7 and using the SimpleConsumer. Future versions may also include support for the ZookeeperConsumer (which tracks offsets and brokers using Zookeeper) and support for producing messages.

### SimpleConsumer

```clj
user> (def c (create-consumer "localhost" 9092))
user> (def f (fetch "test" 0 0 4096))

user> (messages c f)
({:message {:crc 1513777821, :payload #<byte[] [B@3088890d>, :size 1089}, :offset 1093} {:message {:crc 4119364266, :payload #<byte[] [B@3088890d>, :size 968}, :offset 2065} {:message {:crc 3827222527, :payload #<byte[] [B@3088890d>, :size 1137}, :offset 3206})
```

## License

Copyright &copy; 2012 Paul Ingles

Distributed under the Eclipse Public License, the same as Clojure.
