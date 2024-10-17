# Tests for Envoy Kafka filters

Runs some Kafka clients against Envoy and upstream Kafka clusters.

The tests should not be executed in parallel, as some of them depend on strict ordering of how the messages get appended.

Configuration for Envoy is present in `kafka-all.yaml` (notice it requires 6 Kafka brokers - 3 for broker tests, 3 for mesh - see below).

If Kafka brokers and Envoy are running with config mentioned below, then `./gradlew test` should work.

`StatefulConsumerProxyTest` uses Envoy's librdkafka consumers, so it might be necessary to restart Envoy before running the same test again.

If upgrading Kafka, you can use `compare.py` to figure out what changed.

## Requirements for broker-filter tests:

* 3-node Kafka cluster:
    * broker id=1 - localhost:9092,
    * broker id=2 - localhost:9093,
    * broker id=3 - localhost:9094
* Envoy proxy listening on on localhost:19092, localhost:19093, localhost:19094 which performs the response rewrite to:
    * broker id=1 ->  localhost:19092,
    * broker id=2 ->  localhost:19093,
    * broker id=3 ->  localhost:19094

Notice that broker ids are important here.

## Requirements for mesh-filter tests:

* 3 Kafka clusters on:
    * localhost:9492
    * localhost:9493
    * localhost:9494
* Envoy listening on localhost:29092, with forwarding prefix-based rules:
    * `a` to localhost:9492
    * `b` to localhost:9493
    * `c` to localhost:9494

Broker ids do not matter here, as Envoy is going to advertise itself as its own "cluster".
