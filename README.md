# Tests for Envoy Kafka filters

Runs some Kafka clients against Envoy and upstream Kafka clusters.

The tests should not be executed in parallel, as some of them depend on strict ordering of how the messages get appended.

Configuration for Envoy is present in `kafka-all.yaml` (notice it requires 4 Kafka brokers - 1 for broker tests, 3 for mesh - see below).

If Kafka brokers and Envoy are running with config mentioned below, then `./gradlew test` should work.

## Requirements for broker-filter tests:

* 1-node Kafka cluster on localhost:9292, advertising itself on localhost:19092 (what means Envoy)
    * expected version is 3.3.1
    * broker.id = 1
* Envoy listening on localhost:19092

## Requirements for mesh-filter tests:

* 3 Kafka clusters on:
    * localhost:9492, with topics `apples`, `apricots` with 1 partition each
    * localhost:9493, with topics `bananas`, `berries` with 1 partition each
    * localhost:9494, with topics `cherries`, `chocolates` with 5 partitions each
* Envoy listening on localhost:29092, with forwarding prefix-based rules:
    * `a` to localhost:9492
    * `b` to localhost:9493
    * `c` to localhost:9494
