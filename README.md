# envoy-kafka-mesh-test

Runs some Kafka clients against Envoy and upstream Kafka clusters.

Execute with `./gradlew test`.

The tests should not be executed in parallel, as some of them depend on strict ordering of how the messages get appended.

## Requirements:

* 3 Kafka clusters on:
    * localhost:9092, with topics `apples`, `apricots` with 1 partition each
    * localhost:9093, with topics `bananas`, `berries` with 1 partition each
    * localhost:9094, with topics `cherries`, `chocolates` with 5 partitions each
* Envoy listening on localhost:19092, with forwarding prefix-based rules - see `envoy-config.yaml`:
    * `a` to localhost:9092
    * `b` to localhost:9093
    * `c` to localhost:9094

These constants are in `envoy.Environment`.
