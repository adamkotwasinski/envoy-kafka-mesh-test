# envoy-kafka-mesh-test

Runs some Kafka clients against Envoy and upstream Kafka clusters.

Right now, one can run all tests from `envoy.broker` or `envoy.mesh` package, but not both, as there are some configuration collisions that I need to address.

The tests should not be executed in parallel, as some of them depend on strict ordering of how the messages get appended.

## Requirements for broker-filter tests:

* 1-node Kafka clusters on localhost:9092, advertising itself on localhost:19092 (what means Envoy)
* Envoy listening on localhost:19092 - see `envoy-broker.yaml`.

## Requirements for mesh-filter tests:

* 3 Kafka clusters on:
    * localhost:9092, with topics `apples`, `apricots` with 1 partition each
    * localhost:9093, with topics `bananas`, `berries` with 1 partition each
    * localhost:9094, with topics `cherries`, `chocolates` with 5 partitions each
* Envoy listening on localhost:29092, with forwarding prefix-based rules - see `envoy-mesh.yaml`:
    * `a` to localhost:9092
    * `b` to localhost:9093
    * `c` to localhost:9094
