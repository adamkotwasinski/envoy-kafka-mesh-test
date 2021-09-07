# envoy-kafka-mesh-test

Runs some Kafka clients against Envoy and upstream Kafka clusters.

The tests should not be executed in parallel, as some of them depend on strict ordering of how the messages get appended.
