#!/bin/bash

ENVOY_BIN='/Users/adam.kotwasinski/space-envoy/envoy2/bazel-bin/contrib/exe/envoy-static'

"${ENVOY_BIN}" -c kafka-all.yaml -l info --concurrency 4
