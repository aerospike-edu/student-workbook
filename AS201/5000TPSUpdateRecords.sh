#!/bin/bash
# ------------------------------
# Load test data using benchmark
# -
cd ~/packages/aerospike/aerospike-client-java-*/benchmarks
./run_benchmarks -h 127.0.0.1 -p 3000 -n ns1 -s testset -k 100000 -g 5000 -latency "7,1" -S 1 -o S:100 -w RU,50 -z 8
