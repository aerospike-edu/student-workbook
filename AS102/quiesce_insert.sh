#!/bin/bash
# ------------------------------
# Load test data using benchmark
# -
cd ~/packages/aerospike/aerospike-client-java-*/benchmarks
./run_benchmarks -h 127.0.0.1 -p 3000 -n ns1 -s s  -k 64000000 -latency "7,1" -S 1 -o S:1 -w I -z 8
