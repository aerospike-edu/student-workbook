#!/bin/bash
# ------------------------------
# Load test data using benchmark
# -
cd ~/packages/aerospike/aerospike-client-java-*/benchmarks
./run_benchmarks -h 127.0.0.1 -p 3000 -n Cache -s cachetest -k 9000000 -latency "7,1" -S 1 -o S:600 -w I -z 1
