#!/bin/bash
cd ~/packages/aerospike/aerospike-client-java*/benchmarks
./run_benchmarks  -n $1 -s testset -k 1500000 -S 1 -o S:1 -w RU,50  -z 16
