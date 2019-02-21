#!/bin/bash
cd ~/packages/aerospike/aerospike-client-java*/benchmarks
./run_benchmarks -n ns1 -s testset -k 400000 -S 1 -o S:2048 -w RU,50 -z 8
