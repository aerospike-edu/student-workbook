#!/bin/bash
cd ~/packages/aerospike/aerospike-client-java*/benchmarks
./run_benchmarks -h 52.66.212.176  -n ns1 -s testset -k 1000 -S 1 -o S:30000 -w I  -z 8
