#!/bin/bash
cd ~/packages/aerospike/aerospike-client-java*/benchmarks
# Insert 10K records
./run_benchmarks -h 52.66.212.176  -n ns1 -s testset -k 10000 -S 1 -o S:30000 -w I  -z 8

# Read - Update 10K records.
./run_benchmarks -h 52.66.212.176  -n ns1 -s testset -k 10000 -S 1 -o S:30000 -w RU,50  -z 8
