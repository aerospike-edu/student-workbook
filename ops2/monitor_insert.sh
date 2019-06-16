#!/bin/bash
cd ~/packages/aerospike/aerospike-client-java*/benchmarks
./run_benchmarks -h 127.0.0.1  -n ns1 -s testset -k 10000 -S 1 -o S:16 -w I  -z 1 -g 50
