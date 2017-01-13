#!/bin/bash
cd ~/packages/aerospike/aerospike-client-java*/benchmarks
asinfo -v 'set-config:context=namespace;id=ns1;default-ttl=3h’

./run_benchmarks -n ns1 -s testset -k 100000 -S 1 -o S:2048 -w I -z 8

asinfo -v 'set-config:context=namespace;id=ns1;default-ttl=4h’

./run_benchmarks -n ns1 -s testset -k 100000 -S 100001 -o S:2048 -w I -z 8

asinfo -v 'set-config:context=namespace;id=ns1;default-ttl=5h’

./run_benchmarks -n ns1 -s testset -k 100000 -S 200001 -o S:2048 -w I -z 8
