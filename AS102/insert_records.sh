#!/bin/bash
cd ~/packages/aerospike/aerospike-client-java*/benchmarks
for i in {1..200}
do
  asinfo -v "set-config:context=namespace;id=ns1;default-ttl=${i}m"
  ./run_benchmarks -n ns1 -s testset -k 500 -S $((300000+500*i)) -o S:2048 -w I -z 8
done
