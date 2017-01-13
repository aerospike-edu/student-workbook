#!/bin/bash
# ------------------------------
# Install Java Benchmark
# -
cd ~/packages/aerospike
tar xvf aerospike-client-java.tgz
cd aerospike-client-java-*/benchmarks
mvn package
