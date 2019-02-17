#!/bin/bash
# ------------------------------
# Install Java Benchmark
# -
cd ~
wget -O aerospike-client-java.tgz  http://www.aerospike.com/download/client/java/latest/artifact/tgz
cd ~/packages/aerospike
tar xvf aerospike-client-java.tgz
cd aerospike-client-java-*/benchmarks
mvn package
