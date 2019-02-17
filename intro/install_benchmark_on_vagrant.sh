#!/bin/bash
# ------------------------------
# Install Java Benchmark
# -
cd ~
sudo yum install -y java-1.8.0-openjdk-devel
java -version
cd /opt
sudo wget http://www-eu.apache.org/dist/maven/maven-3/3.5.4/binaries/apache-maven-3.5.4-bin.tar.gz
sudo tar xzf apache-maven-3.5.4-bin.tar.gz
sudo ln -s apache-maven-3.5.4 maven
sudo cp ~/student-workbook/intro/maven.sh /etc/profile.d/maven.sh
source /etc/profile.d/maven.sh
cd ~
sudo mkdir packages
cd packages
sudo mkdir aerospike
cd aerospike
sudo wget -O aerospike-client-java.tgz  http://www.aerospike.com/download/client/java/latest/artifact/tgz
cd ~/packages/aerospike
tar xvf aerospike-client-java.tgz
cd aerospike-client-java-*/benchmarks
mvn package
