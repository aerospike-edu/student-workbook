#!/bin/bash
# ------------------------------
# Setup for installing C client on AWS instance
# However python client install already installs c client.
# ---

cd ~/packages/aerospike
wget -O aerospike-client-c.tgz http://www.aerospike.com/download/client/c/4.1.6/artifact/el6
sudo yum -y install openssl-devel
sudo yum -y install gcc gcc-c++
tar xvf aerospike-client-c.tgz
cd aerospike-client-c-4.1.6.el6.x86_64/
sudo rpm -i aerospike-client-c-devel-4.1.6-1.el6.x86_64.rpm
cd ~/student-workbook/AS101/C/solution
pwd
