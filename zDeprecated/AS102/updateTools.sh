#!/bin/bash
# ------------------------------
# Update Tools Package to 3.12.0
# -
cd ~/packages/aerospike
wget -O tools-3.12.0.tgz http://www.aerospike.com/download/tools/3.12.0/artifact/el6
tar xvf tools-3.12.0.tgz
cd aerospike-tools-3.12.0-*
sudo ./asinstall

