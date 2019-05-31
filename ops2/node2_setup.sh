#!/bin/bash
# ------------------------------
# Setup for Histogram Exercise
# ---

cd ~
sudo service amc stop
sudo service aerospike stop

# Clean data and log files
sudo rm /opt/aerospike/data/*.dat
sudo rm /var/log/aerospike/aerospike.log

# Copy node configuration file
sudo cp ~/student-workbook/ops2/node2.conf /etc/aerospike/aerospike.conf

sudo service aerospike start
sudo service amc start

echo "Enable read, write, storage, svc and proxy benchmarks"
asinfo -v 'set-config:context=namespace;id=ns1;enable-benchmarks-read=true'
asinfo -v 'set-config:context=namespace;id=ns1;enable-benchmarks-write=true'
asinfo -v 'set-config:context=namespace;id=ns1;enable-benchmarks-storage=true'
asinfo -v 'set-config:context=service;enable-benchmarks-svc=true'
asinfo -v 'set-config:context=namespace;id=ns1;enable-hist-proxy=true'

