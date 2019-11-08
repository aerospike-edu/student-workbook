#!/bin/bash
# ------------------------------
# Setup for Histogram Exercise
# ---

cd ~
service amc stop
service aerospike stop

# Clean data and log files
rm /opt/aerospike/data/*.dat
rm /var/log/aerospike/aerospike.log

# Copy node configuration file
cp ~/student-workbook/ops2/node1.conf /etc/aerospike/aerospike.conf

service aerospike start
service amc start

echo "Enable read, write, storage, svc and proxy benchmarks"
asinfo -v 'set-config:context=namespace;id=ns1;enable-benchmarks-read=true'
asinfo -v 'set-config:context=namespace;id=ns1;enable-benchmarks-write=true'
asinfo -v 'set-config:context=namespace;id=ns1;enable-benchmarks-storage=true'
asinfo -v 'set-config:context=service;enable-benchmarks-svc=true'
asinfo -v 'set-config:context=namespace;id=ns1;enable-hist-proxy=true'

