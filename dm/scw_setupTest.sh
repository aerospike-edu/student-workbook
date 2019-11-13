#!/bin/bash
# ------------------------------
# Setup test namespace
# Clear cache example files, if any
# ---

cd ~
service aerospike stop
service amc stop
rm /opt/aerospike/data/Cache.dat
rm /var/log/aerospike/aerospike.log
cp ~/student-workbook/dm/start_aerospike.conf /etc/aerospike/aerospike.conf
service aerospike start
service amc start
