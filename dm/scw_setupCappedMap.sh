#!/bin/bash
# ------------------------------
# Setup test namespace for CappedMap
# ---

cd ~
service aerospike stop
service amc stop
rm /opt/aerospike/data/test.dat
rm /var/log/aerospike/aerospike.log
cp ~/student-workbook/dm/cappedMap_aerospike.conf /etc/aerospike/aerospike.conf
service aerospike start
service amc start
