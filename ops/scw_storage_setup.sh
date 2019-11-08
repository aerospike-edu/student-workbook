#!/bin/bash
# ------------------------------
# Setup for storage exercises
# ---

cd ~
service amc stop
service aerospike stop

# Clean any secondary indexes and data files
rm /opt/aerospike/data/*.dat
rm /opt/aerospike/smd/sindex.smd

cp ~/student-workbook/ops/storage_aerospike.conf /etc/aerospike/aerospike.conf
service aerospike start
service amc start
