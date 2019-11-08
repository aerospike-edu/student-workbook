#!/bin/bash
# ------------------------------
# Setup Aerospike
# ---

cd ~
service amc stop
service aerospike stop

#clean any secondary indexes or data files
rm /opt/aerospike/data/*.dat
rm /opt/aerospike/smd/sindex.smd

cp ~/student-workbook/intro/start_aerospike.conf /etc/aerospike/aerospike.conf
service aerospike start
service amc start
