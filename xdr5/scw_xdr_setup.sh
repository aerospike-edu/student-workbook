#!/bin/bash
# ------------------------------
# Setup for xdr exercises
# ---

cd ~
service amc stop
service aerospike stop
rm /opt/aerospike/data/ns1.dat
rm /var/log/aerospike/aerospike.log
rm /var/log/aerospike/xdr.log
cp ~/student-workbook/xdr5/xdr_aerospike.conf /etc/aerospike/aerospike.conf
service aerospike start
service amc start
