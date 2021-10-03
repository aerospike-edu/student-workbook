#!/bin/bash
# ------------------------------
# Setup for xdr exercises
# ---

cd ~
sudo service amc stop
sudo service aerospike stop
sudo rm /opt/aerospike/data/ns1.dat
sudo rm /opt/aerospike/digestlog
sudo rm /var/log/aerospike/aerospike.log
sudo rm /var/log/aerospike/xdr.log
sudo cp ~/student-workbook/xdr/xdr_aerospike.conf /etc/aerospike/aerospike.conf
sudo service aerospike start
sudo service amc start
