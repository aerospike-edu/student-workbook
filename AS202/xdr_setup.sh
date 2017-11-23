#!/bin/bash
# ------------------------------
# Setup for xdr exercises
# ---

cd ~
sudo service amc stop
sudo service aerospike stop
sudo rm /opt/aerospike/data/ns1.dat
sudo rm /opt/aerospike/digestlog
sudo cp ~/student-workbook/AS202/xdr_aerospike.conf /etc/aerospike/aerospike.conf
sudo service aerospike start
sudo service amc start
