#!/bin/bash
# ------------------------------
# Setup for xdr tuning exercises
# ---

cd ~
sudo service amc stop
sudo service aerospike stop
sudo rm /opt/aerospike/data/ns1.dat
sudo rm /opt/aerospike/digestlog
sudo rm /var/log/aerospike/aerospike.log
sudo rm /var/log/aerospike/xdr.log
sudo cp ~/student-workbook/AS201/tune1_aerospike.conf /etc/aerospike/aerospike.conf
sudo service aerospike start
sudo service amc start
