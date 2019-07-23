#!/bin/bash
# ------------------------------
# Setup test namespace for CappedMap
# ---

cd ~
sudo service aerospike stop
sudo service amc stop
sudo rm /opt/aerospike/data/test.dat
sudo rm /var/log/aerospike/aerospike.log
sudo cp ~/student-workbook/dm/cappedMap_aerospike.conf /etc/aerospike/aerospike.conf
sudo service aerospike start
sudo service amc start
