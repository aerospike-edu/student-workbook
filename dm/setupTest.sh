#!/bin/bash
# ------------------------------
# Setup test namespace
# Clear cache example files, if any
# ---

cd ~
sudo service aerospike stop
sudo service amc stop
sudo rm /opt/aerospike/data/Cache.dat
sudo rm /var/log/aerospike/aerospike.log
sudo cp ~/student-workbook/dm/start_aerospike.conf /etc/aerospike/aerospike.conf
sudo service aerospike start
sudo service amc start
