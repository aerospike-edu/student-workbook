#!/bin/bash
# ------------------------------
# Setup for Cache Example
# ---

cd ~
sudo service aerospike stop
sudo service amc stop
sudo rm /opt/aerospike/data/Cache.dat
sudo rm /var/log/aerospike/aerospike.log
sudo cp ~/student-workbook/AS301/start_aerospike.conf /etc/aerospike/aerospike.conf
sudo service aerospike start
sudo service amc start
