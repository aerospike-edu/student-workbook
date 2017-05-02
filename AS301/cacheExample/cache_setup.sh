#!/bin/bash
# ------------------------------
# Setup for Cache Example
# ---

cd ~
sudo service aerospike stop
sudo rm /opt/aerospike/data/Cache.dat
sudo cp ~/student-workbook/AS102/cache_aerospike.conf /etc/aerospike/aerospike.conf
sudo service aerospike start
