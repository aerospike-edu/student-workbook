#!/bin/bash
# ------------------------------
# Setup for storage exercises
# ---

cd ~
sudo service amc stop
sudo service aerospike stop

# Clean any secondary indexes and data files
sudo rm /opt/aerospike/data/*.dat
sudo rm /opt/aerospike/smd/sindex.smd

sudo cp ~/student-workbook/ops/storage_aerospike.conf /etc/aerospike/aerospike.conf
sudo service aerospike start
sudo service amc start
