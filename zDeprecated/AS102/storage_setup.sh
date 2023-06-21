#!/bin/bash
# ------------------------------
# Setup for storage exercises
# ---

cd ~
sudo service aerospike stop
sudo rm /opt/aerospike/data/ns1.dat
sudo cp ~/student-workbook/AS102/storage_aerospike.conf /etc/aerospike/aerospike.conf
sudo service aerospike start
