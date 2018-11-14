#!/bin/bash
# ------------------------------
# Setup for storage exercises
# ---

cd ~
sudo service aerospike stop
sudo rm /opt/aerospike/data/ns1.dat
sudo rm -r /opt/aerospike/data/ns1pi
sudo mkdir /opt/aerospike/data/ns1pi
sudo chown aerospike /opt/aerospike/data/ns1pi
sudo cp ~/student-workbook/AS102/quiesce_aerospike.conf /etc/aerospike/aerospike.conf
sudo service aerospike start
