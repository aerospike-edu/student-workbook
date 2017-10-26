#!/bin/bash
# ------------------------------
# Setup for xdr exercises
# ---

cd ~
sudo service aerospike stop
sudo rm /opt/aerospike/data/ns1.dat
sudo cp ~/student-workbook/AS202/xdr_aerospike.conf /etc/aerospike/aerospike.conf
sudo service aerospike start
