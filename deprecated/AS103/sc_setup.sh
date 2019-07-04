#!/bin/bash
# ------------------------------
# Setup for Strong Consistency Mode exercises
# ---

cd ~
sudo service aerospike stop
sudo service amc stop
sudo rm /opt/aerospike/data/ns1f1.dat
sudo rm /opt/aerospike/data/ns1f2.dat
sudo cp ~/student-workbook/AS103/sc_aerospike.conf /etc/aerospike/aerospike.conf
sudo service aerospike start
sudo service amc start
