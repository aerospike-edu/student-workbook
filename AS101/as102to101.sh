#!/bin/bash
# ------------------------------
# Setup for migrating AS102 instance for AS101
# ---

cd ~
sudo service aerospike stop
sudo service amc stop
sudo rm /opt/aerospike/data/*.dat
sudo cp ~/student-workbook/AS101/orig_aerospike.conf /etc/aerospike/aerospike.conf
