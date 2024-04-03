#!/bin/bash
# ------------------------------
# Setup Aerospike
# ---

cd ~
sudo service amc stop
sudo service aerospike stop

#clean any secondary indexes or data files
sudo rm /opt/aerospike/data/*.dat
sudo rm /opt/aerospike/smd/sindex.smd

sudo cp ~/student-workbook/intro/start_aerospike.conf /etc/aerospike/aerospike.conf
sudo service aerospike start
sudo service amc start
