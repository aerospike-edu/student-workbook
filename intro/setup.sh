#!/bin/bash
# ------------------------------
# Setup Aerospike
# ---

cd ~
sudo service aerospike stop
sudo service amc stop
sudo cp ~/student-workbook/intro/start_aerospike.conf /etc/aerospike/aerospike.conf
sudo service aerospike start
sudo amc aerospike start
