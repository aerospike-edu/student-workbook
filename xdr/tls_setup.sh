#!/bin/bash
# ------------------------------
# Setup for generating TLS certificates for exercises
# ---

sudo service amc stop
sudo service aerospike stop
sudo rm /opt/aerospike/data/ns1.dat
sudo rm /opt/aerospike/digestlog
sudo rm /var/log/aerospike/aerospike.log
sudo rm /var/log/aerospike/xdr.log
sudo cp ~/student-workbook/xdr/tls_aerospike.conf /etc/aerospike/aerospike.conf
sudo service aerospike start
sudo service amc start
