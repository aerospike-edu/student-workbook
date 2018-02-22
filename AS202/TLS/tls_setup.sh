#!/bin/bash
# ------------------------------
# Setup for generating TLS certificates for exercises
# ---

sudo service amc stop
sudo service aerospike stop
sudo rm /opt/aerospike/data/ns1.dat
sudo rm /opt/aerospike/data/ns2.dat
sudo rm /var/log/aerospike/aerospike.log
sudo rm /var/log/aerospike/security.log
sudo cp ~/student-workbook/AS202/TLS/tls_aerospike.conf /etc/aerospike/aerospike.conf
sudo service aerospike start
sudo service amc start
