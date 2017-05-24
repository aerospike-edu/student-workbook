#!/bin/bash
# ------------------------------
# Setup for migrating AS102 instance for AS101
# ---

cd ~
sudo service aerospike start
asinfo -v "tip-clear:host-port-list=all"
asinfo -v 'services-alumni-reset'
sudo service aerospike stop
sudo service amc stop
sudo rm /opt/aerospike/data/*.dat
sudo cp ~/student-workbook/AS101/orig_aerospike.conf /etc/aerospike/aerospike.conf
sudo pip install aerospike -U
#For Stream UDFs in python client, students working on AWS
sudo chown aerotraining /usr/local/aerospike/usr-lua
