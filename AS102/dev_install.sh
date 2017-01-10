#!/bin/bash
# ------------------------------
# For Deployment training course
# ---

cd ~
./install_as.sh
cd packages/aerospike
tar -xvf aerospike-server.tgz
cd aerospike-server-*
sudo ./asinstall
cd ..
sudo rpm -ivh aerospike-amc.rpm
cd ~
sudo pip install aerospike -U
#For Stream UDFs in python client, students working on AWS
sudo chown aerotraining /usr/local/aerospike/usr-lua
sudo cp ~/student-workbook/AS102/start_aerospike.conf /etc/aerospike/aerospike.conf
