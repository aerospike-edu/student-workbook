#!/bin/bash
# ------------------------------
# For testing constructs
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
git clone https://github.com/aerospike-edu/student-workbook.git
sudo chown aerotraining /usr/local/aerospike/usr-lua
