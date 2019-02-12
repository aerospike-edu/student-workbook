#!/bin/bash
# ------------------------------
# Setup for ALLFLASH exercises
# ---

cd ~
sudo service aerospike stop
sudo service amc stop
sudo rm /opt/aerospike/data/allflash.dat
sudo rm /opt/aerospike/data/in_mem.dat
sudo rm -r /opt/aerospike/data/pi
sudo mkdir /opt/aerospike/data/pi
sudo chown aerospike /opt/aerospike/data/pi
sudo cp ./allflash_aerospike.conf /etc/aerospike/aerospike.conf
sudo service aerospike start
sudo service amc start
