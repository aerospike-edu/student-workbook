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
#git clone https://github.com/aerospike/aerospike-client-python.git
#cd aerospike-client-python
#git fetch
#git checkout -b 2.2.2-ami-fix origin/2.2.2-ami-fix
#git submodule update --init
#sudo python setup.py install

#sudo rm -rf /tmp/pip-build-root/aerospike
#sudo pip install -vvv -U --index-url https://test.pypi.org/simple/ aerospike

sudo pip install aerospike -U
#For Stream UDFs in python client, students working on AWS
sudo mkdir /usr/local/aerospike/usr-lua
sudo chown aerotraining /usr/local/aerospike/usr-lua
