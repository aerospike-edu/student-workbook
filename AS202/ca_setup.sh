#!/bin/bash
# ------------------------------
# Setup for generating TLS certificates for exercises
# ---

sudo mkdir /CA
sudo chown aerotraining:aerotraining /CA
cd /CA
echo '01'> serial
touch index.txt
mkdir private
mkdir newcerts
cp ~/student-workbook/AS202/openssl.cnf ./openssl.cnf
