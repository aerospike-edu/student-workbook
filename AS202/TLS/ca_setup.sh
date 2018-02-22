#!/bin/bash
# ------------------------------
# Setup for generating TLS certificates for exercises
# ---

cd ~/student-workbook/AS202/TLS
mkdir ./CA
cd ./CA
echo '01'> serial
touch index.txt
mkdir private
mkdir newcerts
cp ~/student-workbook/AS202/openssl.cnf ./openssl.cnf
