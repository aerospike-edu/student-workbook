#!/bin/bash
# ------------------------------
# Setup for generating TLS certificates for exercises
# ---

cd ~/student-workbook/xdr
mkdir ./CA
cd ./CA
echo '01'> serial
touch index.txt
mkdir private
mkdir newcerts
cp ~/student-workbook/xdr/openssl.cnf ./openssl.cnf
