#!/bin/bash
# ------------------------------
# Setup for generating TLS certificates for exercises
# ---

cd ~/student-workbook/AS201
echo '01'> serial
touch index.txt
mkdir private
mkdir newcerts
cp ~/student-workbook/AS201/openssl.cnf ./openssl.cnf
