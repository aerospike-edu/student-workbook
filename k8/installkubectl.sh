#!/bin/bash
echo "Installing kubectl ..."
cd ~
curl -o kubectl https://amazon-eks.s3.us-west-2.amazonaws.com/1.18.9/2020-11-02/bin/linux/amd64/kubectl
chmod +x ./kubectl
mkdir -p $HOME/bin 
cp ./kubectl $HOME/bin/kubectl
echo 'export PATH=$PATH:$HOME/bin' >> ~/.bashrc
kubectl version --short --client
