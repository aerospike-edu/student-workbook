#!/bin/bash
echo "Installing docker ..."
sudo amazon-linux-extras install docker -y
which docker
sudo service docker start
sudo usermod -a -G docker training
# logout, and log back in for command below to work
docker info
