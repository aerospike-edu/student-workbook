#!/bin/bash
docker build -t websvc .  
docker images 
docker tag websvc aerospiketraining/websvc
docker images
docker push aerospiketraining/websvc  
kubectl -n aerospike delete deployment websvc
kubectl -n aerospike create deployment websvc --image=aerospiketraining/websvc:latest 
kubectl -n aerospike get pods
