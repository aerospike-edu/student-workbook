#!/bin/bash
docker build -t websvc .  
docker images 
docker tag websvc aerospiketraining/websvc
docker images
cat pwdfile | docker login --username aerospiketraining --password-stdin
docker push aerospiketraining/websvc  
kubectl -n aerospike create deployment websvc --image=aerospiketraining/websvc:latest 
kubectl -n aerospike apply -f ~/student-workbook/k8/k8package/exposeWebsvcPort.yaml
kubectl -n aerospike get pods
