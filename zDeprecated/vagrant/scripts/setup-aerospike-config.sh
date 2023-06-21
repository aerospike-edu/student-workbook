#!/bin/bash

TOTAL_NODES=3

NODE=1

while getopts p:t:n: option
do
	case "${option}" in
		p) IP_PREFIX=${OPTARG};;
		t) TOTAL_NODES=${OPTARG};;
		n) NODE=${OPTARG};;
	esac
done

function setupAerospike {
	echo "modifying Aerospike.conf file"
	rm /etc/aerospike/aerospike.conf
	cat /vagrant/scripts/aerospike-1.conf >> /etc/aerospike/aerospike.conf

	echo " logging {" >> /etc/aerospike/aerospike.conf
	echo "    file /vagrant/logs/aerospike${NODE}.log {" >> /etc/aerospike/aerospike.conf
	echo "        context any info" >> /etc/aerospike/aerospike.conf 
	echo "    }" >> /etc/aerospike/aerospike.conf
	echo "    file /vagrant/logs/udf${NODE}.log {" >> /etc/aerospike/aerospike.conf
	echo "         context any critical" >> /etc/aerospike/aerospike.conf
	echo "         context udf info" >> /etc/aerospike/aerospike.conf
	echo "    }" >> /etc/aerospike/aerospike.conf
	echo "    file /vagrant/logs/agg${NODE}.log {" >> /etc/aerospike/aerospike.conf
	echo "         context any critical" >> /etc/aerospike/aerospike.conf
	echo "         context aggr debug" >> /etc/aerospike/aerospike.conf
	echo "    }" >> /etc/aerospike/aerospike.conf
	echo "}" >> /etc/aerospike/aerospike.conf

	echo "network {" >> /etc/aerospike/aerospike.conf
	echo "	service {" >> /etc/aerospike/aerospike.conf
	echo "		address any" >> /etc/aerospike/aerospike.conf
	echo "		port 3000" >> /etc/aerospike/aerospike.conf
	echo "		access-address ${IP_PREFIX}${NODE} virtual" >> /etc/aerospike/aerospike.conf
	echo "		network-interface-name eth2" >> /etc/aerospike/aerospike.conf
	echo "		}" >> /etc/aerospike/aerospike.conf
	echo "	heartbeat {" >> /etc/aerospike/aerospike.conf
	echo "		mode mesh" >> /etc/aerospike/aerospike.conf
	echo "		port 3002" >> /etc/aerospike/aerospike.conf

	for i in $(seq 1 $TOTAL_NODES)
	do 
		entry="                mesh-seed-address-port ${IP_PREFIX}${i} 3002 node${i}"
		echo "adding ${entry}"
		echo "${entry}" >> /etc/aerospike/aerospike.conf
	done
	cat /vagrant/scripts/aerospike-2.conf >> /etc/aerospike/aerospike.conf
	sudo service aerospike restart
}


echo "setup Aerospike.conf file"
setupAerospike