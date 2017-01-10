#!/bin/bash
# ------------------------------
# Download and install Aerospike
# ------------------------------
cd
mkdir -p packages/aerospike

cd packages/aerospike
#rm -fr *

hasWget=`rpm -qa wget`
if [ ! "$hasWget" ]
then
  yum -y install wget
fi

read -p "Please enter the AS Server version (or nothing for latest version): " SERVER_VERSION
read -p "Please enter the AMC version (or nothing for latest version): " AMC_VERSION

# prompt for the user
read -p "Please enter the Aerospike Enterprise username (or nothing for Community Edition): " USER

if [ "$USER" != "" ]
then
  echo "Loading Aerospike Enterprise software for user: $USER"
  wget -O aerospike-server.tgz --user $USER --ask-password  http://www.aerospike.com/enterprise/download/server/${SERVER_VERSION:-latest}/artifact/el6
  wget -O aerospike-amc.rpm --user $USER --ask-password  http://www.aerospike.com/enterprise/download/amc/${AMC_VERSION:-latest}/artifact/el6
else
  echo "Loading Aerospike Community software"
  wget -O aerospike-server.tgz http://www.aerospike.com/download/server/${SERVER_VERSION:-latest}/artifact/el6
  wget -O aerospike-amc.rpm http://www.aerospike.com/download/amc/${AMC_VERSION:-latest}/artifact/el6
fi

# Get the java client to get the benchmarking tool too
wget -O aerospike-client-java.tgz  http://www.aerospike.com/download/client/java/latest/artifact/tgz

# Write out the helper for inserting benchmark data
cat > insert_records.sh <<'endmsg'
#!/bin/bash
cd aerospike-client-java*/benchmarks
for i in {1..200}
do
  asinfo -v "set-config:context=namespace;id=ns1;default-ttl=${i}m"
  ./run_benchmarks -n ns1 -s testset -k 500 -S $((300000+500*i)) -o S:2048 -w I -z 8
done
endmsg
chmod +x insert_records.sh

#Clone the student-workbook for AS101 & AS102 exercises
cd ~
git clone https://github.com/aerospike-edu/student-workbook.git
