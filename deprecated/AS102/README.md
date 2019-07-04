# Aerospike Deployment Training README File

There are specific commands that should be used in conjunction with the Aerospike Deployment Training. These are available in the following script files.

### Using Scripts to Download and Install Server and AMC
Note: Instead of latest (default), enter 3.10.1.2 for Aerospike server. For AMC you can use latest (default) or 3.6.13.

For Deployment course: Just use: ~/install_as.sh. Then students follow course directions to install and start the server and AMC.

For Development course: Just use: ~/dev_install.sh. It will call ~/install_as.sh.

Then students will start the server and AMC and cd to:

~/student-workbook/AS101/Python/solution

and invoke

python Program.py

Using command 11, and then 12, they can populate the test data on the server for exercises.

For reference, here are the actual download commands should you need them.

### Downloading Aerospike Management Console
This command gets the latest version of the Aerospike Management Console from the Aerospike Web site. Please note that the "O" is a
 capital letter, not the numeral.

    wget -O aerospike-amc.rpm http://www.aerospike.com/download/amc/latest/artifact/el6

### Downloading Aerospike Server
This command gets the latest version of the Aerospike server from the Aerospike Web site. Please note that the "O" is a capital let
ter, not the numeral.


    wget -O aerospike-server.tgz http://www.aerospike.com/download/server/latest/artifact/el6
