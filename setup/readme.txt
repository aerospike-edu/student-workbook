6/19/18 - Issue install benchmarks fails to build due to surefire plugin. 
Cause - need to upgrade java 7 on AWS instance to java 8
Use getjava8.sh to get java8 on AWS instance
$ cd /usr/java
$ cp ~/student-workbook/getjava8.sh .
$ sudo ./getjava8.sh
$ ls  --> should show the downloaded 8u...jdk, install it as below:
$ sudo rpm -Uvh jdk-8u172-linux-x64.rpm 
$ cd ~/student-workbook/AS201
$ ./install_bechmark.sh  --> should work

