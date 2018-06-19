6/19/18 - Issue install benchmarks fails to build due to surefire plugin. 
Cause - need to upgrade java 7 on AWS instance to java 8
Use getjava8.sh to get java8 on AWS instance
$ sudo ./getjava.sh
$ ls  --> should show the downloaded 8u...jdk, install it as below:
$ sudo rpm -Uvh jdk-8u172-linux-x64.rpm 
