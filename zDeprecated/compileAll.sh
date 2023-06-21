#!/bin/bash

echo "###################################################"
echo "# Checking all java files to make sure they compile"
echo "#--------------------------------------------------"
MVN=`which mvn`
if [ "$MVN" == "" ]
then
	echo "No version of Maven exists on the path"
	exit 2
fi

echo "Checking for files to compile"
paths=`find . -name pom.xml -print | grep -v classes`

here=`pwd`
for thisPath in $paths
do
	dir=`dirname $thisPath`
	cd $dir
	output=`mvn clean compile`

	if [ $? -eq 0 ]
	then
  		echo "Successfully compiled $dir"
	else
		echo "Could not compile $dir, aborting" >&2
		echo $output
		exit 1
	fi
	cd $here
done

