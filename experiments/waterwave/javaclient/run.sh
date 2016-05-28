#!/bin/bash
m=100
n=100
if [ $1 == "FFFSwrite" ]; then
	echo "FFFSwrite test";
	hadoop jar Client.jar Client -Dtest.buffersize=400 -Dtest.timelimit=$3 $2 FFFSwrite /real_data/data;
elif [ $1 == "HDFSwrite" ]; then
	echo "HDFSwrite test";
	hadoop jar Client.jar Client -Dtest.buffersize=400 -Dtest.timelimit=$3 $2 HDFSwrite /real_data/data;
else
	echo "read test";
	hadoop jar Client.jar Client $2,$m,$n,0 read $3;
fi
