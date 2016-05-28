#!/bin/bash
dirs=`hadoop dfs -ls /data/.snapshot | awk '{print $8}'`
i=0
for dir in $dirs; do
	echo "get $i slice: $dir";
	hadoop jar Client.jar Client $i,100,100,0 read $dir;
	i=$(($i+1));
done
