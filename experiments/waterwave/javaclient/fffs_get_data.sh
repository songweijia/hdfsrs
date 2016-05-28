#!/bin/bash
i=0
st=$1
end=$2
inv=50
while [ $st -le $end ]; do
	echo "get $i slice: $st";
	hadoop jar Client.jar Client -Dtest.buffersize=400 $i,100,100,$st read /data;
	i=$(($i+1));
	st=$(($st+$inv))
done
