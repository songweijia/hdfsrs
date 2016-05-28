#!/bin/bash

si=$1
sj=$2
ti=$3
tj=$4

mstep=10
nstep=20
timesteps=100
writeOp=HDFSwrite

for ((i=$si;i<$ti;i+=$mstep))
do
	for ((j=$sj;j<$tj;j+=$nstep))
	do
		ei=$(($i+$mstep))
		ej=$(($j+$nstep))
		echo "run client $i,$j,$ei,$ej"
		hadoop jar Client.jar Client -Dtest.buffersize=800 -Dtest.timelimit=$timesteps $i,$j,$ei,$ej $writeOp /real_data/data &
	done
done

wait
