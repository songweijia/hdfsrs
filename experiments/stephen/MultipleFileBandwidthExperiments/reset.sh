#!/bin/bash
stop-dfs.sh
rm -rf /opt/hadoop/data/dn
rm -rf /opt/hadoop/data/nn
rm -rf /opt/hadoop/logs
hdfs namenode -format
start-dfs.sh

num_clients=$1
clients_per_machine=$1
if [ $num_clients -gt 1 ]
then
	clients_per_machine=$((clients_per_machine / 2))
fi

#Split up file sizes evenly
file_size=$((128 / num_clients))


#Write 1024 GB to file
for i in $(seq 1 $clients_per_machine)
do
	echo $file_size' written to node1file'$i'.txt'
	hadoop jar hdfsrs/experiments/stephen/MultipleFileBandwidthExperiments/MultipleFileBandwidth.jar setup $file_size /node1file$i.txt
 	if [ $num_clients -gt 1 ]
 	then
		echo $file_size' written to node2file'$i'.txt'
		hadoop jar hdfsrs/experiments/stephen/MultipleFileBandwidthExperiments/MultipleFileBandwidth.jar setup $file_size /node2file$i.txt
	fi
done
