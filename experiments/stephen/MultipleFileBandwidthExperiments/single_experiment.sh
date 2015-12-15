#!/bin/bash

#Number of requests across all machines
requests_per_machine=16384

#Total Clients (across both machines)
total_clients=$1

#Clients per machine
clients_per_machine=$1

#Read Write Ratio
rw_ratio=$2

#Node id (either 1 or 2)
node_id=$3

#Output file
output_filename='hdfsrs/experiments/stephen/MultipleFileBandwidthData/'$total_clients'_'$rw_ratio'.csv'

#If more than two jobs, split them across the machines
if [ $total_clients -gt 1 ]
then
	#Split requests
	requests_per_machine=$((requests_per_machine / 2))
	#Split Clients
	clients_per_machine=$((clients_per_machine / 2))
fi

#Requests per client
requests_per_client=$(($requests_per_machine / $clients_per_machine))


for i in $(seq 1 $clients_per_machine)
do
	nohup hadoop jar hdfsrs/experiments/stephen/MultipleFileBandwidthExperiments/MultipleFileBandwidth.jar run $requests_per_client 32 $rw_ratio '/node'$node_id'file'$i'.txt' &>> $output_filename&
done
