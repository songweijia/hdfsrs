#This runs the experiment for a specific number of clients and readwrite ratio

num_clients=$1
ratio=$2

cmd='sh hdfsrs/experiments/stephen/MultipleFileThroughputExperiments/single_experiment.sh'

#Restart File System and write files
echo "Resetting File System"
sh hdfsrs/experiments/stephen/MultipleFileThroughputExperiments/reset.sh $num_clients

#Run on first node
ssh -i stephen.pem 128.84.105.91 -l root $cmd' '$num_clients' '$ratio' 1'
echo "Running Node 1 "${clients[c]}' clients, readratio: '$ratio
if [ $c -gt 0 ]
then
	#Run on second node
	ssh -i stephen.pem 128.84.105.149 -l root $cmd' '$num_clients' '$ratio' 2'
	echo "Running Node 2 "$num_clients' clients, readratio: '$ratio
fi

