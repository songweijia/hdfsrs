#This runs the experiment for every number of clients and readwrite ratio
#Clients 1, 2, 4, 8, 16, 32, 64
#Ratios 0, 0.25, 0.5, 0.75, 1.0

clients=(1 2 4 8 16 32 64)
ratios=(1.0 0.0)
waitTimes=(300s 1000s)

cmd='sh hdfsrs/experiments/stephen/SharedFileLatencyExperiments/single_experiment.sh'

for c in $(seq 0 6)
do
     for r in $(seq 0 1)
	 do
		 #Restart File System and write 1G GB to test.txt
		 echo "Resetting File System"
		 ssh -i stephen.pem 128.84.105.89 -l root 'sh hdfsrs/experiments/stephen/SharedFileLatencyExperiments/reset.sh' &> /dev/null
		 #Run on first node
		 ssh -i stephen.pem 128.84.105.91 -l root $cmd' '${clients[c]}' '${ratios[r]}
		 echo "Running Node 1 "${clients[c]}' clients, readratio: '${ratios[r]}
		 if [ $c -gt 0 ]
		 then
		     #Run on second node
			 ssh -i stephen.pem 128.84.105.149 -l root $cmd' '${clients[c]}' '${ratios[r]}
			 echo "Running Node 2 "${clients[c]}' clients, readratio: '${ratios[r]}
		 fi
		 sleep ${waitTimes[r]}
	 done
done
