#This runs the experiment for every number of clients and readwrite ratio
#Clients 1, 2, 4, 8, 16, 32, 64
#Ratios 0, 0.25, 0.5, 0.75, 1.0

clients=(1 2 4 8 16 32 64)
ratios=(1.0 0.75 0.5 0.25 0.0)
waitTimes=(60s 600s 5s 5s 5s)

cmd='sh hdfsrs/experiments/stephen/MultipleFileBandwidthExperiments/single_experiment.sh'

for c in $(seq 5 5)
do
     for r in $(seq 4 5)
	 do
		 #Restart File System and write 1G GB to test.txt
		 echo "Resetting File System"
		 sh hdfsrs/experiments/stephen/MultipleFileBandwidthExperiments/reset.sh ${clients[c]}
		 #Run on first node
		 ssh -i stephen.pem 128.84.105.91 -l root $cmd' '${clients[c]}' '${ratios[r]}' 1'
		 echo "Running Node 1 "${clients[c]}' clients, readratio: '${ratios[r]}
		 if [ $c -gt 0 ]
		 then
		     #Run on second node
			 ssh -i stephen.pem 128.84.105.149 -l root $cmd' '${clients[c]}' '${ratios[r]}' 2'
			 echo "Running Node 2 "${clients[c]}' clients, readratio: '${ratios[r]}
		 fi
		 #sleep ${waitTimes[r]}
	 done
done
