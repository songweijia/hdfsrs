clients=(1 2 4 8 16 32 64)
ratios=(1.0 0.0)
for c in $(seq 0 6)
do
	for r in $(seq 0 1)
	do
		cat ${clients[c]}_${ratios[r]}/temp* >> ${clients[c]}_${ratios[r]}.csv
		rm -f ${clients[c]}_${ratios[r]}/temp*
	done
done
