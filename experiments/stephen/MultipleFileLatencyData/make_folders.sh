clients=(1 2 4 8 16 32 64)
ratios=(1.0 0.0)
for c in $(seq 0 6)
do
	for r in $(seq 0 1)
	do
		mkdir ${clients[c]}'_'${ratios[r]}
	done
done
