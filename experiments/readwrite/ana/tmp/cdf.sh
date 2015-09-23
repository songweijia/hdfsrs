#!/bin/bash
for exp in org crtc
do
  for dim in pkt send read
  do
    cat ${exp}_65536_dn.out | grep "^${dim}" | grep "131072\$" > ${exp}_${dim}.out
    cat ${exp}_65536_dn.out | grep "^${dim}" | grep "131103\$" >> ${exp}_${dim}.out
    ./getcdf.sh ${exp}_${dim}.out > ${exp}_${dim}.cdf
#    rm ${exp}_${dim}.out
  done
done

gnuplot cdf.plot
scp cdf.eps root@128.84.105.127:/root/
