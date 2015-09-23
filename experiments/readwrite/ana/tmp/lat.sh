#!/bin/bash

for exp in org crtc
do
#  cat ${exp}_65536_dn.out | grep "^sendPacket.\+131072$" | awk '{print $2+$3+$4+$5" "$3+$4" "$3}' > ${exp}.lat
#  cat ${exp}_65536_dn.out | grep "^pkt.\+131072$" | awk '{print $2}' > ${exp}.lat
  cat ${exp}_65536_dn.out | grep "^read.\+131072$" | awk '{print $2}' > ${exp}.lat
done
gnuplot latread.plot
#gnuplot latpkt.plot
scp *.eps root@128.84.105.127:/root/
