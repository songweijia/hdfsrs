#!/bin/bash
if [ $# != 1 ]; then
  echo "Usage: $0 <iostat_log>"
  exit 1
fi

AVG=`cat $1 | awk '{SUM=SUM+$7}END{print SUM/NR}'`
P05=`cat $1 | awk '{print $7}' | sort -n | awk '{all[NR] = $0} END{print all[int(NR*0.05)]}'`
P95=`cat $1 | awk '{print $7}' | sort -n | awk '{all[NR] = $0} END{print all[int(NR*0.95 - 0.5)]}'`
echo "$AVG $P05 $P95"
