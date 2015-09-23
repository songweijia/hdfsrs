#!/bin/bash

# $1 - type
# $2 - block size
# $3 - paket size
# $4 - page size
# $5 - write size
# $6 - duration sec
# $7 - snapshot?
function run(){
  EXP=$1
  BS=$2
  PKS=$3
  PGS=$4
  WS=$5
  DUR=$6
  SNAP=$7
  #prepare
  ./prepare.sh $EXP $BS $PKS $PGS
  #run
  hadoop jar Exp1.jar timeappend /timefile $WS $DUR $SNAP
  #collect
  BYTES=`hdfs dfs -ls / | grep timefile | awk '{print $5}'`
  expr $BYTES \/ $DUR > wt_${EXP}_${BS}_${PKS}_${PGS}_${WS}_${DUR}_${SNAP}
}

for exp in org crtc
# for exp in crtc
do
 for bs in 64M
 do
#  for pks in 16384 32768 65536 131072 262144 524288 1048576 2097152 4194304
#  for pks in 16384 32768 65536 131072 262144 524288 1048576 2097152 4194304
  for pks in 8388608 16777216 33554432
#  for pks in 65536
#  for pks in 16384 32768 65536 131072 262144 524288 1048576 2097152 4194304
#  for pks in 32768 131072 524288 2097152
  do
   for pgs in 4096
   do
#    for ws in 256 1048576
    for ws in 256
    do
     for dur in 60
     do
#      for snap in true false
      for snap in false
      do
        run $exp $bs $pks $pgs $ws $dur $snap > clientlog_${exp}_${bs}_${pks}_${pgs}_${ws}_${dur}_${snap}
        scp weijia@compute29:/home/weijia/opt/hadoop/logs/hadoop-weijia-datanode-compute29.out ./hdfs_${pks}_dn.out
        scp weijia@compute29:/home/weijia/opt/hadoop/logs/hadoop-weijia-datanode-compute29.log ./hdfs_${pks}_dn.log
      done
     done
    done
   done
  done
 done
done
