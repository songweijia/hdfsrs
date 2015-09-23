#!/bin/bash
source env.sh
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

#for exp in crtc
for exp in org crtc
do
 for bs in 64M
 do
#  for pks in 16384 32768 65536 131072 262144 524288 1048576 2097152 4194304
#  for pks in 16384 32768 65536 131072 262144 524288 1048576 2097152 4194304
#  for pks in 16777216 33554432
  for pks in 65536
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
      for snap in true
      do
#        for idx in 1 2 3 4 5
        for idx in 1 2 3 4 5
        do
#STEP 1 write
          run $exp $bs $pks $pgs $ws $dur $snap > clientlog_${exp}_${bs}_${pks}_${pgs}_${ws}_${dur}_${snap}
#STEP 1.1 evict cat
#          evictCache
#STEP 1.2 pin datanode single core
#          for i in 27 28 29 30; do ssh compute$i /home/weijia/pindn.sh; done
#STEP 2 read
#          hadoop jar FileTester.jar read /timefile > read_${exp}_${idx}
#          hadoop jar FileTester.jar read /timefile >> read_${exp}_${idx}
          evictCache
          spid=`hdfs dfs -ls /.snapshot | grep -o  "[[:digit:]]\+$" | awk '{n++;if(n==40){print $1}}'`
          hadoop jar FileTester.jar read /.snapshot/${spid}/timefile > snapread_${exp}_${idx}
          hadoop jar FileTester.jar read /.snapshot/${spid}/timefile >> snapread_${exp}_${idx}
#          scp weijia@compute29:/home/weijia/opt/hadoop/logs/hadoop-weijia-datanode-compute29.out ./${exp}_${pks}_dn.out
        done
#        scp weijia@compute29:/home/weijia/opt/hadoop/logs/hadoop-weijia-datanode-compute29.log ./${type}_${pks}_dn.log
      done
     done
    done
   done
  done
 done
done
