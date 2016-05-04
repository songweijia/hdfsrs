#!/bin/bash

if [ $# != 4 ]; then
  echo "USAGE: $0 <master|org> <block_size> <packet_size> <page_size>"
  echo "block_size = 512K|1M|2M ..."
  echo "packet_size = 8192|16384|32768|65536 ..."
  echo "page_size = 1024|2048|4096|8192 ..."
  exit 1;
fi

EXP=$1
BS=$2
PKS=$3
PGS=$4

#STEP 1 stop existing service
source env.sh
runMaster "stop-yarn.sh"
runMaster "stop-dfs.sh"

#STEP 2 adjust service
runAll "rm -rf ${workspace}hadoop"
runAll "rm -rf /dev/shm/fffs.pg"
runAll "ln -s ${workspace}hadoop-$EXP ${workspace}hadoop"
runAll "rm -rf ${workspace}hadoop/data/dn/* ${workspace}hadoop/data/nn/* ${workspace}hadoop/logs/*"

#STEP 3 prepare configuration
cat $CFG/hdfs-site.xml.$EXP | \
  sed "s/\[BLOCKSIZE\]/$BS/g" | \
  sed "s/\[PACKETSIZE\]/$PKS/g" | \
  sed "s/\[AUTOFLUSHSIZE\]/$PKS/g" | \
  sed "s/\[PAGESIZE\]/$PGS/g" > \
  hdfssitecfg

#STEP 4 upload configuration
for host in ${nodes}
do
  scp hdfssitecfg ${host}:${workspace}hadoop/etc/hadoop/hdfs-site.xml
  scp slaves ${host}:${workspace}hadoop/etc/hadoop/slaves
  scp masters ${host}:${workspace}hadoop/etc/hadoop/masters
done

#STEP 5 star up new service
runMaster hdfs namenode -format
runMaster start-dfs.sh 
runMaster start-yarn.sh
hdfs dfsadmin -allowSnapshot /

#STEP 6 sleep 30 seconds.
sleep 30
echo done...
