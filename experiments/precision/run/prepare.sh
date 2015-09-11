#!/bin/bash

if [ $# != 4 ]; then
  echo "USAGE: $0 <org|rtc|crtc> <block_size> <packet_size> <page_size>"
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
runAll "ln -s ${workspace}hadoop-$EXP ${workspace}hadoop"
rm -rf ${workspace}hadoop
ln -s ${workspace}hadoop-$EXP ${workspace}hadoop
runAll "rm -rf ${workspace}hadoop/data/dn/* ${workspace}hadoop/data/nn/* ${workspace}hadoop/logs/*"

#STEP 3 prepare and upload config files
cat hdfs-site.xml.$EXP | \
sed "s/\[BLOCKSIZE\]/$BS/g" | \
sed "s/\[PACKETSIZE\]/$PKS/g" | \
sed "s/\[PAGESIZE\]/$PGS/g" > \
hdfssitecfg
for host in ${nodes}
do
  scp hdfssitecfg $host:${workspace}hadoop/etc/hadoop/hdfs-site.xml
  scp slaves $host:${workspace}hadoop/etc/hadoop/slaves
done
rm hdfssitecfg
scp hdfssitecfg compute31:${workspace}hadoop/etc/hadoop/hdfs-site.xml

#STEP 4 star up new service
runMaster hdfs namenode -format
runMaster start-dfs.sh 
runMaster start-yarn.sh
hdfs dfsadmin -allowSnapshot /

#STEP 5 prepare the test file
date > $TESTFILE
echo $EXP >> $TESTFILE
hdfs dfs -copyFromLocal $TESTFILE /
rm $TESTFILE
