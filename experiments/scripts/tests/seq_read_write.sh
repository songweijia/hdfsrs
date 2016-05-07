#!/bin/bash
# test sequential read/write performance


tester_jar=FileTester.jar
result_folder=seq_read_write
current_folder=`pwd`
deploy_folder=../deploy/


# STEP 0 checks
if [[ ! -e $tester_jar ]]; then
  echo "$tester_jar not found, check <prj>/experiment/java/"
  exit -1;
fi

cd $deploy_folder
source env.sh
cd $current_folder

# STEP 1 create the result folder
rm -rf $result_folder
mkdir  $result_folder

# STEP 2 function: sequential write 
# seq_read_write_exp master|org folder
function seq_read_write_exp
{
  exp=$1
  result_folder=$2

  for block_size in 64M
  do
    for packet_size in 131072
    do
      for page_size in 4096
      do
      cd $deploy_folder
      ./prepare.sh $exp $block_size $packet_size $page_size
      cd $current_folder

      dd if=/dev/zero of=rawfile bs=$packet_size count=1
      hdfs dfs -copyFromLocal rawfile /rawfile

      # write throughput
      hadoop jar FileTester.jar timeappend /rawfile $packet_size 10 >> $result_folder/wthp_$packet_size

      # wait for the block to be reported to the namenode: very important
      sleep 5

      # read throughput
      hadoop jar FileTester.jar standardRead /rawfile $packet_size 2 >> $result_folder/rthp_$packet_size

      rm -rf rawfile
      done
    done
  done
}

# STEP 3 run exp
for exp in master
do
  mkdir -p $result_folder/$exp
  seq_read_write_exp $exp $result_folder/$exp
done
