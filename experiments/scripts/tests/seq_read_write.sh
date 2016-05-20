#!/bin/bash
# test sequential read/write performance


tester_jar=FileTester.jar
result_folder=seq_read_write
current_folder=`pwd`
deploy_folder=../deploy/
number_loop=5


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
  cfg=$2
  r_folder=$3

  for block_size in 64M # block size
  do
    for page_size in 4096 # page size
    do

      for((i=0;i<$number_loop;i++))
      do
        # write throughput
        for packet_size in 1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576 2097152 4194304
        do
        cd $deploy_folder
        ./prepare.sh $exp $cfg $block_size $packet_size $page_size
        cd $current_folder

        dd if=/dev/zero of=rawfile bs=$packet_size count=1
        hdfs dfs -copyFromLocal rawfile /rawfile
        rm -rf rawfile

        hadoop jar FileTester.jar timeappend /rawfile $packet_size 10 >> $r_folder/wthp_$packet_size

        # read throughput
        if [[ $cfg == 'rdma' ]]; then
          hadoop jar FileTester.jar zeroCopyRead /rawfile $packet_size 2 >> $r_folder/rthp_$packet_size
        else
          hadoop jar FileTester.jar standardRead /rawfile $packet_size 2 >> $r_folder/rthp_$packet_size
        fi
      done

      done
    done
  done
}

# STEP 3 run exp

# - FFFS TCP
mkdir -p $result_folder/fffs.tcp
seq_read_write_exp fffs tcp $result_folder/fffs.tcp

# - FFFS RDMA
# mkdir -p $result_folder/fffs.rdma
# seq_read_write_exp fffs rdma $result_folder/fffs.rdma

# - HDFS ORG
# mkdir -p $result_folder/hdfs.org
# seq_read_write_exp hdfs org $result_folder/hdfs.org
#mkdir -p $result_folder/hdfs.nocache
#seq_read_write_exp hdfs nocache $result_folder/hdfs.nocache


