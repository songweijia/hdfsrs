#!/bin/bash

echo "This script is deprecated by a new java tool, which is much faster. Run hadoop FileTester.jar analyzesnap \$DFPATH."

#DFPATH=/benchmarks/TestDFSIO/io_data
DFPATH=/pmudata
#GARBAGE_PREFIX=test_io_
GARBAGE_PREFIX=pmu
# Control: the default data pattern is like
# 1441256756825 PPPPPP...
#DATA_PATTERN="[[:digit:]]\ [[:digit:]]"
DATA_PATTERN="[[:digit:]]\ PP"

# OUTPUT: file_name snapshot actual_cut
# STEP 0 clean up
rm -rf $GARBAGE_PREFIX.* *.latest
# STEP 1 get file list
for f in `hdfs dfs -ls $DFPATH | awk '{print $8}'`
do
  arr=(`echo $f | tr "/" "\n"`)
  fn=${arr[-1]}
  #1 copy the latest to local
  hdfs dfs -copyToLocal $DFPATH/$fn $fn.latest 2> /dev/null
  if [ $? == 1 ]; then
    echo "ERROR:cannot copyfile from hdfs: $DFPATH/$fn"
    exit 1;
  fi
  start_ts=`head $fn.latest -n 1 | awk '{print $1}'`
  end_ts=`tail $fn.latest -n 1 | awk '{print $1}'`
  for s in `hdfs dfs -ls /.snapshot | awk '{print $8}'`
  do
    arr=(`echo $s | tr "/" "\n"`)
    sn=${arr[1]}
    if [ $sn -gt `expr $end_ts \+ 1000` -o $sn -lt $start_ts ];then
      continue
    fi
    actual_ts=$sn
    #3 copy the snapshotted to local
    hdfs dfs -copyToLocal $s/$DFPATH/$fn 2> /dev/null

    if [ $? -eq 1 ]; then # file does not appear in the directory
      actual_ts=$start_ts
    else # file does appear in the directory, calculate the latest timestamp.
      actual_ts=`tail $fn -n 2 | grep "$DATA_PATTERN" | tail -n 1 | awk '{print $1}'`
      rm -rf $fn
    fi

    if [ -z $actual_ts ]; then
      actual_ts=$start_ts
    fi
    #4 echo
#    echo $fn $sn $end_ts $actual_ts `expr $actual_ts \- $sn`
    echo $fn $sn $end_ts $actual_ts `expr $sn \- $actual_ts`
    if [ $actual_ts -eq $end_ts ]; then
      break
    fi
  done
  rm -rf $fn.latest
done

