#!/bin/bash
let start=$1
len=50

for((i=0;i<$len;i++))
do
  rm -rf tmp
  mkdir tmp
  for f in `hdfs dfs -ls /.snapshot/${start}/benchmarks/TestDFSIO/io_data| awk '{print $8}'`
  do
    hadoop jar FileTester.jar read $f
  done

# hdfs dfs -copyToLocal /.snapshot/$start/benchmarks/TestDFSIO/io_data/* tmp
#  hdfs dfs -copyToLocal /.snapshot/$i/benchmarks/TestDFSIO/io_control/* tmp
  let start=$start+1
done
