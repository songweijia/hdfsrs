#!/bin/bash
if [ $# != 2 ]; then
  echo "USAGE $0 <recsize> <dursec>"
  echo "  recsize := size of the record, 256/512 ... bytes"
  echo "  dursec  := duration in seconds, 10/20/30 ... seconds"
  exit -1
fi
source env.sh
hadoop jar FileTester.jar timeappend /$TESTFILE $1 $2 >> latency
hdfs dfs -ls / | grep $TESTFILE | awk '{print $5}' >> filesize
