#!/bin/bash
source env.sh

if [ $# != 1 ]; then
  echo "USAGE $0 <expname>"
  exit -1
fi

EXPNAME=$1

if [ -e $EXPNAME ]; then
  echo "Experiment $EXPNAME already exists...quit."
  exit -1
else
  mkdir $EXPNAME
fi

let i=1
for SP in `hdfs dfs -ls /.snapshot/ | awk '{print $8}'`
do
  TS=`echo $SP | grep -o "[[:digit:]]\+"`
  hdfs dfs -copyToLocal $SP/$TESTFILE $EXPNAME/$TS
  echo `tail -n 1 $EXPNAME/$TS | grep -o "[[:digit:]]\+"`:$TS >> $EXPNAME/result
  rm -rf $EXPNAME/$TS
  let i=i+1
done

mv ./latency $EXPNAME
mv ./filesize $EXPNAME
