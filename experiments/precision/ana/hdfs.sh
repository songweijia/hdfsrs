#!/bin/bash
for time in `cat hdfs.incon | awk '{print $1}' | uniq`
do
  let min=100
  for val in `cat hdfs.incon | grep "^$time " | awk '{print $2}'`
  do
    if [ $min -gt $val ]; then
      min=$val
    fi
  done
  echo $time" "$val
done
