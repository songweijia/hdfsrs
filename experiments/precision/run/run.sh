#!/bin/bash

#for type in crtc org
for type in org
do
  ./prepare.sh $type 64M 65536 4096
  sleep 5
  ./startclients.sh /pmudata 20 44 60
  sleep 1
  ./snapshot.sh 60
  #collect
  #hadoop jar src/FileTester.jar analyzesnap /pmudata > ${type}_20_60.dat
  ./clean.sh
done
