#!/bin/bash
#hadoop jar /home/weijia/opt/hadoop-crtc/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.4.1-tests.jar TestDFSIO $@
./prepare.sh crtc 64M 65536 4096
time hadoop jar /home/weijia/opt/hadoop-crtc/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.4.1-tests.jar TestDFSIO -write -nrFiles 10 -size 100MB
#sleep 10
#hadoop jar FileTester.jar snapshot / 200 500
