#!/bin/bash
SRCFILE=TestDFSIO.java
HADOOPSRC=/home/weijia/ws/hadoop-crtc-src
DSTFILE=${HADOOPSRC}/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/fs/TestDFSIO.java
cp  $SRCFILE $DSTFILE
