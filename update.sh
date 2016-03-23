#!/bin/bash
HADOOP_DIR=../hadoop-2.4.1-src

BLOGJAVA=./hadoop-hdfs/src/main/java/edu/cornell/cs/blog
BLOGC=./hadoop-hdfs/src/main/native/blog
DNIMPL=./hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl
DNAPI=./hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset
DN=./hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode
TESTDNIMPL=./hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl
TESTDN=./hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode


FILES=" \
$BLOGJAVA/JNIBlog.java \
$BLOGC/JNIBlog.h \
$BLOGC/JNIBlog.c \
$BLOGC/types.h \
$BLOGC/map.h \
$BLOGC/Makefile \
$DNIMPL/MemDatasetImpl.java \
$DNIMPL/MemDatasetManager.java \
$DNIMPL/FsDatasetImpl.java \
$DNAPI/FsDatasetSpi.java \
$DN/MemBlockReceiver.java \
$DN/BlockReceiver.java \
$TESTDNIMPL/TestWriteToReplica.java \
$TESTDN/SimulatedFSDataset.java \
"

for f in $FILES
do
  cp $HADOOP_DIR/hadoop-hdfs-project/$f $f
done
