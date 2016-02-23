#!/bin/bash
for f in \
src/main/native/blog/JNIBlog.c \
src/main/native/blog/JNIBlog.h \
src/main/native/blog/types.h \
src/main/java/edu/cornell/cs/blog/JNIBlog.java \
src/main/java/edu/cornell/cs/blog/IRecordParser.java \
src/main/java/edu/cornell/cs/blog/DefaultRecordParser.java \
src/main/java/org/apache/hadoop/hdfs/server/datanode/MemBlockReceiver.java \
src/main/java/org/apache/hadoop/hdfs/server/datanode/DataXceiver.java \
src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/HLCOutputStream.java \
src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/MemDatasetManager.java
do
cp ../hadoop-2.4.1-src/hadoop-hdfs-project/hadoop-hdfs/$f ./hadoop-hdfs/$f
done
