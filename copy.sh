#!/bin/bash
HADOOP_DIR=../hadoop-2.4.1-src
rm -rf $HADOOP_DIR/hadoop-common-project/hadoop-annotations;cp -r hadoop-annotations $HADOOP_DIR/hadoop-common-project/;
rm -rf $HADOOP_DIR/hadoop-common-project/hadoop-auth;cp -r hadoop-auth $HADOOP_DIR/hadoop-common-project/;
rm -rf $HADOOP_DIR/hadoop-common-project/hadoop-common;cp -r hadoop-common $HADOOP_DIR/hadoop-common-project/;
rm -rf $HADOOP_DIR/hadoop-common-project/hadoop-minikdc;cp -r hadoop-minikdc $HADOOP_DIR/hadoop-common-project/;
rm -rf $HADOOP_DIR/hadoop-hdfs-project/hadoop-hdfs; cp -r hadoop-hdfs $HADOOP_DIR/hadoop-hdfs-project/;
rm -rf $HADOOP_DIR/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient;cp -r hadoop-mapreduce-client-jobclient $HADOOP_DIR/hadoop-mapreduce-project/hadoop-mapreduce-client/;
