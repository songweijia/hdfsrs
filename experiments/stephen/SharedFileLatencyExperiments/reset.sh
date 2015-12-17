#!/bin/bash
stop-dfs.sh
rm -rf /opt/hadoop/data/dn
rm -rf /opt/hadoop/data/nn
rm -rf /opt/hadoop/logs
hdfs namenode -format
start-dfs.sh

#Write 1024 GB to file
hadoop jar FFFS_MEng/SingleFileLatencyExperiments/SingleFileLatency.jar setup 1024