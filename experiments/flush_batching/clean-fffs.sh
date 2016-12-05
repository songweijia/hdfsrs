#!/bin/bash

# stop
stop-dfs.sh

# clean
ssh compute30 "rm -rf opt/hadoop/logs/*;rm -rf opt/hadoop/data/nn/*"
ssh compute32 "rm -rf opt/hadoop/logs/*;rm -rf opt/hadoop/data/dn/*;rm -rf /dev/shm/fffs.pg"
ssh compute30 "hdfs namenode -format"

# start
start-dfs.sh
