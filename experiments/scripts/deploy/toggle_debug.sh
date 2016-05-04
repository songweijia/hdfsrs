#!/bin/bash
#~/bin/bash

# 1) test parameters
if [ $# != 1 ]; then
  echo "Toggle debug messages output."
  echo "$0 <on|off>"
  exit -1
fi
DEBUG=$1

# 2) load environments
source env.sh

# 3) toggle client debug settings
if [ $DEBUG == 'on' ]; then
  runAll sed -i '/hadoop.root.logger=/c\HADOOP_OPTS=\"\$HADOOP_OPTS\ -Dhadoop.root.logger=\${HADOOP_ROOT_LOGGER\:-DEBUG\,RFA}\"'  ${workspace}/hadoop/libexec/hadoop-config.sh
else
  runAll sed -i '/hadoop.root.logger=/c\HADOOP_OPTS=\"\$HADOOP_OPTS\ -Dhadoop.root.logger=\${HADOOP_ROOT_LOGGER\:-ERROR\,RFA}\"'  ${workspace}/hadoop/libexec/hadoop-config.sh
fi

# 4) toggle server debug settings
if [ $DEBUG == 'on' ]; then
  runAll sed -i '/HADOOP_ROOT_LOGGER=/c\export\ HADOOP_ROOT_LOGGER=\${HADOOP_ROOT_LOGGER\:-\"DEBUG\,RFA\"}' ${workspace}/hadoop/sbin/hadoop-daemon.sh
else
  runAll sed -i '/HADOOP_ROOT_LOGGER=/c\export\ HADOOP_ROOT_LOGGER=\${HADOOP_ROOT_LOGGER\:-\"ERROR\,RFA\"}' ${workspace}/hadoop/sbin/hadoop-daemon.sh
fi
