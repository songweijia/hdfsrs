#!/bin/bash
export JAVA_HOME=/home/weijia/opt/jdk
export MAVEN_HOME=/opt/maven
export PATH=$PATH:$JAVA_HOME/bin
export CFG=./cfg

nodes=`cat $CFG/masters cfg/slaves cfg/clients`
master=`cat $CFG/masters`
hdfsrs_pkg="hadoop-2.4.1-src/hadoop-dist/target/hadoop-2.4.1.tar.gz"
workspace=/home/weijia/opt/
TESTFILE=timefile

function runMaster(){
  ssh $master "source /etc/profile;$@"
}

function runAll(){
  for n in ${nodes};
  do
    ssh $n "source /etc/profile;$@"
  done
}

function uploadAll(){
  for n in ${nodes};
  do
    scp $1 $n:$2
  done
}

function evictCache(){
  for n in ${nodes};
  do
    ssh $n "/home/weijia/evictcache.sh" 
  done
}

export HADOOP_CLIENT_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
