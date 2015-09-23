#!/bin/bash
export JAVA_HOME=/home/weijia/opt/jdk
#export MAVEN_HOME=/opt/maven
export PATH=$PATH:$JAVA_HOME/bin

#nodes="fsfs1 fsfs2 fsfs3 fsfs4 fsfs5"
#nodes="fsfs1 fsfs2 fsfs3 fsfs4"
nodes="compute26 compute28 compute29 compute30"
master=compute30
hdfsrs_pkg="hadoop-2.4.1-src/hadoop-dist/target/hadoop-2.4.1.tar.gz"
#workspace=/local/weijia/
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

export HADOOP_CLIENT_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
