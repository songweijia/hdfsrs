#!/bin/bash
rm -rf bin/*
javac -encoding utf-8 -classpath /home/ubuntu/opt/hadoop/share/hadoop/hdfs/hadoop-hdfs-2.4.1.jar:/home/ubuntu/opt/hadoop/share/hadoop/common/hadoop-common-2.4.1.jar:/home/ubuntu/opt/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar -d bin src/*.java
jar -cvf Client.jar -C bin/ .
