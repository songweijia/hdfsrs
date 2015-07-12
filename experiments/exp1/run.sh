#!/bin/bash
hdfs dfs -rm  /test
hdfs dfs -copyFromLocal ../env.sh /test
hadoop jar FileTester.jar overwrite /test 100 CONCENTRATE
