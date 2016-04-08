#!/bin/bash
for host in 29 30
do
  scp ds.c compute$host:/home/weijia/workspace/rdmads/
  scp Makefile compute$host:/home/weijia/workspace/rdmads/
  ssh compute$host "cd workspace/rdmads;make"
done
