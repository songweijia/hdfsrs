#!/bin/bash
for i in 27 28 29 30 31
do
 scp libedu_cornell_cs_blog_JNIBlog.so weijia@compute$i:/home/weijia/opt/hadoop-master/lib/native/
done

