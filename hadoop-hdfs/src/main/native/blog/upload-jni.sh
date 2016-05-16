#!/bin/bash
for i in 24 25 26 27 28 29 30 31 32
do
 scp libedu_cornell_cs_blog_JNIBlog.so weijia@compute$i:/home/weijia/opt/hadoop-fffs/lib/native/
done

