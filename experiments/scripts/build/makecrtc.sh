#!/bin/bash
SOURCE_URL=https://archive.apache.org/dist/hadoop/core/hadoop-2.4.1/hadoop-2.4.1-src.tar.gz

#STEP 0 clean
./clean.sh

#STEP 1 download
wget -c $SOURCE_URL
if [ -e hadoop-2.4.1-src.tar.gz ]; then
  tar -xf hadoop-2.4.1-src.tar.gz
else
  echo "download 'hadoop-2.4.1-src.tar.gz' failed."
  exit -1
fi

#STEP 2 clone git repository
git clone https://github.com/songweijia/hdfsrs.git
cd hdfsrs
git checkout CRTC
cd ..

#STEP 3 apply path
for pkg in hadoop-auth hadoop-common hadoop-annotations hadoop-minikdc
do
  rm -rf hadoop-2.4.1-src/hadoop-common-project/$pkg
  cp -r hdfsrs/$pkg hadoop-2.4.1-src/hadoop-common-project/
done

rm -rf hadoop-2.4.1-src/hadoop-hdfs-project/hadoop-hdfs
cp -r hdfsrs/hadoop-hdfs hadoop-2.4.1-src/hadoop-hdfs-project/


rm -rf hadoop-2.4.1-src/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient
cp -r hdfsrs/hadoop-mapreduce-client-jobclient hadoop-2.4.1-src/hadoop-mapreduce-project/hadoop-mapreduce-client/
#STEP 4 make
cd  hadoop-2.4.1-src
mvn package -Pnative -Pdist -Dtar -DskipTests
cd ..
if [ ! -e hadoop-2.4.1-src/hadoop-dist/target/hadoop-2.4.1.tar.gz ]; then
  echo "build failed!"
  exit -1
fi

#STEP update - please implementate your own version

#for i in 27 28 29 30 31
for i in 
do
 scp hadoop-2.4.1-src/hadoop-dist/target/hadoop-2.4.1.tar.gz weijia@compute$i:/home/weijia/opt/
 ssh weijia@compute$i "cd /home/weijia/opt;rm -rf hadoop-2.4.1;tar -xf hadoop-2.4.1.tar.gz"
 ssh weijia@compute$i "cd /home/weijia/opt;rm -rf hadoop-crtc/lib hadoop-crtc/share"
 ssh weijia@compute$i "cd /home/weijia/opt;mv hadoop-2.4.1/lib hadoop-crtc/;mv hadoop-2.4.1/share hadoop-crtc/"
done

