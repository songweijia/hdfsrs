#!/bin/bash
function run_test(){
  prefix=$1
  nr_thread=$2
  batch_size=$3
  # restart fffs
  ./clean-fffs.sh
  # start iostat
  ssh compute32 "nohup stdbuf -i0 -e0 -o0 iostat -x -t 1 | stdbuf -i0 -e0 -o0 grep ^sde > tmp/iostat.${batch_size} &"
  # copy
  for((i=1;i<${nr_thread};i++))
  do
    hdfs dfs -copyFromLocal ${prefix}.${i} / &
  done
  hdfs dfs -copyFromLocal ${prefix}.${nr_thread} /
  # kill iostat
  ssh compute32 "kill -9 \`ps -A | grep iostat | awk '{print \$1}'\`"
  # collect result
  scp compute32:tmp/iostat.${batch_size} ./iostat_t${nr_thread}_b${batch_size}
}

#for batch_size in 1 10 20 30 40 60 80 100 200 300 400 500 600 700 800 900 1000
for batch_size in 1 2 3 4 5 6 7 8 9 10 20 30 40 60 80 100
do
  # upload shared library
  for host in compute30 compute31 compute32
  do
    scp batch_${batch_size}.so ${host}:opt/hadoop/lib/native/libedu_cornell_cs_blog_JNIBlog.so
  done
  # test
  run_test 20G 1 ${batch_size}
  run_test 10G 2 ${batch_size}
  run_test 5G  4 ${batch_size}
done
