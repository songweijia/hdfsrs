#!/usr/bin/zsh

if [ $# -lt 2 ]; then
  echo "Usage:"
  echo "$0 <type:=breakdown> <datafolder>"
  echo "$0 <type:=throughput> <datafolder>"
  exit -1
fi

type=$1

# $1 - datafolder
function getbreakdown(){
  dp=$1
  echo "pksz recv pars reqw entr copy tick"
#  for pksz in 16384 32768 65536 131072 262144 524288 1048576 2097152 4194304; do
  for pksz in 16384 32768 65536 131072 262144 524288 1048576 2097152 4194304 8388608 16777216 33554432; do
    cat $dp/hdfs_${pksz}_dn.log | grep "EXP0911" | awk '{print $6" "$7" "$8" "$9}' | grep " ${pksz}\$" >> thd1.out
    cat $dp/hdfs_${pksz}_dn.out | grep " ${pksz}\$" >> thd2.out
    recv=`cat thd1.out | awk '{ sum += $1; n++ } END { if (n > 0) print sum / n; }'`
    pars=`cat thd1.out | awk '{ sum += $2; n++ } END { if (n > 0) print sum / n; }'`
    reqw=`cat thd1.out | awk '{ sum += $3; n++ } END { if (n > 0) print sum / n; }'`
    entr=`cat thd2.out | awk '{ sum += $1; n++ } END { if (n > 0) print sum / n; }'`
    copy=`cat thd2.out | awk '{ sum += $2; n++ } END { if (n > 0) print sum / n; }'`
    tick=`cat thd2.out | awk '{ sum += $3; n++ } END { if (n > 0) print sum / n; }'`
    (( pkszk= $pksz / 1024 ))
    echo "${pkszk} ${recv} ${pars} ${reqw} ${entr} ${copy} ${tick}"
    rm -rf thd1.out thd2.out
  done
}

# $1 - datafolder
function getthroughput(){
  dp=$1
  for pksz in 16384 32768 65536 131072 262144 524288 1048576 2097152 4194304; do
    cat $dp/wt_crtc_64M_${pksz}_4096_256_60_false;
  done
}

if [[ "$type" == "breakdown" ]]; then
  getbreakdown $2
elif [[ "$type" == "throughput" ]]; then
  getthroughput $2
else
  echo "unknown command:$1"
fi
