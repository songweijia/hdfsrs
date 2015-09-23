#!/bin/bash


function run () {
  #type = crtc org
  fst=$1
  # block size 64M
  bs=$2
  # packet size 65536
  pks=$3
  # page size
  pgs=$4
  # file size 100M
  fs=$5
  # number of file
  nf=$6
  # buffersize 256
  bufs=$7
  # snapshot interval in ms
  sint=$8
  #number of snapshots
  ns=$9

  ./prepare.sh $fst $bs $pks $pgs
  echo "./runTestDFSIO.sh -write -nrFiles $nf -size $fs -bufferSize $bufs"
  echo "hadoop jar clientsrc/FileTester.jar snapshot / $sint $ns"
  echo "./collect_precision.sh > precision_$fs"
#  wait $writePID
#  wait $snapshotPID
#  ./collect_precision.sh > percision_fs
}

run crtc 64M 65536 4096 64MB 128 256 50 600
#run crtc 64M 65536 4096 32MB 64 256 50 600
#run crtc 64M 65536 4096 64MB 50 256 50 1000
#run crtc 64M 65536 4096 128MB 25 256 50 1000
#run crtc 64M 65536 4096 256MB 16 256 50 800
#run crtc 64M 65536 4096 512MB 8 256 50 600
#run crtc 64M 65536 4096 1024MB 2 256 50 600

