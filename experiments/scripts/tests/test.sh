#!/bin/bash

# function: copy_file
function copy_file() {
  FSIZE=$1
  TESTNAME=$2

  TMPFILE=data.tmp
  RANDFILE=data.rand
  DUPFILE=data.dup

  dd if=/dev/zero of=$TMPFILE bs=$FSIZE count=1 status=noxfer >/dev/null 2>&1
  openssl enc -aes-256-ctr -pass pass:"$(dd if=/dev/urandom bs=128 count=1 2>/dev/null | base64)" -nosalt < $TMPFILE > $RANDFILE
  hdfs dfs -copyFromLocal $RANDFILE /$RANDFILE
  hdfs dfs -copyToLocal /$RANDFILE ./$DUPFILE
  diff $RANDFILE $DUPFILE
  if [ $? != 0 ]; then
    echo "$TESTNAME ... FAILED"
    exit 1;
  else
    echo "$TESTNAME ... PASSED"
    hdfs dfs -rm -f /$RANDFILE
    rm -rf $TMPFILE $RANDFILE $DUPFILE
  fi
}

# STEP 1
if [ $# != 2 ]; then
  echo "$0 <pagesize> <blocksize>"
  exit 1;
fi

PGS=$1
BKS=$2

# STEP 2
echo "TEST ONE: small read/write"


HALF_=`expr $PGS / 2`
ONE_=$PGS
TWO_=`expr $PGS + $PGS`
TWO_AND_HALF_=`expr $TWO_ + $HALF_`

copy_file $HALF_ "copy-0.5-page"
copy_file $ONE_ "copy-1-page"
copy_file $TWO_ "copy-2-page"
copy_file $TWO_AND_HALF_ "copy-2.5-page"

# STEP 3
echo ""
echo "TEST TWO: block read/write"

HALF_=`expr $BKS / 2`
ONE_=$BKS
TWO_=`expr $BKS + $BKS`
TWO_AND_HALF_=`expr $TWO_ + $HALF_`

copy_file $HALF_ "copy-0.5-block"
copy_file $ONE_ "copy-1-block"
copy_file $TWO_ "copy-2-block"
copy_file $TWO_AND_HALF_ "copy-2.5-block"
