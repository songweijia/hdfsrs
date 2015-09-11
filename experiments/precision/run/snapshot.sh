#!/bin/bash
PROG=FileTester.jar
SRCP=src
NSNAP=600

if [ $# -lt 1 ]; then
  echo "Usage: $0 <duration>"
  exit -1
fi

hadoop jar $PROG snapshot / `expr $1 \* 1000 \/ $NSNAP` $NSNAP
