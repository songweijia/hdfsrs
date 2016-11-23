#!/bin/bash
testfile=/test
snaplog=snaplog
num_snapshot=5
num_writepersnapshot=1000
reverse=False
output=snap_500_forward.dat

echo "STEP 1: Begin write..."
# snapwrite <filename> <filesize(MB)> <writesize(Byte)> <interval(cnt)> <interval(ms)> <snapcount>
hadoop jar FileTester.jar snapwrite ${testfile} 64 32768 ${num_writepersnapshot} ${num_writepersnapshot} ${num_snapshot} > ${snaplog}
echo "end..."

echo "STEP 2: Begin read..."
hadoop jar FileTester.jar snapread ${snaplog} ${reverse} > ${output}
