#!/usr/bin/zsh
TMPFILE=tmp

cat $1 | awk 'function abs(x){return ((x < 0.0) ? -x : x)} {print abs($1)}' | sort -n > $TMPFILE.1
let cnt=`cat $1 | wc -l`
for ((i=1; i <= $cnt; i++))
do
  echo $(($i*1.0/cnt)) >> $TMPFILE.2
done

paste $TMPFILE.1 $TMPFILE.2
rm $TMPFILE.1 $TMPFILE.2
