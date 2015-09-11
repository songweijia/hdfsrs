#!/bin/bash
PROG=FileTester.jar
SRCP=src

# $1 - path
# $2 - number of emulated PMUs
# $3 - duration in seconds
# $4 - optional: random duration lower bound
function printUsage(){
  echo "Usage:"
  echo "$1 <path> <num_pmu> <recordsize> <duration|upper bound of random duration(sec)> [lower bound of random duration(sec)]"
  echo "example:"
  echo "  path:=/pmudata"
  echo "  num_pmu:100"
  echo "  recordsize:44"
  echo "  duration:10s"
  echo "  lower bound of random duration:5s"
}
# parameter check
if [ $# -lt 4 ]; then
  printUsage $0
  exit -1
fi

DATAPATH=$1
NUMPMU=$2
RECSIZE=$3
DURHI=$4
DURLO=$5

# prepare client applicatioin
if [ -e $SRCP/$PROG ]; then
  cp $SRCP/$PROG .
else
  cd $SRCP
  make
  cd ..
  cp $SRCP/$PROG .
  rm $SRCP/$PROG
fi

# start clients
DUR=$DURHI
for(( i=1 ; i <= $NUMPMU ; i++ ))
do
#  if [ $DURLO ]; then
#   DUR=`expr $RANDOM \% \( $DURHI \- $DURLO \) \+ $DURLO`
#  else
#    DURLO=5
#  fi
  echo "#!/bin/bash" > pmu$i.sh
#  echo "sleep `expr $RANDOM \% $DURLO \+ 1` " >> pmu$i.sh
  echo "sleep `expr $RANDOM \% 4 \+ 1` " >> pmu$i.sh
  if [ $i -eq 1 ]; then
    DUR=$DURHI
  fi
  echo "hadoop jar $PROG pmuwrite $DATAPATH $i $RECSIZE $DUR" >> pmu$i.sh
  chmod +x pmu$i.sh
  nohup ./pmu$i.sh > nohup$i.out &
done
