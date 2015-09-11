#!/bin/bash

ST=`head -n 1 $1 | awk '{print $2}'`

cat $1 | awk -v st=$ST '{ print $2-st+1" "$3}'
