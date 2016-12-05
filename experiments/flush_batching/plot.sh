#!/bin/bash
for f in `ls *.plot`
do
    gnuplot $f
done

for f in `ls *.eps`
do
    epspdf -b $f
done
