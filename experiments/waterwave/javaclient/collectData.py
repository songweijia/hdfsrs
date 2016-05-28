import os, sys, math, string, time
import numpy as np

m = 100;
timesteps = 2000;      
itemstep = 10;

def genData():
    # init the height matrix
    for i in range(0, m, itemstep):
        for j in range(0, m, itemstep):
            ei = i + itemstep;
            ej = j + itemstep;
            out = open('tdata/data_%d_%d_%d_%d' % (i, j, ei, ej), 'w')
            for t in range(1, timesteps+1):
                handle = open('data/data%d'%t, 'r')
                for ti in range(0, i):
                    line = handle.readline()
                for ti in range(i, ei):
                    line = handle.readline()
                    items = line.split()
                    for tj in range(j, ej):
                        out.write(items[tj] + " ")
                handle.close()
                out.write("\n")
            out.close()
               

if __name__ == "__main__":
    genData()
