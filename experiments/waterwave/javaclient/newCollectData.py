import os, sys, math, string, time
from struct import pack

m = 100;
n = 100;
mstep = 10;
nstep = 10;
timesteps = 100;      

def genData():
    # init the height matrix
    for i in range(0, m, mstep):
        for j in range(0, n, nstep):
            ei = i + mstep;
            ej = j + nstep;
            out = open('tdata/data_%d_%d_%d_%d' % (i, j, ei, ej), 'wb')
            for t in range(0, timesteps):
                handle = open('data/data%d'%t, 'r')
                for ti in range(0, i):
                    line = handle.readline()
                for ti in range(i, ei):
                    line = handle.readline()
                    items = line.split()
                    for tj in range(j, ej):
                        out.write(pack("!f", float(items[tj])))
                handle.close()
            out.close()

if __name__ == "__main__":
    genData()
