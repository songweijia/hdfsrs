#!/usr/bin/python

import os, sys, math, string, time
import numpy as np
from mpl_toolkits.mplot3d import axes3d
import matplotlib.pyplot as plt
from matplotlib import cm

    
def waterwave(f1,f2,f3):
#    H_base = np.loadtxt('data/data%s' % fname[fname.rfind('a')+1:])
    H1 = np.loadtxt(f1)
    H2 = np.loadtxt(f2)
    H3 = np.loadtxt(f3)
    m = H1.shape[0]
    x, y = np.meshgrid(np.linspace(-1, 1, m), np.linspace(-1, 1, m), indexing='ij')

    fig = plt.figure(figsize=(28,8),dpi=300)
    ax1 = fig.add_subplot(131, projection='3d')
    ax1.set_zlim(0, 2)
    ax1.plot_surface(x, y, H1, rstride=5, cstride=5, linewidth=0, alpha=0.5, color='white')
    ax1.plot_wireframe(x, y, H1, rstride=5,cstride=5,color='k')
    ax1.set_title('(a) HDFS',y=-0.1)
    ax2 = fig.add_subplot(132, projection='3d')
    ax2.set_zlim(0, 2)
    ax2.plot_surface(x, y, H2, rstride=5, cstride=5, linewidth=0, alpha=0.5, color='white')
    ax2.plot_wireframe(x, y, H2, rstride=5,cstride=5,color='k')
    ax2.set_title('(b) FFFS (server time)',y=-0.1)
    ax3 = fig.add_subplot(133, projection='3d')
    ax3.set_zlim(0, 2)
    ax3.plot_surface(x, y, H3, rstride=5, cstride=5, linewidth=0, alpha=0.5, color='white')
    ax3.plot_wireframe(x, y, H3, rstride=5,cstride=5,color='k')
    ax3.set_title('(c) FFFS (sensor time)',y=-0.1)
#, antialiased=False)
    #ax.grid(False)
    #ax.plot_surface(x, y, H, rstride=1, cstride=1, linewidth=0)#cmap = cm.cool, lntialiased=False)
#    ax.plot_surface(x, y, H_base, rstride=5, cstride=5, linewidth=0, alpha=0.5, color='c')#, antialiased=False)
    
    #ax.contourf(x, y, H_base, zdir='x', offset=-5, color='c')
    #ax.contourf(x, y, H, zdir='x', offset=-5, color='r')
#    ax.view_init(10, 360)
    plt.savefig('image/weijia.eps', bbox_inches='tight')
    
#    delta = 0.0
#    for i in range(m):
#        for j in range(m):
#            delta += math.fabs(H[i][j] - H_base[i][j])
#    print delta

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print 'Usage python plotWave.py <datafile> <datafile> <datafile>'
        exit(1)

    waterwave(sys.argv[1],sys.argv[2],sys.argv[3])

