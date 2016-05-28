import os, sys, math, string, time
import numpy as np
from mpl_toolkits.mplot3d import axes3d
import matplotlib.pyplot as plt
from matplotlib import cm

    
def waterwave(fname):
    H_base = np.loadtxt('data/data%s' % fname[fname.rfind('a')+1:])
    H = np.loadtxt(fname)
    m = H.shape[0]
    x, y = np.meshgrid(np.linspace(-1, 1, m), np.linspace(-1, 1, m), indexing='ij')

    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    ax.set_zlim(0, 2)
    #ax.grid(False)
    #ax.plot_surface(x, y, H, rstride=1, cstride=1, linewidth=0)#cmap = cm.cool, lntialiased=False)
    #ax.plot_surface(x, y, H_base, rstride=5, cstride=5, linewidth=0, alpha=0.5, color='c')#, antialiased=False)
    ax.plot_surface(x, y, H, rstride=5, cstride=5, linewidth=1, alpha=0.5, color='w')#, antialiased=False)
    
    #ax.contourf(x, y, H_base, zdir='x', offset=-5, color='c')
    #ax.contourf(x, y, H, zdir='x', offset=-5, color='r')
    #ax.plot_wireframe(x, y, H, rstride=1,cstride=1)
    #ax.view_init(10, 360)
    plt.savefig('image/%s.eps' % fname[fname.rfind('a')+1:])
    
    delta = 0.0
    for i in range(m):
        for j in range(m):
            delta += math.fabs(H[i][j] - H_base[i][j])
    print delta

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print 'Usage python plotWave.py <datafile>'
        exit(1)

    waterwave(sys.argv[1])

