import os, sys, math, string, time
import numpy as np
from mpl_toolkits.mplot3d import axes3d
import matplotlib.pyplot as plt
from matplotlib import cm

    
def waterwave(fname):
    H = np.loadtxt(fname)
    m = H.shape[0]
    x, y = np.meshgrid(np.linspace(-1, 1, m), np.linspace(-1, 1, m), indexing='ij')

    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    ax.set_zlim(-2, 5)
    ax.grid(False)
    ax.plot_surface(x, y, H, rstride=1, cstride=1, linewidth=0)#cmap = cm.cool, lntialiased=False)

    plt.savefig('image/%s.png' % fname[fname.rfind('a')+1:])
    
        
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print 'Usage python plotWave.py <datafile>'
        exit(1)

    waterwave(sys.argv[1])

