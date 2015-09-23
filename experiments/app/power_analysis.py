import math, string, time
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Ellipse
import pylab

def setUp():
    fontsize = 20
    settings = {'backend': 'eps',
            'axes.labelsize': fontsize,
            'text.fontsize': fontsize,
            'xtick.labelsize': fontsize,
            'ytick.major.pad': 10,
            'ytick.labelsize': fontsize,
            'legend.fontsize': fontsize,
            'font.size': fontsize}
    pylab.rcParams.update(settings)

def m_plot(results):
    setUp()
    
    n = 10
    ticks = [i * 30 for i in range(n)]
    ticks[0] = 1
    plt.figure()
    color_list = ['g', 'y', 'w', 'c', ]
    keys = ['FFFS_first_read', 'FFFS_without_randomwrite', 'HDFS_filter']
    legends = ['FFFS', 'FFFS without random write', 'HDFS']
    handles = []
    index = np.arange(n) + 1.0 / ((len(keys)+1) * 2)
    width = 1.0 / (len(keys) + 1)
    for i in range(len(keys)):
        res = results[keys[i]]
        handle = plt.bar(index + i * width, [mean for (mean, std) in res], width, color = color_list[i], ecolor='k', yerr=[std for (mean, std) in res])
        handles.append(handle[0])

    plt.ylabel('Job Execute Time(s)')
    plt.xticks(index + len(keys)*width/2, ticks)
    plt.xlabel('Time (min)')
    plt.ylim(0, 40)
    plt.legend(handles, legends, loc=2, borderaxespad=0.)
    plt.grid(False)
    #plt.ylim(0,3500)
    
    def autolabel(rects):
        for rect in rects:
            height = rect.get_height()
            plt.text(rect.get_x() + rect.get_width()/2., height + 3, '%.1f'%(height), ha='center', va='bottom')

    plt.savefig("job_time.eps")

def analysis_new(file):
    handle = open(file)
    result = [[],[]]
    start = 0
    step = 6
    r0 = []
    r1 = []
    for line in handle:
        ts = line.split()
        r0.append(string.atof(ts[0])/1000)
        for l in range(1,len(ts)):
            r1.append(string.atof(ts[l])/1000)
        start += 1
        if start == step:
            mean0 = sum(r0)/len(r0)
            std0 = math.sqrt(sum([t**2 for t in r0])/len(r0) - mean0*mean0)
            result[0].append((mean0, std0))
            mean1 = sum(r1)/len(r1)
            std1 = math.sqrt(sum([t**2 for t in r1])/len(r1) - mean1*mean1)
            result[1].append((mean1, std1))
            start = 0
            r0 = []
            r1 = []
            
    return result

def analysis_old(file):
    handle = open(file)
    result = []
    for line in handle:
        ts = line.split()
        l = []
        for i in range(len(ts)):
            l.append(string.atof(ts[i])/1000)
        mean = sum(l)/len(l)
        std = math.sqrt(sum([t**2 for t in l])/len(l) - mean*mean)
        result.append((mean, std))
    return result

def run():
    results = {}
    res = analysis_new('ttlog')
    results['FFFS_first_read'] = res[0]
    results['FFFS_second_read'] = res[1]
    results['FFFS_without_randomwrite'] = analysis_old('nonrandom')
    results['HDFS_filter'] = analysis_old('timelog')
    m_plot(results)

if __name__ == "__main__":
    run()
