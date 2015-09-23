set size 0.5,0.5
set terminal postscript enhanced color dashed lw 1 "Helvetica" 14
set output "hdfsp.eps"
set ylabel "T_{snapshot}-T_{data} (ms)"
set xlabel "Time (sec)"
#set logscale y
#set logscale x 10
set key bottom

#set arrow from 6,0 to 6,400 nohead front lc rgb "blue" linewidth 0.5 lt 3

plot \
  "hdfs.new.incon" u ($1/1000):2 notitle lc rgb "blue" pt 3 ps 0.3 with filledcurve y1=0, \
  "hdfs.new.incon" u ($1/1000):2 title "FALSE RECORD" lc rgb "blue" pt 2 ps 0.3
