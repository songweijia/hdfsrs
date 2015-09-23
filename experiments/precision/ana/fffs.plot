set size 0.5,0.5
set terminal postscript enhanced color dashed lw 1 "Helvetica" 14
set output "fffsp.eps"
set ylabel "T_{snapshot}-T_{data} (ms)"
set xlabel "Time (sec)"
#set logscale y
#set logscale x 10

set arrow from 4,0 to 4,600 nohead front lc rgb "blue" linewidth 0.5 lt 3

plot \
  "fffs.miss" u ($1/1000):2 title "MISSING RECORD" lc rgb "red" pt 2 ps 0.3, \
  "fffs.incon" u ($1/1000):2 title "FALSE RECORD" lc rgb "blue" pt 3 ps 0.3
#  "org_20_60.delta" u ($1/1000):2 title "HDFS" lc rgb "blue" pt 7 ps 0.1

