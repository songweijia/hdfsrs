set size 0.5,0.5
set terminal postscript enhanced color dashed lw 1 "Helvetica" 10
set output "delta.eps"
set ylabel "T_{snapshot}-T_{data} (ms)"
set xlabel "Time (sec)"
#set logscale y
#set logscale x 10

# set arrow from 6,0 to 6,400 nohead front lc rgb "blue" linewidth 0.5 lt 3

plot \
  "crtc_20_60.delta" u ($1/1000):2 title "FFFS" lc rgb "red" pt 2 ps 0.2, \
  "org_20_60.delta" u ($1/1000):2 title "HDFS" lc rgb "blue" pt 7 ps 0.1

