set size 0.5,0.5
set terminal postscript enhanced color dashed lw 1 "Helvetica" 10
set output "cdf.eps"
set xlabel "abs(T_{snapshot}-T_{data}) (ms)"
set ylabel "Cumulative Frequency"
set logscale x 10

plot \
  "crtc_20_60.cdf" u 1:2 title "FFFS" with lines lc rgb "red", \
  "org_20_60.cdf" u 1:2 title "HDFS" with lines lc rgb "blue"
