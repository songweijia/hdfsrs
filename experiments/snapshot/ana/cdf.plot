set size 0.5,0.5
set terminal postscript enhanced color dashed lw 1 "Helvetica" 10
set output "snapcdf.eps"
set xlabel "The Time Cost of Snapshot Creation (ms)"
set ylabel "Cumulative Frequency"
set logscale x 10

plot \
  "crtc.cdf" u 1:2 title "FFFS" with lines lc rgb "red", \
  "org.cdf" u 1:2 title "HDFS" with lines lc rgb "blue"
