set size 0.5,0.5
set terminal postscript enhanced color dashed lw 1 "Helvetica" 10
set output "thp.eps"
set ylabel "average sequential write throughput (MB/s)"
set xlabel "packet size(KB)"
set xrange [8:8192]
set yrange [0:500]
set key left
set logscale x 2
plot \
  "thp.dat" u 1:10 title "FFFS10g-wp" with linespoints lc rgb "black" lt 3 lw 1 pt 3 ps 1,\
  "" u 1:9 title "FFFS10g" with linespoints lc rgb "blue" lt 1 lw 1 pt 2 ps 1,\
  "" u 1:8 title "HDFS10g" with linespoints lc rgb "blue" lt 2 lw 1 pt 2 ps 1,\
  "" u 1:5 title "FFFS1g" with linespoints lc rgb "red" lt 1 lw 1 pt 1 ps 1, \
  "" u 1:4 title "HDFS1g" with linespoints lc rgb "red" lt 2 lw 1 pt 1 ps 1
  
# "thp.dat" u 1:7 title "FFFS10g-wp" with linespoints lc rgb "black" lt 1 lw 1 pt 3 ps 1,\
