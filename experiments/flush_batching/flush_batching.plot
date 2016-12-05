set size 0.5,0.5
set terminal postscript enhanced color dashed lw 1 "Helvetica" 10
set output "flush_batching.eps"
set xrange [0:110]
set yrange [0:400]
set xlabel "Batch size ( # of log entries per flush )"
set ylabel "Disk I/O Throughput(MBps)"
set arrow from 0,216.5 to 110,216.5 nohead front lc rgb "gray" linewidth 1 lt 3
set label "Avergae raw SSD throughput is 216.5MB/s." at 20,230 textcolor "gray"

plot \
  "flush_batching_1.dat" u 1:($2/1000) title "1 writer"  with lines lc rgb "red", \
  "flush_batching_2.dat" u 1:($2/1000) title "2 writers" with lines lc rgb "blue", \
  "flush_batching_4.dat" u 1:($2/1000) title "4 writers" with lines lc rgb "black", \
  "flush_batching_1.dat" u 1:($2/1000):($3/1000):($4/1000) notitle with errorbars lc rgb "red" ps 0.5 lw 0.5, \
  "flush_batching_2.dat" u 1:($2/1000):($3/1000):($4/1000) notitle with errorbars lc rgb "blue" ps 0.5 lw 0.5, \
  "flush_batching_4.dat" u 1:($2/1000):($3/1000):($4/1000) notitle with errorbars lc rgb "black" ps 0.5 lw 0.5
