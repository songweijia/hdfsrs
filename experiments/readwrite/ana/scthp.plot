set size 0.5,0.5
set terminal postscript enhanced color solid lw 1 "Helvetica" 10
set output "scthp.eps"
set ylabel "Aggregate Throughput(MBps)"
set xlabel "Number of DataNodes"
set style fill pattern
set style histogram errorbars
set style data histograms
#set xtics rotate by -45
set auto x
set yrange [0:4000]
set key inside left top
#set boxwidth 0.35

plot "scthp.dat" u 2:3:4:xtic(1) title "HDFS Write" lc rgb "blue" fs pattern 1,\
              "" u 5:6:7 title "HDFS First Reads" lc rgb "blue" fs pattern 2,\
              "" u 8:9:10 title "HDFS Re-read" lc rgb "blue" fs pattern 4,\
              "" u 11:12:13 title "FFFS Write" lc rgb "red" fs pattern 5,\
              "" u 14:15:16 title "FFFS Read" lc rgb "red" fs pattern 6
