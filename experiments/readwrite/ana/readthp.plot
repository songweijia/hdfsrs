set size 0.5,0.5
set terminal postscript enhanced color solid lw 1 "Helvetica" 10
set output "readthp.eps"
set ylabel "Read Throughput(MBps)"
set style fill pattern
set style histogram errorbars
set style data histograms
#set xtics rotate by -45
set auto x
set yrange [0:1200]
set key inside right top
#set boxwidth 0.35

plot "readthp.dat" u 2:3:4:xtic(1) title "First Reads" lc rgb "red" fs pattern 4,\
     "" u 5:6:7 title "Re-reads" lc rgb "red" fs pattern 5, \
     "" u 8:9:10 title "First Reads(Snapshot)" lc rgb "blue" fs pattern 1, \
     "" u 11:12:13 title "Re-reads(Snapshot)" lc rgb "blue" fs pattern 2
