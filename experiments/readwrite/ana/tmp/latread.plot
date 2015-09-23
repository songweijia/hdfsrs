set size 0.5,0.5
set terminal postscript enhanced color dashed lw 1 "Helvetica" 10
set output "latread.eps"
set xlabel "points"
#set logscale y 10
set ylabel "read 128K latency(us)"
#set xrange [0:13000]

plot \
  "org.lat" u 1 title "HDFS-read" with lines, \
  "crtc.lat" u 1 title "FFFS-read" with lines
#  "org.lat" u 2 title "HDFS-read+net" with lines, \
#  "crtc.lat" u 2 title "FFFS-read+net" with lines, \
#  "org.lat" u 3 title "HDFS-read" with lines, \
#  "crtc.lat" u 3 title "FFFS-read" with lines
