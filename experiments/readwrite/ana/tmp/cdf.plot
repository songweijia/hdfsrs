set size 0.5,0.5
set terminal postscript enhanced color dashed lw 1 "Helvetica" 10
set output "cdf.eps"
set xlabel "us"
set ylabel "Cumulative Frequency"
set logscale x 10

plot \
  "org.cdf" u 1:2 title "HDFS" with lines, \
  "crtc.cdf" u 1:2 title "FFFS" with lines

#  "crtc_pkt.cdf" u 1:2 title "FFFS" with lines, \
#  "crtc_read.cdf" u 1:2 title "FFFS-read" with lines, \
#  "crtc_send.cdf" u 1:2 title "FFFS-send" with lines, \
#  "org_pkt.cdf" u 1:2 title "HDFS" with lines, \
#  "org_read.cdf" u 1:2 title "HDFS-read" with lines, \
#  "org_send.cdf" u 1:2 title "HDFS-send" with lines
