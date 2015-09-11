set size 0.5,0.5
set terminal postscript enhanced color dashed lw 1 "Helvetica" 10
set output "newsnapshot.eps"
set xlabel "T_{snapshot}-T_{data} (ms)"
set ylabel "Cumulative Frequency"
#set logscale x 10

plot \
  "newcdf.dat" u 1:2 title "new" with lines
