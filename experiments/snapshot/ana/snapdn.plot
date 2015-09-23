set size 0.5,0.5
set terminal postscript enhanced color dashed lw 1 "Helvetica" 10
set output "snapdn.eps"
set xlabel "The Number of Snapshots been Touched"
set ylabel "DataNode Memory Usage (MB)"
set yrange [2000:3500]

set arrow from 0,3000 to 500,3000 nohead lw 0.5 lt 2

plot \
  "snapdn.dat" using 1:2 notitle with linespoints pt 1
