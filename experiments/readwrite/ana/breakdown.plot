set size 0.5,0.5
set terminal postscript enhanced color solid lw 1 "Helvetica" 10
set output "breakdown.eps"
set ylabel "time (us)"
set xlabel "packet size(KB)"
set auto x
#set logscale y 10
set logscale x 2
set key inside left top
nCat=2
set boxwidth 0.35
lof=1.1

plot "breakdown.dat" u ($1/lof):($2+$3+$4) title "Receive Packet" with boxes lc rgb "red" fs pattern 6, \
  "" u ($1/lof):($4+$3) title "Parse" with boxes lc rgb "red" fs solid 0.7, \
  "" u ($1/lof):($4) title "Enqueue" with boxes lc rgb "red" fs pattern 2, \
  "" u ($1*lof):($7+$5+$6) title "Other Costs" with boxes lc rgb "blue" fs pattern 4, \
  "" u ($1*lof):($6+$7) title "Tick HLC" with boxes lc rgb "blue" fs solid 0.7, \
  "" u ($1*lof):($6) title "Copy Data" with boxes lc rgb "blue" fs pattern 5

#plot \
#  "breakdown.dat" u ($1/lof):($2+$3+$4) title "reqw" lc rgb "red" with boxes fill pattern 5, \
#  "" u ($1/lof):($2+$3) title "pars" lc rgb "red" with boxes fill solid 0.7, \
#  "" u ($1/lof):($2) title "recv" lc rgb "red" with boxes fill pattern 0, \
#  "" u ($1*lof):($5+$6+$7) title "copy" lc rgb "blue" with boxes fill pattern 5, \
#  "" u ($1*lof):($5+$7) title "tick" lc rgb "blue" with boxes fill solid 0.7, \
#  "" u ($1*lof):($5) title "entr" lc rgb "blue" with boxes lt -1 fs pattern 4
