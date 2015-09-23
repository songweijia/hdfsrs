%.pdf: %.eps
	epstopdf $<
%.eps: %.plot
	gnuplot $<

all: breakdown.pdf breakdownp.pdf thp.pdf

#clean:
#	rm -rf *.eps *.pdf
