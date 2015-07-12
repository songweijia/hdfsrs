# $Id: Makefile,v 1.1 2014/08/05 20:05:37 hweather Exp $

#LATEX=pdflatex -output-format=dvi
LATEX=pdflatex
DVIPS=dvips -Ppdf -t letter
PS2PDF=ps2pdf -sDEVICE=pdfwrite -sPAPERSIZE=letter -dPDFSETTINGS=/prepress -dEmbedAllFonts=true -dSubsetFonts=true -dMaxSubsetPct=100

BIBTEX=bibtex
PAPER=exp

TEXFILES = $(wildcard *.tex)
EPSFIGS=$(wildcard figures/*.eps)
PDFFIGS=$(wildcard figures/*.pdf)


all: $(PAPER).pdf

bib:
	$(BIBTEX) $(PAPER)

$(PAPER).pdf: $(EPSFIGS) $(PDFFIGS) $(TEXFILES) exp.bib
	$(LATEX) $(PAPER)
	$(BIBTEX) $(PAPER)
	$(LATEX) $(PAPER)
	$(LATEX) $(PAPER)
#	$(DVIPS) $(PAPER).dvi
#	$(PS2PDF) $(PAPER).ps

clean:
	/bin/rm -f *.dvi *.aux $(PAPER).ps $(PAPER).pdf *~ *.log *.lot *.lof *.toc *.blg *.bbl *.out *.synctex.gz
