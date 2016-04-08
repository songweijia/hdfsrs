C=gcc
PROG=ds
#CFLAGS=-Wall -g -D PRINT_WAITTIME
CFLAGS=-Wall -g 
LDFLAGS= -lrdmacm -libverbs -lpthread

# all: server client

${PROG}: ${PROG}.o
	$(CC) ${PROG}.o -o ${PROG} $(LDFLAGS)

${PROG}.o:

clean:
	rm -f ${PROG}.o ${PROG}
