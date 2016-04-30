OFED_INCLUDE=/usr
OFED_LIBRARY=/lib
C=gcc
PROG=ds
#CFLAGS=-Wall -g -D PRINT_WAITTIME
CFLAGS=-Wall -g -I$(OFED_INCLUDE)
LDFLAGS= -lrdmacm -libverbs -lpthread -L$(OFED_LIBRARY)

# all: server client

${PROG}: ${PROG}.o
	$(CC) ${PROG}.o -o ${PROG} $(LDFLAGS)

${PROG}.o:

clean:
	rm -f ${PROG}.o ${PROG}
