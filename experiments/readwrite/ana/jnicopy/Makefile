#LIB=libJNIBlog.so
PROG=JNICopy.class
PROGSRC=JNICopy.java
LIB=libJNICopy.so
SRCS=JNICopy.c
HEADERS=JNICopy.h
CFLAGS=$(DFLAGS) -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux
LDFLAGS=-shared -fpic -pthread

all: $(LIB) ${PROG}

${PROG}: ${PROGSRC}
	javac ${PROGSRC}

$(LIB): $(SRCS) $(HEADERS)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $(LIB) $(SRCS)

clean:
	rm -rf $(LIB) ${PROG}
