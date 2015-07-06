LIB=libJNIBlog.so
SRCS=JNIBlog.c
HEADERS=JNIBlog.h
CFLAGS=-I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux
LDFLAGS=-shared -fpic

$(LIB): $(SRCS) $(HEADERS)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $(LIB) $(SRCS)

clean:
	rm -rf $(LIB)
