LIB=libedu_cornell_cs_blog_JNIBlog.so
SRCS=JNIBlog.c
HEADERS=JNIBlog.h map.h types.h
CFLAGS=$(DFLAGS) -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux -Wall -D__STDC_FORMAT_MACROS -DDEBUG
LDFLAGS=-shared -fpic -pthread

all: $(LIB)

$(LIB): $(SRCS) $(HEADERS)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $(LIB) $(SRCS)

clean:
	rm -rf $(LIB)
