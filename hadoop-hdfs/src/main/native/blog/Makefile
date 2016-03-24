#LIB=libJNIBlog.so
LIB=libedu_cornell_cs_blog_JNIBlog.so
SRCS=JNIBlog.c InfiniBandRDMA.c
HEADERS=JNIBlog.h map.h types.h InfiniBandRDMA.h
#CFLAGS=$(DFLAGS) -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux -D DEBUG
#CFLAGS=$(DFLAGS) -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux -D PERF_RDMA
CFLAGS=$(DFLAGS) -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux -D USE_GRH
LDFLAGS=-shared -fpic -pthread -libverbs -lrdmacm
TESTLDFLAGS= -lrdmacm -libverbs -lpthread
TESTCFLAGS=-D DEBUG -D USE_GRH
TESTPROG=test
TESTSRC=InfiniBandRDMA.c test.c
#LDFLAGS=-shared -fpic

all: $(LIB) $(TESTPROG)

$(LIB): $(SRCS) $(HEADERS)
	$(CC) $(CFLAGS) -o $(LIB) $(SRCS) $(LDFLAGS) 

$(TESTPROG): $(TESTSRC)
	$(CC) $(TESTCFLAGS) -o $(TESTPROG) $(TESTSRC) $(TESTLDFLAGS)

clean:
	rm -rf $(LIB) $(TESTPROG)
