LIB=libedu_cornell_cs_blog_JNIBlog.so
SRCS=JNIBlog.c InfiniBandRDMA.c
HEADERS=JNIBlog.h map.h types.h InfiniBandRDMA.h
#CFLAGS=$(DFLAGS) -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux -D DEBUG
#CFLAGS=$(DFLAGS) -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux -D PERF_RDMA -DPERSISTENT
#CFLAGS=$(DFLAGS) -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux -D PERF_RDMA -D DEBUG
#CFLAGS=$(DFLAGS) -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -DDEBUG
CFLAGS=$(DFLAGS) -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS
LDFLAGS=-shared -fpic -pthread -libverbs -lrdmacm
TESTLDFLAGS= -lrdmacm -libverbs -lpthread
#TESTCFLAGS=-D DEBUG -D USE_GRH
TESTCFLAGS=-D DEBUG
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
