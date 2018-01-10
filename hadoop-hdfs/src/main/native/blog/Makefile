LIB=libedu_cornell_cs_blog_JNIBlog.so
SRCS=JNIBlog.cc InfiniBandRDMA.cc LibFabricRDMA.cc
HEADERS=JNIBlog.h map.h types.h InfiniBandRDMA.h LibFabricRDMA.h bitmap.h
# CC=g++ -O3 -std=c++14
CC=g++ -std=c++14
CPPFLAGS=$(DFLAGS) -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -DFLUSH_BATCHING -DDEBUG -I/home/weijia/workspace/libfabric/build4/include/
LDFLAGS=-shared -fpic -pthread -libverbs -lrdmacm  -L/home/weijia/workspace/libfabric/build4/lib -lfabric
TESTLDFLAGS=-libverbs -pthread -lrdmacm -L/home/weijia/workspace/libfabric/build4/lib -lfabric
# TESTCPPFLAGS=-I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux -I/home/weijia/workspace/libfabric/build4/include/ -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -DFLUSH_BATCHING -DDEBUG -ggdb
TESTCPPFLAGS=-I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux -I/home/weijia/workspace/libfabric/build4/include/ -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -DFLUSH_BATCHING -O3
TESTSRC=LibFabricRDMA.cc InfiniBandRDMA.cc test.cc
#TESTSRC=InfiniBandRDMA.c test.c
TESTPROG=test

all: $(LIB)

tests: $(TESTPROG)

$(LIB): $(SRCS) $(HEADERS)
	$(CC) $(CPPFLAGS) $(LDFLAGS) $(SRCS) -o $@

$(TESTPROG): $(TESTSRC) ${HEADERS}
	$(CC) $(TESTCPPFLAGS) $^ $(TESTLDFLAGS) -o $@
#	gcc $(TESTCPPFLAGS) $^ $(TESTLDFLAGS) -o $@
	
clean:
	rm -rf $(LIB) $(TESTPROG)
