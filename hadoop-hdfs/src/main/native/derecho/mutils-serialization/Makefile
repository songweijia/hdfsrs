.PHONY : clean

CPPFLAGS=-fPIC -g --std=c++14
LDFLAGS=-shared 

SOURCES = SerializationSupport.cpp
HEADERS = SerializationMacros.hpp SerializationSupport.hpp
OBJECTS=$(SOURCES:.cpp=.o)

TARGET=libmutils-serialization.so

all: $(TARGET)

clean:
	rm -f $(OBJECTS) $(TARGET)

$(TARGET) : $(OBJECTS)
	$(CXX) $(CPPFLAGS) $(OBJECTS) -o $@ $(LDFLAGS)

demo : $(TARGET)
	$(CXX) $(CPPFLAGS) -L`pwd` -Wl,-rpath=`pwd` serialization-demo.cpp -lmutils-serialization
