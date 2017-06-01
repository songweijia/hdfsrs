cmake_minimum_required(VERSION 2.8)

PROJECT(sst CXX)
SET(CMAKE_CXX_FLAGS "-std=c++14 -g -Wall -Wno-unused-function -Wno-unused-variable -fno-omit-frame-pointer -Wno-unused-but-set-variable -Wno-unused-result")

ADD_LIBRARY(sst SHARED tcp.cpp verbs.cpp)
TARGET_LINK_LIBRARIES(sst rdmacm ibverbs pthread rt) 
