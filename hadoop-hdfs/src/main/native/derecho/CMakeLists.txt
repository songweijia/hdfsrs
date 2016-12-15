cmake_minimum_required(VERSION 2.8)

project(derecho CXX)
set(CMAKE_CXX_FLAGS "-std=c++14 -Wall -ggdb -gdwarf-3")
add_subdirectory(rdmc EXCLUDE_FROM_ALL)
add_subdirectory(sst EXCLUDE_FROM_ALL)
add_subdirectory(experiments EXCLUDE_FROM_ALL)

add_library(derecho SHARED managed_group.cpp view.cpp view_utils.cpp derecho_row.cpp logger.cpp filewriter.cpp)
target_link_libraries(derecho rdmacm ibverbs  rt pthread atomic rdmc sst)

add_executable(main main.cpp)
target_link_libraries(main derecho)

add_executable(conntest connection_test.cpp)
target_link_libraries(conntest rdmc sst)

add_executable(gms_test gms_test.cpp experiments/block_size.cpp)
target_link_libraries(gms_test derecho)

add_executable(gms_test2 gms_test2.cpp experiments/block_size.cpp)
target_link_libraries(gms_test2 derecho)

