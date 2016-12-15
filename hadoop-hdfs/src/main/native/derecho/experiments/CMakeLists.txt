cmake_minimum_required(VERSION 2.8)

project(derecho CXX)
set(CMAKE_CXX_FLAGS "-std=c++14 -Wall -ggdb -gdwarf-3")

# test_group_interface
add_executable(test_group_interface test_group_interface.cpp initialize.cpp )
target_link_libraries(test_group_interface derecho)

# single_sender_one_message
add_executable(single_sender_one_message single_sender_one_message.cpp initialize.cpp )
target_link_libraries(single_sender_one_message derecho)

# single_sender_multiple_messages
add_executable(single_sender_multiple_messages single_sender_multiple_messages.cpp initialize.cpp )
target_link_libraries(single_sender_multiple_messages derecho)

# multiple_senders_multiple_messages
add_executable(multiple_senders_multiple_messages multiple_senders_multiple_messages.cpp initialize.cpp )
target_link_libraries(multiple_senders_multiple_messages derecho)

# derecho_bw_test
add_executable(derecho_bw_test derecho_bw_test.cpp block_size.cpp aggregate_bandwidth.cpp initialize.cpp )
target_link_libraries(derecho_bw_test derecho)

# ssd_bw_test
add_executable(ssd_bw_test ssd_bw_test.cpp block_size.cpp aggregate_bandwidth.cpp initialize.cpp )
target_link_libraries(ssd_bw_test derecho)

# gms_bw_test
add_executable(gms_bw_test gms_bw_test.cpp)
target_link_libraries(gms_bw_test derecho)

# rdmc_bw_test
add_executable(rdmc_bw_test rdmc_bw_test.cpp block_size.cpp aggregate_bandwidth.cpp initialize.cpp )
target_link_libraries(rdmc_bw_test derecho)

# rdmc_bw_test1
add_executable(rdmc_bw_test1 rdmc_bw_test1.cpp block_size.cpp aggregate_bandwidth.cpp initialize.cpp )
target_link_libraries(rdmc_bw_test1 derecho)

# rdmc_bw_test2
add_executable(rdmc_bw_test2 rdmc_bw_test2.cpp block_size.cpp aggregate_bandwidth.cpp initialize.cpp )
target_link_libraries(rdmc_bw_test2 derecho)

# window_size_test
add_executable(window_size_test window_size_test.cpp block_size.cpp aggregate_bandwidth.cpp initialize.cpp )
target_link_libraries(window_size_test derecho)

# write_to_ssd
add_executable(write_to_ssd write_to_ssd.cpp block_size.cpp aggregate_bandwidth.cpp initialize.cpp )
target_link_libraries(write_to_ssd derecho)

# timeline_test
add_executable(timeline_test timeline_test.cpp)
target_link_libraries(timeline_test derecho)

add_executable(viewchange_timeline viewchange_timeline.cpp)
target_link_libraries(viewchange_timeline derecho)
