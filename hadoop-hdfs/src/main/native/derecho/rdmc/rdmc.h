
#ifndef RDMC_H
#define RDMC_H

#include "verbs_helper.h"

#include <boost/optional.hpp>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace rdmc {

class exception {};
class connection_broken : public exception {};
class invalid_args : public exception {};
class nonroot_sender : public exception {};
class group_busy : public exception {};

enum send_algorithm {
    BINOMIAL_SEND = 1,
    CHAIN_SEND = 2,
    SEQUENTIAL_SEND = 3,
    TREE_SEND = 4
};

struct receive_destination {
    std::shared_ptr<rdma::memory_region> mr;
    size_t offset;
};

typedef std::function<receive_destination(size_t size)>
    incoming_message_callback_t;
typedef std::function<void(char* buffer, size_t size)> completion_callback_t;
typedef std::function<void(boost::optional<uint32_t> suspected_victim)>
    failure_callback_t;

bool initialize(const std::map<uint32_t, std::string>& addresses,
                uint32_t node_rank);
void add_address(uint32_t index, const std::string& address);
void shutdown();

bool create_group(uint16_t group_number, std::vector<uint32_t> members,
                  size_t block_size, send_algorithm algorithm,
                  incoming_message_callback_t incoming_receive,
                  completion_callback_t send_callback,
                  failure_callback_t failure_callback);
void destroy_group(uint16_t group_number);

bool send(uint16_t group_number, std::shared_ptr<rdma::memory_region> mr,
          size_t offset, size_t length);

class barrier_group {
    // Queue Pairs and associated remote memory regions used for performing a
    // barrier.
    std::vector<rdma::queue_pair> queue_pairs;
    std::vector<rdma::remote_memory_region> remote_memory_regions;

    // Additional queue pairs which will handle incoming writes (but which this
    // node does not need to interact with directly).
    std::vector<rdma::queue_pair> extra_queue_pairs;

    // RDMA memory region used for doing the barrier
    std::array<volatile int64_t, 32> steps;
    std::unique_ptr<rdma::memory_region> steps_mr;

    // Current barrier number, and a memory region to issue writes from.
    volatile int64_t number = -1;
    std::unique_ptr<rdma::memory_region> number_mr;

    // Number of steps per barrier.
    unsigned int total_steps;

    // Lock to ensure that only one barrier is in flight at a time.
    std::mutex lock;

    // Index of this node in the list of members
    uint32_t member_index;
    uint32_t group_size;

public:
    barrier_group(std::vector<uint32_t> members);
    void barrier_wait();
};
};

#endif /* RDMC_H */
