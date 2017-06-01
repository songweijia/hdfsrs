
#ifndef GROUP_SEND_H
#define GROUP_SEND_H

#include "message.h"
#include "rdmc.h"
#include "util.h"
#include "verbs_helper.h"

#include <boost/optional.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <vector>

using boost::optional;
using std::vector;
using std::map;
using std::unique_ptr;
using rdmc::incoming_message_callback_t;
using rdmc::completion_callback_t;

class group {
private:
    const vector<uint32_t> members;  // first element is the sender

    // Set of receivers who are ready to receive the next block from us.
    std::set<uint32_t> receivers_ready;

    std::mutex monitor;

    unique_ptr<rdma::memory_region> first_block_mr;
    optional<size_t> first_block_number;
    unique_ptr<char[]> first_block_buffer;

    std::shared_ptr<rdma::memory_region> mr;
    size_t mr_offset;
    size_t message_size;
    size_t incoming_block;
    size_t message_number = 0;

    size_t outgoing_block;
    bool sending = false;  // Whether a block send is in progress
    size_t send_step = 0;  // Number of blocks sent/stalls so far

    // Total number of blocks received and the number of chunks
    // received for ecah block, respectively.
    size_t num_received_blocks = 0;
    size_t receive_step = 0;
    vector<bool> received_blocks;

    completion_callback_t completion_callback;
    incoming_message_callback_t incoming_message_upcall;

protected:
    const uint16_t group_number;
    const size_t block_size;
    const uint32_t num_members;
    const uint32_t member_index;  // our index in the members list
    size_t num_blocks;

    // maps from member_indices to the queue pairs
    map<size_t, rdma::queue_pair> queue_pairs;
    map<size_t, rdma::queue_pair> rfb_queue_pairs;

    struct block_transfer {
        uint32_t target;
        size_t block_number;
    };

    virtual optional<block_transfer> get_outgoing_transfer(
        size_t send_step) const = 0;
    virtual optional<block_transfer> get_incoming_transfer(
        size_t receive_step) const = 0;
    virtual optional<block_transfer> get_first_block() const = 0;
    virtual size_t get_total_steps() const = 0;
    virtual void form_connections() = 0;
    void connect(size_t neighbor);

public:
    group(uint16_t group_number, size_t block_size, vector<uint32_t> members,
          uint32_t member_index, incoming_message_callback_t upcall,
          completion_callback_t callback);
    virtual ~group();

    void receive_block(uint32_t send_imm, size_t size);
    void receive_ready_for_block(uint32_t step, uint32_t sender);
    void complete_block_send();

    void send_message(std::shared_ptr<rdma::memory_region> message_mr,
                      size_t offset, size_t length);

    // This function is necessary because we can't issue a virtual function call
    // from a constructor, but we need to perform them in order to know which
    // queue pair to post the first_block_buffer on, and which nodes we must
    // connect to.
    void init();

private:
    void post_recv(block_transfer transfer);
    void send_next_block();
    void complete_message();
    void prepare_for_next_message();
    void send_ready_for_block(uint32_t neighbor);
};
class chain_group : public group {
public:
    using group::group;

protected:
    void form_connections();
    optional<block_transfer> get_outgoing_transfer(size_t send_step) const;
    optional<block_transfer> get_incoming_transfer(size_t receive_step) const;
    optional<block_transfer> get_first_block() const;
    size_t get_total_steps() const;
};
class sequential_group : public group {
public:
    using group::group;

protected:
    void form_connections();
    optional<block_transfer> get_outgoing_transfer(size_t send_step) const;
    optional<block_transfer> get_incoming_transfer(size_t receive_step) const;
    optional<block_transfer> get_first_block() const;
    size_t get_total_steps() const;
};
class tree_group : public group {
public:
    using group::group;

protected:
    void form_connections();
    optional<block_transfer> get_outgoing_transfer(size_t send_step) const;
    optional<block_transfer> get_incoming_transfer(size_t receive_step) const;
    optional<block_transfer> get_first_block() const;
    size_t get_total_steps() const;
};
class binomial_group : public group {
private:
    // Base to logarithm of the group size, rounded down.
    unsigned int log2_num_members;

    optional<block_transfer> get_vertex_outgoing_transfer(size_t send_step);
    optional<block_transfer> get_vertex_incoming_transfer(size_t receive_step);

public:
    static optional<block_transfer> get_vertex_outgoing_transfer(
        uint32_t vertex, size_t step, uint32_t num_members,
        unsigned int log2_num_members, size_t num_blocks, size_t total_steps);
    static optional<block_transfer> get_vertex_incoming_transfer(
        uint32_t vertex, size_t step, uint32_t num_members,
        unsigned int log2_num_members, size_t num_blocks, size_t total_steps);
    static optional<block_transfer> get_outgoing_transfer(
        uint32_t node, size_t step, uint32_t num_members,
        unsigned int log2_num_members, size_t num_blocks, size_t total_steps);
    static optional<block_transfer> get_incoming_transfer(
        uint32_t node, size_t step, uint32_t num_members,
        unsigned int log2_num_members, size_t num_blocks, size_t total_steps);
    static optional<size_t> get_intravertex_block(uint32_t vertex, size_t step,
                                                  uint32_t num_members,
                                                  unsigned int log2_num_members,
                                                  size_t num_blocks,
                                                  size_t total_steps);
    static uint32_t get_intervertex_receiver(uint32_t vertex, size_t step,
                                             uint32_t num_members,
                                             unsigned int log2_num_members,
                                             size_t num_blocks,
                                             size_t total_steps);
    binomial_group(uint16_t group_number, size_t block_size,
                   vector<uint32_t> members, uint32_t member_index,
                   incoming_message_callback_t upcall,
                   completion_callback_t callback);

protected:
    void form_connections();
    optional<block_transfer> get_outgoing_transfer(size_t send_step) const;
    optional<block_transfer> get_incoming_transfer(size_t receive_step) const;
    optional<block_transfer> get_first_block() const;
    size_t get_total_steps() const;
};

#endif /* GROUP_SEND_H */
