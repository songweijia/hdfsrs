
#include "rdmc.h"
#include "group_send.h"
#include "message.h"
#include "microbenchmarks.h"
#include "util.h"
#include "verbs_helper.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <sys/mman.h>
#include <sys/resource.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include <math.h>

extern "C" {
#include <infiniband/verbs.h>
}

using namespace std;
using namespace rdma;

namespace rdma {
namespace impl {
extern ibv_cq* verbs_get_cq();
}
}

namespace rdmc {
uint32_t node_rank;
atomic<bool> shutdown_flag;

// map from group number to group
map<uint16_t, shared_ptr<group> > groups;
mutex groups_lock;

static void main_loop() {
    TRACE("Spawned main loop");

    const int max_work_completions = 1024;
    unique_ptr<ibv_wc[]> work_completions(new ibv_wc[max_work_completions]);

    while(true) {
        int num_completions = 0;
        while(!shutdown_flag && num_completions == 0) {
            num_completions =
                ibv_poll_cq(::rdma::impl::verbs_get_cq(), max_work_completions,
                            work_completions.get());
        }

        if(shutdown_flag) {
            groups.clear();
            ::rdma::impl::verbs_destroy();
            return;
        }

        if(num_completions < 0) {  // Negative indicates an IBV error.
            fprintf(stderr, "ERROR: Failed to poll completion queue.");
            continue;
        }

        for(int i = 0; i < num_completions; i++) {
            ibv_wc& wc = work_completions[i];
            auto tag = parse_tag(wc.wr_id);

            if(wc.status != 0) {
                printf("wc.status = %d; wc.wr_id = 0x%llx; imm = 0x%x\n",
                       (int)wc.status, (long long)wc.wr_id,
                       (unsigned int)wc.imm_data);
                fflush(stdout);
            } else if(wc.opcode == IBV_WC_SEND) {
                if(tag.message_type == MessageType::DATA_BLOCK) {
                    shared_ptr<group> g;

                    {
                        unique_lock<mutex> lock(groups_lock);
                        auto it = groups.find(tag.group_number);
                        if(it == groups.end()) {
                            fprintf(stderr,
                                    "ERROR: Sent data block didn't belong to "
                                    "any group?!");
                            continue;
                        }
                        g = it->second;
                    }

                    g->complete_block_send();
                } else if(tag.message_type == MessageType::READY_FOR_BLOCK) {
                    // Do nothing
                } else {
                    printf(
                        "sent unrecognized message type?! (message_type=%d)\n",
                        (int)tag.message_type);
                }
            } else if(wc.opcode == IBV_WC_RECV) {
                if(tag.message_type == MessageType::DATA_BLOCK) {
                    assert(wc.wc_flags & IBV_WC_WITH_IMM);
                    shared_ptr<group> g;
                    {
                        unique_lock<mutex> lock(groups_lock);
                        auto it = groups.find(tag.group_number);
                        if(it == groups.end()) {
                            fprintf(stderr,
                                    "ERROR: Received data block didn't belong "
                                    "to any group?!");
                            continue;
                        }
                        g = it->second;
                    }

                    g->receive_block(wc.imm_data, wc.byte_len);
                } else if(tag.message_type == MessageType::READY_FOR_BLOCK) {
                    shared_ptr<group> g;

                    {
                        unique_lock<mutex> lock(groups_lock);
                        auto it = groups.find(tag.group_number);
                        if(it != groups.end()) g = it->second;
                        // else
                        //     TRACE("Got RFB for group not yet created...");
                    }

                    if(g) {
                        g->receive_ready_for_block(wc.imm_data, tag.target);
                    }
                } else {
                    printf(
                        "received message with unrecognized type, even though "
                        "we posted the buffer?! (message_type=%d)\n",
                        (int)tag.message_type);
                }
            } else if(wc.opcode == IBV_WC_RDMA_WRITE) {
                // Do nothing
            } else {
                puts("Sent unrecognized completion type?!");
            }
        }
    }
}

bool initialize(const map<uint32_t, string>& addresses, uint32_t _node_rank) {
    if(shutdown_flag) return false;

    node_rank = _node_rank;

    TRACE("starting initialize");
    if(!::rdma::impl::verbs_initialize(addresses, node_rank)) {
        return false;
    }
    TRACE("verbs initialized");

    thread t(main_loop);
    t.detach();
    return true;
}
void add_address(uint32_t index, const string& address) {
    ::rdma::impl::verbs_add_connection(index, address, node_rank);
}

bool create_group(uint16_t group_number, std::vector<uint32_t> members,
                  size_t block_size, send_algorithm algorithm,
                  incoming_message_callback_t incoming_upcall,
                  completion_callback_t callback,
                  failure_callback_t failure_callback) {
    if(shutdown_flag) return false;

    shared_ptr<group> g;
    uint32_t member_index = index_of(members, node_rank);
    if(algorithm == BINOMIAL_SEND) {
        g = make_shared<binomial_group>(group_number, block_size, members,
                                        member_index, incoming_upcall,
                                        callback);
    } else if(algorithm == CHAIN_SEND) {
        g = make_shared<chain_group>(group_number, block_size, members,
                                     member_index, incoming_upcall, callback);
    } else if(algorithm == SEQUENTIAL_SEND) {
        g = make_shared<sequential_group>(group_number, block_size, members,
                                          member_index, incoming_upcall,
                                          callback);
    } else if(algorithm == TREE_SEND) {
        g = make_shared<tree_group>(group_number, block_size, members,
                                    member_index, incoming_upcall, callback);
    } else {
        puts("Unsupported group type?!");
        fflush(stdout);
        return false;
    }

    unique_lock<mutex> lock(groups_lock);
    auto p = groups.emplace(group_number, std::move(g));
    if(!p.second) return false;
    p.first->second->init();
    return true;
}

void destroy_group(uint16_t group_number) {
    if(shutdown_flag) return;

    unique_lock<mutex> lock(groups_lock);
    LOG_EVENT(group_number, -1, -1, "destroy_group");
    groups.erase(group_number);
}
void shutdown() { shutdown_flag = true; }
bool send(uint16_t group_number, shared_ptr<memory_region> mr, size_t offset,
          size_t length) {
    if(shutdown_flag) return false;

    shared_ptr<group> g;
    {
        unique_lock<mutex> lock(groups_lock);
        auto it = groups.find(group_number);
        if(it == groups.end()) return false;
        g = it->second;
    }
    LOG_EVENT(group_number, -1, -1, "preparing_to_send_message");
    g->send_message(mr, offset, length);
    return true;
}

barrier_group::barrier_group(vector<uint32_t> members) {
    member_index = index_of(members, node_rank);
    group_size = members.size();

    if(group_size <= 1 || member_index >= members.size())
        throw rdmc::invalid_args();

    total_steps = ceil(log2(group_size));
    for(unsigned int m = 0; m < total_steps; m++) steps[m] = -1;

    steps_mr = make_unique<memory_region>((char*)&steps[0],
                                          total_steps * sizeof(int64_t));
    number_mr = make_unique<memory_region>((char*)&number, sizeof(number));

    set<uint32_t> targets;
    for(unsigned int m = 0; m < total_steps; m++) {
        auto target = (member_index + (1 << m)) % group_size;
        auto target2 =
            (group_size * (1 << m) + member_index - (1 << m)) % group_size;
        targets.insert(target);
        targets.insert(target2);
    }

    map<uint32_t, queue_pair> qps;
    for(auto target : targets) {
        qps.emplace(target, queue_pair(members[target]));
    }

    auto remote_mrs = ::rdma::impl::verbs_exchange_memory_regions(
        members, node_rank, *steps_mr.get());
    for(unsigned int m = 0; m < total_steps; m++) {
        auto target = (member_index + (1 << m)) % group_size;

        remote_memory_regions.push_back(remote_mrs.find(target)->second);

        auto qp_it = qps.find(target);
        queue_pairs.push_back(std::move(qp_it->second));
        qps.erase(qp_it);
    }

    for(auto it = qps.begin(); it != qps.end(); it++) {
        extra_queue_pairs.push_back(std::move(it->second));
        qps.erase(it);
    }
}
void barrier_group::barrier_wait() {
    // See:
    // http://mvapich.cse.ohio-state.edu/static/media/publications/abstract/kinis-euro03.pdf

    unique_lock<mutex> l(lock);
    LOG_EVENT(-1, -1, -1, "start_barrier");
    number++;

    for(unsigned int m = 0; m < total_steps; m++) {
        if(!queue_pairs[m].post_write(
               *number_mr.get(), 0, 8,
               form_tag(0, (node_rank + (1 << m)) % group_size,
                        MessageType::BARRIER),
               remote_memory_regions[m], m * 8, false, true)) {
            throw rdmc::connection_broken();
        }

        while(steps[m] < number) /* do nothing*/
            ;
    }
    LOG_EVENT(-1, -1, -1, "end_barrier");
}
}
