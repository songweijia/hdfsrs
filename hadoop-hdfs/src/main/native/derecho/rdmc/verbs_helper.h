
#ifndef VERBS_HELPER_H
#define VERBS_HELPER_H

#include "message.h"

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

struct ibv_mr;
struct ibv_qp;

namespace rdma {
class exception {};
class invalid_args : public exception {};
class connection_broken : public exception {};
class creation_failure : public exception {};
class mr_creation_failure : public creation_failure {};
class qp_creation_failure : public creation_failure {};
class memory_region {
    std::unique_ptr<ibv_mr, std::function<void(ibv_mr*)>> mr;
    friend class queue_pair;

public:
    memory_region(char* buffer, size_t size);
    uint32_t get_rkey() const;

    char* const buffer;
    const size_t size;
};
class remote_memory_region {
public:
    remote_memory_region(uint64_t remote_address, size_t length,
                         uint32_t remote_key)
        : buffer(remote_address), size(length), rkey(remote_key) {}

    const uint64_t buffer;
    const size_t size;
    const uint32_t rkey;
};

class queue_pair {
    std::unique_ptr<ibv_qp, std::function<void(ibv_qp*)>> qp;

public:
    ~queue_pair();
    explicit queue_pair(size_t remote_index);
    queue_pair(size_t remote_index,
               std::function<void(queue_pair*)> post_recvs);
    queue_pair(queue_pair&&) = default;
    bool post_send(const memory_region& mr, size_t offset, size_t length,
                   uint64_t wr_id, uint32_t immediate);
    bool post_recv(const memory_region& mr, size_t offset, size_t length,
                   uint64_t wr_id);

    bool post_empty_send(uint64_t wr_id, uint32_t immediate);
    bool post_empty_recv(uint64_t wr_id);

    bool post_write(const memory_region& mr, size_t offset, size_t length,
                    uint64_t wr_id, remote_memory_region remote_mr,
                    size_t remote_offset, bool signaled = false,
                    bool send_inline = false);
};

namespace impl {
bool verbs_initialize(const std::map<uint32_t, std::string>& node_addresses,
                      uint32_t node_rank);
bool verbs_add_connection(uint32_t index, const std::string& address,
                          uint32_t node_rank);
void verbs_destroy();
// int poll_for_completions(int num, ibv_wc* wcs,
//                          std::atomic<bool>& shutdown_flag);

// This function exchanges memory regions with all other connected nodes which
// enables us to do one-sided RDMA operations between them. Due to its nature,
// the function requires that it is called simultaneously on all nodes and that
// only one execution is active at any time.
std::map<uint32_t, remote_memory_region> verbs_exchange_memory_regions(
    const std::vector<uint32_t>& members, uint32_t node_rank,
    const memory_region& mr);
}
}
#endif /* VERBS_HELPER_H */
