
#include "verbs_helper.h"
#include "connection.h"
#include "util.h"

#include <arpa/inet.h>
#include <byteswap.h>
#include <cstring>
#include <endian.h>
#include <getopt.h>
#include <iostream>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

extern "C" {
#include <infiniband/verbs.h>
}

using namespace std;

namespace rdma {
const uint32_t TCP_PORT = 19875;

struct config_t {
    const char *dev_name;  // IB device name
    int ib_port = 1;       // local IB port to work with
    int gid_idx = 0;       // gid index to use
};

// structure to exchange data which is needed to connect the QPs
struct cm_con_data_t {
    uint32_t qp_num;  // QP number
    uint16_t lid;     // LID of the IB port
    uint8_t gid[16];  // gid
} __attribute__((packed));

// sockets for each connection
static map<uint32_t, tcp::socket> sockets;

// listener to detect new incoming connections
unique_ptr<tcp::connection_listener> connection_listener;

config_t local_config;

// structure of system resources
struct ibv_resources {
    ibv_device_attr device_attr;  // Device attributes
    ibv_port_attr port_attr;      // IB port attributes
    ibv_context *ib_ctx;          // device handle
    ibv_pd *pd;                   // PD handle
    ibv_cq *cq;                   // CQ handle
} verbs_resources;

static int modify_qp_to_init(struct ibv_qp *qp, int ib_port) {
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = ib_port;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                           IBV_ACCESS_REMOTE_WRITE;
    flags =
        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    rc = ibv_modify_qp(qp, &attr, flags);
    if(rc) fprintf(stderr, "failed to modify QP state to INIT\n");
    return rc;
}

static int modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn,
                            uint16_t dlid, uint8_t *dgid, int ib_port,
                            int gid_idx) {
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = verbs_resources.port_attr.active_mtu;
    attr.dest_qp_num = remote_qpn;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 16;
    attr.ah_attr.is_global = 1;
    attr.ah_attr.dlid = dlid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = ib_port;
    if(gid_idx >= 0) {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num = 1;
        memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit = 0xFF;
        attr.ah_attr.grh.sgid_index = gid_idx;
        attr.ah_attr.grh.traffic_class = 0;
    }
    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
            IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    rc = ibv_modify_qp(qp, &attr, flags);
    if(rc) fprintf(stderr, "failed to modify QP state to RTR\n");
    return rc;
}

static int modify_qp_to_rts(struct ibv_qp *qp) {
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 4;
    attr.retry_cnt = 6;
    attr.rnr_retry = 6;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;
    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
    rc = ibv_modify_qp(qp, &attr, flags);
    if(rc) fprintf(stderr, "failed to modify QP state to RTS. ERRNO=%d\n", rc);
    return rc;
}

namespace impl {
void verbs_destroy() {
    if(verbs_resources.cq && ibv_destroy_cq(verbs_resources.cq)) {
        fprintf(stderr, "failed to destroy CQ\n");
    }
    if(verbs_resources.pd && ibv_dealloc_pd(verbs_resources.pd)) {
        fprintf(stderr, "failed to deallocate PD\n");
    }
    if(verbs_resources.ib_ctx && ibv_close_device(verbs_resources.ib_ctx)) {
        fprintf(stderr, "failed to close device context\n");
    }
}

bool verbs_initialize(const map<uint32_t, string> &node_addresses,
                      uint32_t node_rank) {
    memset(&verbs_resources, 0, sizeof(verbs_resources));

    auto local_it = node_addresses.find(node_rank);
    if(local_it == node_addresses.end()) {
        fprintf(stderr,
                "ERROR: address list for verbs_initialize doesn't"
                "contain current node");
        return false;
    }

    connection_listener = make_unique<tcp::connection_listener>(TCP_PORT);

    TRACE("Starting connection phase");

    // Connect to other nodes in group. Since map traversal is ordered, we don't
    // have to worry about circular waits, so deadlock can't occur.
    for(auto it = node_addresses.begin(); it != node_addresses.end(); it++) {
        if(it->first != node_rank) {
            if(!verbs_add_connection(it->first, it->second, node_rank)) {
                fprintf(stderr, "WARNING: failed to connect to node %d at %s\n",
                        (int)it->first, it->second.c_str());
            }
        }
    }
    TRACE("Done connecting");

    auto res = &verbs_resources;

    ibv_device **dev_list = NULL;
    ibv_device *ib_dev = NULL;
    int i;
    int cq_size = 0;
    int num_devices = 0;
    int rc = 0;

    fprintf(stdout, "searching for IB devices in host\n");
    /* get device names in the system */
    dev_list = ibv_get_device_list(&num_devices);
    if(!dev_list) {
        fprintf(stderr, "failed to get IB devices list\n");
        rc = 1;
        goto resources_create_exit;
    }
    /* if there isn't any IB device in host */
    if(!num_devices) {
        fprintf(stderr, "found %d device(s)\n", num_devices);
        rc = 1;
        goto resources_create_exit;
    }

    fprintf(stdout, "found %d device(s)\n", num_devices);
    /* search for the specific device we want to work with */
    for(i = 0; i < num_devices; i++) {
        if(!local_config.dev_name) {
            local_config.dev_name = strdup(ibv_get_device_name(dev_list[i]));
            fprintf(stdout, "device not specified, using first one found: %s\n",
                    local_config.dev_name);
        }
        if(!strcmp(ibv_get_device_name(dev_list[i]), local_config.dev_name)) {
            ib_dev = dev_list[i];
            break;
        }
    }
    /* if the device wasn't found in host */
    if(!ib_dev) {
        fprintf(stderr, "IB device %s wasn't found\n", local_config.dev_name);
        rc = 1;
        goto resources_create_exit;
    }
    /* get device handle */
    res->ib_ctx = ibv_open_device(ib_dev);
    if(!res->ib_ctx) {
        fprintf(stderr, "failed to open device %s\n", local_config.dev_name);
        rc = 1;
        goto resources_create_exit;
    }
    /* We are now done with device list, free it */
    ibv_free_device_list(dev_list);
    dev_list = NULL;
    ib_dev = NULL;
    /* query port properties  */
    if(ibv_query_port(res->ib_ctx, local_config.ib_port, &res->port_attr)) {
        fprintf(stderr, "ibv_query_port on port %u failed\n",
                local_config.ib_port);
        rc = 1;
        goto resources_create_exit;
    }
    /* allocate Protection Domain */
    res->pd = ibv_alloc_pd(res->ib_ctx);
    if(!res->pd) {
        fprintf(stderr, "ibv_alloc_pd failed\n");
        rc = 1;
        goto resources_create_exit;
    }

    cq_size = 1024;
    res->cq = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
    if(!res->cq) {
        fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
        rc = 1;
        goto resources_create_exit;
    }

    TRACE("verbs_initialize() - SUCCESS");
    return true;
resources_create_exit:
    TRACE("verbs_initialize() - ERROR!!!!!!!!!!!!!!");
    if(rc) {
        if(res->cq) {
            ibv_destroy_cq(res->cq);
            res->cq = NULL;
        }
        if(res->pd) {
            ibv_dealloc_pd(res->pd);
            res->pd = NULL;
        }
        if(res->ib_ctx) {
            ibv_close_device(res->ib_ctx);
            res->ib_ctx = NULL;
        }
        if(dev_list) {
            ibv_free_device_list(dev_list);
            dev_list = NULL;
        }
    }
    return false;
}
bool verbs_add_connection(uint32_t index, const string &address,
                          uint32_t node_rank) {
    if(index < node_rank) {
        if(sockets.count(index) > 0) {
            fprintf(stderr,
                    "WARNING: attempted to connect to node %u at %s:%d but we "
                    "already have a connection to a node with that index.",
                    (unsigned int)index, address.c_str(), TCP_PORT);
            return false;
        }

        try {
            sockets[index] = tcp::socket(address, TCP_PORT);
        } catch(tcp::exception) {
            fprintf(stderr, "WARNING: failed to node %u at %s:%d",
                    (unsigned int)index, address.c_str(), TCP_PORT);
            return false;
        }

        // Make sure that the connection works, and that we've connected to the
        // right node.
        uint32_t remote_rank = 0;
        if(!sockets[index].exchange(node_rank, remote_rank)) {
            fprintf(stderr,
                    "WARNING: failed to exchange rank with node %u at %s:%d",
                    (unsigned int)index, address.c_str(), TCP_PORT);
            sockets.erase(index);
            return false;
        } else if(remote_rank != index) {
            fprintf(stderr,
                    "WARNING: node at %s:%d replied with wrong rank (expected"
                    "%d but got %d)",
                    address.c_str(), TCP_PORT, (unsigned int)index,
                    (unsigned int)remote_rank);

            sockets.erase(index);
            return false;
        }
        return true;
    } else if(index > node_rank) {
        try {
            tcp::socket s = connection_listener->accept();

            uint32_t remote_rank = 0;
            if(!s.exchange(node_rank, remote_rank)) {
                fprintf(stderr, "WARNING: failed to exchange rank with node");
                return false;
            } else {
                sockets[remote_rank] = std::move(s);
                return true;
            }
        } catch(tcp::exception) {
            fprintf(stderr, "Got error while attempting to listing on port");
            return false;
        }
    }

    return false;  // we can't connect to ourselves...
}
}
memory_region::memory_region(char *buf, size_t s) : buffer(buf), size(s) {
    if(!buffer || size == 0) throw rdma::invalid_args();

    int mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                   IBV_ACCESS_REMOTE_WRITE;

    // printf("MR Protection Domain = %p\n", verbs_resources.pd);

    mr = unique_ptr<ibv_mr, std::function<void(ibv_mr *)>>(
        ibv_reg_mr(verbs_resources.pd, const_cast<void *>((const void *)buffer),
                   size, mr_flags),
        [](ibv_mr *m) { ibv_dereg_mr(m); });

    if(!mr) {
        fprintf(stderr, "ERROR: ibv_reg_mr failed with mr_flags=0x%x\n",
                mr_flags);
        throw rdma::mr_creation_failure();
    }

    // fprintf(
    //     stdout,
    //     "MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
    //     buffer, mr->lkey, mr->rkey, mr_flags);
}
// memory_region::memory_region(ibv_mr *_mr)
//     : mr(_mr, [](ibv_mr *m) { ibv_dereg_mr(m); }),
//       buffer((char*)mr->addr),
//       size(mr->length) {}
// static ibv_mr *exp_reg_mr(ibv_pd *pd, void *addr, size_t len, uint64_t flags)
// {
//     ibv_exp_reg_mr_in in;
//     in.pd = pd;
// 	in.addr = addr;
// 	in.length = len;
// 	in.exp_access = flags;
// 	in.comp_mask = 0;
// 	in.create_flags = 0;
// 	return ibv_exp_reg_mr(&in);
// }
// memory_region::memory_region(size_t s)
//     : memory_region(exp_reg_mr(
//           verbs_resources.pd, NULL, s,
//           IBV_EXP_ACCESS_LOCAL_WRITE | IBV_EXP_ACCESS_REMOTE_READ |
//               IBV_EXP_ACCESS_REMOTE_WRITE | IBV_EXP_ACCESS_ALLOCATE_MR)) {
//     if(size == 0) throw rdma::invalid_args();

//     if(!mr) {
//         fprintf(stderr, "ERROR: ibv_reg_mr failed");
//         throw rdma::mr_creation_failure();
//     }
// }
uint32_t memory_region::get_rkey() const { return mr->rkey; }
queue_pair::~queue_pair() {
    //    if(qp) cout << "Destroying Queue Pair..." << endl;
}
queue_pair::queue_pair(size_t remote_index)
    : queue_pair(remote_index, [](queue_pair *) {}) {}

// The post_recvs lambda will be called before queue_pair creation completes on
// either end of the connection. This enables the user to avoid race conditions
// between post_send() and post_recv().
queue_pair::queue_pair(size_t remote_index,
                       std::function<void(queue_pair *)> post_recvs) {
    auto it = sockets.find(remote_index);
    if(it == sockets.end()) throw rdma::invalid_args();

    auto &sock = it->second;

    ibv_qp_init_attr qp_init_attr;
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 1;
    qp_init_attr.send_cq = verbs_resources.cq;
    qp_init_attr.recv_cq = verbs_resources.cq;
    qp_init_attr.cap.max_send_wr = 16;
    qp_init_attr.cap.max_recv_wr = 16;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;

    qp = unique_ptr<ibv_qp, std::function<void(ibv_qp *)>>(
        ibv_create_qp(verbs_resources.pd, &qp_init_attr),
        [](ibv_qp *q) { ibv_destroy_qp(q); });

    if(!qp) {
        fprintf(stderr, "failed to create QP\n");
        throw rdma::qp_creation_failure();
    }

    struct cm_con_data_t local_con_data;
    struct cm_con_data_t remote_con_data;
    memset(&local_con_data, 0, sizeof(local_con_data));
    memset(&remote_con_data, 0, sizeof(remote_con_data));
    union ibv_gid my_gid;

    if(local_config.gid_idx >= 0) {
        int rc = ibv_query_gid(verbs_resources.ib_ctx, local_config.ib_port,
                               local_config.gid_idx, &my_gid);
        if(rc) {
            fprintf(stderr, "could not get gid for port %d, index %d\n",
                    local_config.ib_port, local_config.gid_idx);
            return;
        }
    } else {
        memset(&my_gid, 0, sizeof my_gid);
    }

    /* exchange using TCP sockets info required to connect QPs */
    local_con_data.qp_num = qp->qp_num;
    local_con_data.lid = verbs_resources.port_attr.lid;
    memcpy(local_con_data.gid, &my_gid, 16);
    // fprintf(stdout, "Local QP number  = 0x%x\n", qp->qp_num);
    // fprintf(stdout, "Local LID        = 0x%x\n",
    // verbs_resources.port_attr.lid);

    if(!sock.exchange(local_con_data, remote_con_data))
        throw rdma::qp_creation_failure();

    bool success =
        !modify_qp_to_init(qp.get(), local_config.ib_port) &&
        !modify_qp_to_rtr(qp.get(), remote_con_data.qp_num, remote_con_data.lid,
                          remote_con_data.gid, local_config.ib_port,
                          local_config.gid_idx) &&
        !modify_qp_to_rts(qp.get());

    if(!success) printf("Failed to initialize QP\n");

    post_recvs(this);

    /* sync to make sure that both sides are in states that they can connect to
   * prevent packet loss */
    /* just send a dummy char back and forth */
    int tmp = -1;
    if(!sock.exchange(0, tmp) || tmp != 0) throw rdma::qp_creation_failure();
}
bool queue_pair::post_send(const memory_region &mr, size_t offset,
                           size_t length, uint64_t wr_id, uint32_t immediate) {
    if(mr.size < offset + length) return false;

    ibv_send_wr sr;
    ibv_sge sge;
    ibv_send_wr *bad_wr = NULL;

    // prepare the scatter/gather entry
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)(mr.buffer + offset);
    sge.length = length;
    sge.lkey = mr.mr->lkey;

    // prepare the send work request
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = wr_id;
    sr.imm_data = immediate;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_SEND_WITH_IMM;
    sr.send_flags = IBV_SEND_SIGNALED;  // | IBV_SEND_INLINE;

    if(ibv_post_send(qp.get(), &sr, &bad_wr)) {
        fprintf(stderr, "failed to post SR\n");
        return false;
    }
    return true;
}
bool queue_pair::post_empty_send(uint64_t wr_id, uint32_t immediate) {
    ibv_send_wr sr;
    ibv_send_wr *bad_wr = NULL;

    // prepare the send work request
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = wr_id;
    sr.imm_data = immediate;
    sr.sg_list = NULL;
    sr.num_sge = 0;
    sr.opcode = IBV_WR_SEND_WITH_IMM;
    sr.send_flags = IBV_SEND_SIGNALED;  // | IBV_SEND_INLINE;

    if(ibv_post_send(qp.get(), &sr, &bad_wr)) {
        fprintf(stderr, "failed to post SR\n");
        return false;
    }
    return true;
}

bool queue_pair::post_recv(const memory_region &mr, size_t offset,
                           size_t length, uint64_t wr_id) {
    if(mr.size < offset + length) return false;

    ibv_recv_wr rr;
    ibv_sge sge;
    ibv_recv_wr *bad_wr;

    // prepare the scatter/gather entry
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)(mr.buffer + offset);
    sge.length = length;
    sge.lkey = mr.mr->lkey;

    // prepare the receive work request
    memset(&rr, 0, sizeof(rr));
    rr.next = NULL;
    rr.wr_id = wr_id;
    rr.sg_list = &sge;
    rr.num_sge = 1;

    if(ibv_post_recv(qp.get(), &rr, &bad_wr)) {
        fprintf(stderr, "failed to post RR\n");
        fflush(stdout);
        return false;
    }
    return true;
}
bool queue_pair::post_empty_recv(uint64_t wr_id) {
    ibv_recv_wr rr;
    ibv_recv_wr *bad_wr;

    // prepare the receive work request
    memset(&rr, 0, sizeof(rr));
    rr.next = NULL;
    rr.wr_id = wr_id;
    rr.sg_list = NULL;
    rr.num_sge = 0;

    if(ibv_post_recv(qp.get(), &rr, &bad_wr)) {
        fprintf(stderr, "failed to post RR\n");
        fflush(stdout);
        return false;
    }
    return true;
}
bool queue_pair::post_write(const memory_region &mr, size_t offset,
                            size_t length, uint64_t wr_id,
                            remote_memory_region remote_mr,
                            size_t remote_offset, bool signaled,
                            bool send_inline) {
    if(mr.size < offset + length || remote_mr.size < remote_offset + length) {
        cout << "mr.size = " << mr.size << " offset = " << offset
             << " length = " << length << " remote_mr.size = " << remote_mr.size
             << " remote_offset = " << remote_offset;
        return false;
    }

    ibv_send_wr sr;
    ibv_sge sge;
    ibv_send_wr *bad_wr = NULL;

    // prepare the scatter/gather entry
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)(mr.buffer + offset);
    sge.length = length;
    sge.lkey = mr.mr->lkey;

    // prepare the send work request
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = wr_id;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_RDMA_WRITE;
    sr.send_flags = (signaled ? IBV_SEND_SIGNALED : 0) |
                    (send_inline ? IBV_SEND_INLINE : 0);
    sr.wr.rdma.remote_addr = remote_mr.buffer + remote_offset;
    sr.wr.rdma.rkey = remote_mr.rkey;

    if(ibv_post_send(qp.get(), &sr, &bad_wr)) {
        fprintf(stderr, "failed to post SR\n");
        return false;
    }
    return true;
}

// int poll_for_completions(int num, ibv_wc *wcs, atomic<bool> &shutdown_flag) {
//     while(true) {
//         int poll_result = ibv_poll_cq(verbs_resources.cq, num, wcs);
//         if(poll_result != 0 || shutdown_flag) {
//             return poll_result;
//         }
//     }

//     // if(poll_result < 0) {
//     //     /* poll CQ failed */
//     //     fprintf(stderr, "poll CQ failed\n");
//     // } else {
//     //     return
//     //     /* CQE found */
//     //     fprintf(stdout, "completion was found in CQ with status 0x%x\n",
//     //             wc.status);
//     //     /* check the completion status (here we don't care about the
//     //     completion
//     //      * opcode */
//     //     if(wc.status != IBV_WC_SUCCESS) {
//     //         fprintf(
//     //             stderr,
//     //             "got bad completion with status: 0x%x, vendor syndrome:
//     //             0x%x\n",
//     //             wc.status, wc.vendor_err);
//     //     }
//     // }
// }
namespace impl {
map<uint32_t, remote_memory_region> verbs_exchange_memory_regions(
    const vector<uint32_t> &members, uint32_t node_rank,
    const memory_region &mr) {
    map<uint32_t, remote_memory_region> remote_mrs;
    for(uint32_t m : members) {
        if(m == node_rank) {
            continue;
        }

        auto it = sockets.find(m);
        if(it == sockets.end()) {
            throw rdma::connection_broken();
        }

        uintptr_t buffer;
        size_t size;
        uint32_t rkey;

        bool still_connected =
            it->second.exchange((uintptr_t)mr.buffer, buffer) &&
            it->second.exchange((size_t)mr.size, size) &&
            it->second.exchange((uint32_t)mr.get_rkey(), rkey);

        if(!still_connected) {
            fprintf(stderr, "WARNING: lost connection to node %u\n",
                    (unsigned int)it->first);
            throw rdma::connection_broken();
        }

        remote_mrs.emplace(it->first, remote_memory_region(buffer, size, rkey));
    }
    return remote_mrs;
}
ibv_cq *verbs_get_cq() { return verbs_resources.cq; }
}
}
