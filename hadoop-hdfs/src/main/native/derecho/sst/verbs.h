#ifndef VERBS_H
#define VERBS_H

/**
 * @file verbs.h
 * Contains declarations needed for working with RDMA using InfiniBand Verbs,
 * including the Resources class and global setup functions.
 */

#include <infiniband/verbs.h>

namespace sst {

  /** Structure to exchange the data needed to connect the Queue Pairs */
  struct cm_con_data_t {
    /** Buffer address */
    uint64_t addr;
    /** Remote key */
    uint32_t rkey;
    /** Queue Pair number */
    uint32_t qp_num;
    /** LID of the InfiniBand port */
    uint16_t lid;
    /** GID */
    uint8_t gid[16];
  }__attribute__((packed));

  /**
   * Represents the set of RDMA resources needed to maintain a two-way connection
   * to a single remote node.
   */
  class resources {
  private:
    /** Initializes the queue pair. */
    void set_qp_initialized();
    /** Transitions the queue pair to the ready-to-receive state. */
    void set_qp_ready_to_receive();
    /** Transitions the queue pair to the ready-to-send state. */
    void set_qp_ready_to_send();
    /** Connect the queue pairs. */
    void connect_qp();
    /** Post a remote RDMA operation. */
    int post_remote_send(long long int offset, long long int size, int op);

  public:
    /** Index of the remote node. */
    int remote_index;
    /** Handle for the IB Verbs Queue Pair object. */
    struct ibv_qp* qp;
    /** Memory Region handle for the write buffer. */
    struct ibv_mr* write_mr;
    /** Memory Region handle for the read buffer. */
    struct ibv_mr* read_mr;
    /** Connection data values needed to connect to remote side. */
    struct cm_con_data_t remote_props;
    /** Pointer to the memory buffer used for local writes.*/
    char* write_buf; 
    /** Pointer to the memory buffer used for the results of RDMA remote reads. */
    char* read_buf;

    /** Constructor; initializes Queue Pair, Memory Regions, and `remote_props`. */
    resources(int r_index, char* write_addr, char* read_addr, int size_w,
	      int size_r);
    /** Destroys the resources. */
    virtual ~resources();
    /*
      wrapper functions that make up the user interface
      all call post_remote_send with different parameters
    */
    /** Post an RDMA read at the beginning address of remote memory. */
    void post_remote_read(long long int size);
    /** Post an RDMA read at an offset into remote memory. */
    void post_remote_read(long long int offset, long long int size);
    /** Post an RDMA write at the beginning address of remote memory. */
    void post_remote_write(long long int size);
    /** Post an RDMA write at an offset into remote memory. */
    void post_remote_write(long long int offset, long long int size);
  };

  /** Initializes the global verbs resources. */
  void verbs_initialize();
  /** Polls for completion of a single posted remote read. */
  std::pair<int,int> verbs_poll_completion();
  /** Destroys the global verbs resources. */
  void verbs_destroy();

} //namespace sst

#endif //VERBS_H
