#ifndef __LIBFABRICRDMA_H__
#define __LIBFABRICRDMA_H__

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <malloc.h>
#include <getopt.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <inttypes.h>
#include <netdb.h>
#include <errno.h>
#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_errno.h>
#include "debug.h"
#include "map.h"

#define LFPF_MR_KEY (0xFFF3C0DE)
#define LFPF_SERVER_PORT (7689)

#ifdef __cplusplus
extern "C" {
#endif //__cplusplus

////////////////////////////////////////////////
//  Definitiona of structures.                //
////////////////////////////////////////////////
#define MAX_LF_ADDR_SIZE        (256)
// #define DEFAULT_SGE_BAT_SIZE    (16)
// in LibFabric, the maximum entries allowed in an IOV operation
// is limited to SOCK_EP_MAX_IOV_LIMIT, which is hardwired to (8)
// in libfabric source code(prov/sockets/include/sock.h)
#define DEFAULT_SGE_BAT_SIZE    (8)
#define DEFAULT_TRANS_DEPTH     (16)
typedef struct lf_conn LFConn;
MAP_DECLARE(con,LFConn); 
typedef struct lf_ctxt LFCtxt;
struct lf_ctxt {
  struct fi_info     * hints, * fi; // info and hints
  struct fid_fabric  * fabric;
  struct fid_domain  * domain;
  struct fid_pep     * pep; // passive endpoint is only for the server
  struct fid_eq      * eq;
  struct fid_mr      * mr;
  uint64_t           local_mr_key; // local memory key

  // int                timeout_sec;
  struct fi_eq_attr  eq_attr;
  struct fi_cq_attr  cq_attr;

  char               local_ep_addr[MAX_LF_ADDR_SIZE];
  size_t             local_ep_addr_len;

  // parameters for rdma transfers
  uint32_t           tx_depth;     // transfer depth
  uint32_t           sge_bat_size; // scatter/gather batch size
  void               * pool;       // the memory pool
  uint32_t           psz;          // pool size = 1l<<psz
  uint32_t           align;        // page/buf size = 1l<<align
  uint16_t           is_client;    // if this is client or not.
  union {
    struct {
      uint8_t        * bitmap;     // bitmap length = 1<<(max(0,ctxt.psz-ctxt.align-3)); if bitmap == NULL, this is a blog context.
    } client;
    struct {
      pthread_t      daemon;       // daemon thread for blog context. not used if this is client context.
    } server;
  }                  extra_opts;   // extra options for client/server
  uint64_t           cnt;          // allocated page/buf counter
  uint16_t           port;         // blog daemon listen port.
  pthread_mutex_t    lock;         // mutex lock.
  BLOG_MAP_TYPE(con) *con_map;     // map: ip -> lf_conn connection
};
#define LF_CTXT_POOL_SIZE(c) (1l<<((c)->psz))
#define LF_CTXT_PAGE_SIZE(c) (1l<<((c)->align))
#define LF_CTXT_BUF_SIZE(c)  LF_CTXT_PAGE_SIZE(c)
#define LF_CTXT_NPAGE(c)     (1l<<((c)->psz-(c)->align))
#define LF_CTXT_NBUF(c)      LF_CTXT_NPAGE(c)
#define LF_CTXT_NFPAGE(c)    (LF_CTXT_NPAGE(c)-(c)->cnt)
#define LF_CTXT_NFBUF(c)     (LF_CTXT_NBUF(c)-(c)->cnt)
#define LF_CTXT_BYTES_BITMAP(c) \
  ((((c)->psz - (c)->align)>3)?(1l<<((c)->psz-(c)->align-3)):1l)
#define LF_CTXT_BITS_BITMAP(c) \
  (1l<<((c)->psz - (c)->align))
#define LF_CTXT_USE_VADDR(c)    (((c)->fi->domain_attr->mr_mode) & FI_MR_VIRT_ADDR)
#define LF_CTXT_ACTIVE_SGE_BAT_SIZE(c)  MIN((c)->sge_bat_size,(c)->fi->tx_attr->rma_iov_limit)

//libfabric extra configuration knobs for the client
struct lf_extra_config {
  uint64_t      mask;
#define EXTRA_CONFIG_TX_DEPTH           (1UL)
  uint32_t      tx_depth;
#define EXTRA_CONFIG_SGE_BAT_SIZE       (1UL<<1)
  uint32_t      sge_bat_size;
};

//libfabric connection
struct lf_conn {
  struct fid_cq		* txcq;
  struct fid_cq         * rxcq;
  struct fid_ep         * ep;
  struct fid_eq         * eq;
  uint64_t              r_rkey;
  fi_addr_t             remote_fi_addr;
  uint32_t              pid;
};

//connecting handshake procedure
/* PHASE I: request
 * 1. CLIENT  ==[ CONNECT REQ ]=>  SERVER
 * 2. CLIENT  <=[ PEP ADDR    ]==  SERVER
 *
 * PHASE II: establish LibFabric connection
 * 3. CLIENT initializes fabric,domain, and event queue
 * 4. CLIENT registers memory pool
 * 5. CLIENT initializes the endpoint
 * 6. CLIENT  --[ fi_connect  ]->  SERVER
 * 7. SERVER waits for connection requests
 * 8. SERVER creates the endpoint context(LFConn)
 * 9. CLIENT  <-[ fi_accept   ]--  SERVER
 *
 * PHASE III: exchange memory MR and ADDR
 *10. CLIENT ==[ MKey, PID, and VADDR]=> SERVER
 *    CLIENT <=[ MKey, PID, and VADDR]== SERVER
 *11. Both side receive the parameters
 *    Both sides insert this to LFConn
 */
//disconnecting handshake procedure
/* PHASE I: request
 * 1. CLIENT  ==[ DISCONNECT REQ ]=>  SERVER
 * 2. CLIENT  <=[ ECHO REQ       ]==  SERVER
 */
#define REQ_DISCONNECT          (0)
#define REQ_CONNECT             (1)
typedef struct lf_handshake_phase_I1 {
  uint32_t              req;
  uint32_t              pid;
} LF_HS_I1;

/*
typedef struct lf_handshake_phase_I2 {
  char pep_addr[MAX_LF_ADDR_SIZE];
} LF_HS_I2;
*/

typedef struct lf_handshake_phase_III {
  uint64_t              mr_key;
  fi_addr_t             vaddr;
} LF_HS_III;

////////////////////////////////////////////////
//  Definitiona of LIBFABRIC PRIMITIVES.      //
////////////////////////////////////////////////
/* initializeLFContext():Initialize the libfabric context.
 * PARAMETERS
 * ct:     the pointer pointing to an uninitialized context
 * pool:   the pool memory pointer
 * psz:    pool size is calculated by (1l<<psz)
 * align:  page/buffer size is calculated by (1l<<align)
 * dev:    device name for RDMA card. If null, use the first one we saw.
 * port:   port number for blog ctxt.
 * isClient: 
 *         client ctxt, and ctxt->bitmap should be initialized.
 * conf:   extra configuration, default to NULL.
 * RETURN VALUE
 * 0 for success
 * others for failure
 */
extern int initializeLFContext(
  struct lf_ctxt *ct,
  void * pool,
  const uint32_t psz,
  const uint32_t align,
  const char * provider,
  const char * domain,
  const uint16_t port,
  const uint16_t isClient,
  const struct lf_extra_config * conf = NULL);

/* destroyLFContext():Initialize an RDMA context.
 * PARAMETERS
 * ct:     the pointer pointing to an uninitialized context
 * pool:   the pool memory pointer
 * RETURN VALUE
 * 0 for success
 * others for failure
 */
extern int destroyLFContext(
  struct lf_ctxt *ct);

/* allocateLFBuffer(): allocate a buffer from a client context.
 * PARAMETERS
 * ct:      the pointer pointing to an initialized client context
 * buf:     the output parameter receiving the allocated buffer
 * RETURN VALUE
 * 0   success with *pages pointing to the page array
 * -1  could not allocate page array in client mode
 * -2  not enough free pages
 * -3  lock error
 */
extern int allocateLFBuffer(
  struct lf_ctxt *ct,
  void **buf);

/* releaseLFBuffer(): release a buffer to client context.
 * PARAMETERS
 * ct:      the pointer pointing to an initialized client context
 * buf:     the allocated buffer to be released
 * RETURN VALUE
 * 0   success with *pages pointing to the page array
 * -1  could not allocate page array in client mode
 * -2  not enough free pages
 * -3  lock error
 */
extern int releaseLFBuffer(
  struct lf_ctxt *ct,
  void *buf);

/* LFConnect(): connect the client context to a blog context.
 * PARAMETERS
 * ct:      the pointer pointing to an initialized client context
 * hostip:  the ip address of blog context(datanode), the value is decided by sockaddr_in.sin_addr.s_addr, please refer to "man 7 ip"
 * RETURN VALUE
 * 0 for success
 * -1  already connected
 * -2  cannot establish tcp connection to the server
 * -3  cannot send rdma configuration to peer
 * -4  cannot receive rdma configuration from peer
 * -5  cannot parse rdma configuration from peer
 * -6  cannot fill rdma connection to map
 */
extern int LFConnect(
  struct lf_ctxt *ct,
  const uint32_t hostip);

/* LFDisConnect(): connect the client context to a blog context.
 * PARAMETERS
 * ct:      the pointer pointing to an initialized client context
 * hostip:  the ip address of blog context(datanode), the value is decided by sockaddr_in.sin_addr.s_addr, please refer to "man 7 ip"
 * RETURN VALUE
 * 0 for success
 * -1  already connected
 * -2  cannot establish tcp connection to the server
 * -3  cannot send rdma configuration to peer
 * -4  cannot receive rdma configuration from peer
 * -5  cannot parse rdma configuration from peer
 * -6  cannot fill rdma connection to map
 */
extern int LFDisconnect(
  struct lf_ctxt *ct,
  const uint32_t hostip);

/* LFTransfer(): transfer a list of pages through LibFabric.
 * PARAMETERS
 * ct:      the pointer pointing to an initialized blog context.
 * hostip:  the ip address of the client
 * pid:     the pid of the client process
 * r_vaddr: the remote buffer address
 * pagelist:pages to be transfer to/from the client buffer
 * npage:   number of the pages to be read
 * iswrite: true for write, false for read.
 * pagesize: 0 for the default page size specified in ctxt, otherwise, use this user specified page size.
 * RETURN VALUE
 * 0 for success
 * others for failure
 */
extern int LFTransfer(
  struct lf_ctxt *ct,
  const uint32_t hostip,
  const uint32_t pid,
  const uint64_t r_addr,
  void **pargelist,
  int npage,
  int isWrite,
  int pageSize);

#define rdmaWrite( ctxt, hostip, pid, r_vaddr, pagelist, npage, pagesize ) \
  LFTransfer( ctxt, hostip, pid, r_vaddr, pagelist, npage, 1, pagesize)
#define rdmaRead( ctxt, hostip, pid, r_vaddr, pagelist, npage, pagesize ) \
  LFTransfer( ctxt, hostip, pid, r_vaddr, pagelist, npage, 0, pagesize)

// helpers
inline const uint32_t getip(const char* ipstr){
  return (const uint32_t)inet_addr(ipstr);
}

#ifdef __cplusplus
}
#endif //__cplusplus

#endif
