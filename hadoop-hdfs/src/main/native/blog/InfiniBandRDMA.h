#ifndef __INFINIBANDRDMA_H__
#define __INFINIBANDRDMA_H__

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <string.h>
#include <fcntl.h>
#include <pthread.h>
#include <infiniband/verbs.h>
#include <map.h>

  ////////////////////////////////////////////////
 // Definition of structures.                  //
////////////////////////////////////////////////
typedef struct rdma_ctxt   RDMACtxt;
typedef struct rdma_conn   RDMAConnection;
/* rdma context */
MAP_DECLARE(con,RDMAConnection);
struct rdma_ctxt {
  struct ibv_context *ctxt;
  struct ibv_pd      *pd;
  struct ibv_mr      *mr;
  void               *pool;
  uint32_t           psz;      // pool size = 1l<<psz
  uint32_t           align;    // page/buf size = 1l<<align
  uint64_t           cnt;      // allocated page/buf counter
  uint8_t            *bitmap;  // bitmap length = 1<<(max(0,ctxt.psz-ctxt.align-3)); if bitmap == NULL, this is a blog context.
  uint16_t           port;     // blog daemon listen port.
  pthread_t          daemon;   // daemon thread for blog context. not used if this is client context.
  pthread_mutex_t    lock;     // mutex lock.
  BLOG_MAP_TYPE(con) *con_map; // map: ip -> rdma connection
/////below are device capabilities
  int32_t            max_sge;
  int32_t            max_mr;
  int32_t            max_cq;
  int32_t            max_cqe;
};
#define RDMA_CTXT_POOL_SIZE(c) (1l<<((c)->psz))
#define RDMA_CTXT_PAGE_SIZE(c) (1l<<((c)->align))
#define RDMA_CTXT_BUF_SIZE(c)  RDMA_CTXT_PAGE_SIZE(c)
#define RDMA_CTXT_NPAGE(c)     (1l<<((c)->psz-(c)->align))
#define RDMA_CTXT_NBUF(c)      RDMA_CTXT_NPAGE(c)
#define RDMA_CTXT_NFPAGE(c)    (RDMA_CTXT_NPAGE(c)-(c)->cnt)
#define RDMA_CTXT_NFBUF(c)     (RDMA_CTXT_NBUF(c)-(c)->cnt)
#define RDMA_CTXT_BYTES_BITMAP(c) \
  ((((c)->psz - (c)->align)>3)?(1l<<((c)->psz-(c)->align-3)):1l)
#define RDMA_CTXT_BITS_BITMAP(c) \
  (1l<<((c)->psz - (c)->align))
/*rdma connection */
struct rdma_conn {
  struct ibv_cq           *scq; // send completion queue
  struct ibv_cq           *rcq; // recv completion queue
  struct ibv_qp           *qp;  // queue pair
  struct ibv_comp_channel *ch;  // completion channel
  int32_t                 port; // infiniband port
  int32_t                 l_lid,r_lid;
  int32_t                 l_qpn,r_qpn;
  int32_t                 l_psn,r_psn;
  uint32_t                l_rkey,r_rkey;
  uint64_t                l_vaddr,r_vaddr;
};

  ////////////////////////////////////////////////
 // Definition of RDMA PRIMITIVES.             //
////////////////////////////////////////////////
/* initializeContext():Initialize an RDMA context.
 * PARAMETERS
 * ctxt:   the pointer pointing to an uninitialized context
 * psz:    pool size is calculated by (1l<<psz)
 * align:  page/buffer size is calculated by (1l<<align)
 * port:   port number for blog ctxt. port==0 means client ctxt,
 *         and ctxt->bitmap should be initialized.
 * RETURN VALUE
 * 0 for success
 * others for failure
 */
extern int initializeContext(RDMACtxt *ctxt, const uint32_t psz, 
  const uint32_t align, const uint16_t port);
/* destroyContext():Destroy an initialized RDMA context.
 * PARAMTERS
 * ctxt:   the pointer pointing to an initialized RDMA context
 * RETURN VALUE
 * 0 for success
 * others for failure
 */
extern int destroyContext(RDMACtxt *ctxt);
/* allocatePageArray(): allocate pages from a blog context.
 * PARAMETERS
 * ctxt:    the pointer pointing to an initialized blog context
 * pages:   the output parameter receiving allocated pages
 * num:     number of pages to be allocated
 * RETURN VALUE
 * 0 for success
 * others for failure
 */
extern int allocatePageArray(RDMACtxt *ctxt, void **pages, int num);
/* allocateBuffer(): allocate a buffer from a client context.
 * PARAMETERS
 * ctxt:    the pointer pointing to an initialized client context
 * buf:     the output parameter receiving the allocated buffer
 * RETURN VALUE
 * 0   success with *pages pointing to the page array
 * -1  could not allocate page array in client mode
 * -2  not enough free pages
 * -3  lock error
 */
extern int allocateBuffer(RDMACtxt *ctxt, void **buf);
/* releaseBuffer(): release a buffer to a client context.
 * PARAMETERS
 * ctxt:    the pointer pointing to an initialized client context
 * buf:     the allocated buffer to be released
 * RETURN VALUE
 * 0   success
 * -1  could not allocate buffer in blog mode
 * -2  not enough memory
 * -3  lock error
 * -4  bug:could not find buffer
 */
extern int releaseBuffer(RDMACtxt *ctxt, const void *buf);
/* connect(): connect the client context to a blog context.
 * PARAMETERS
 * ctxt:    the pointer pointing to an initialized client context
 * hostip:  the ip address of blog context(datanode)
 * port:    the port where blog daemon thread is listening on
 * RETURN VALUE
 * 0 for success
 * -1  could not allocate buffer in blog mode
 * -2  invalid buffer pointer
 * -3  lock error
 * -4  buffer is being allocated
 */
extern int connect(RDMACtxt *ctxt, const uint32_t hostip, const uint16_t port);
/*
 * disconnect(): disconnect the client context from a blog context.
 * PARAMETERS
 * ctxt:    the pointer pointing to an initialized client context.
 * hostip:  the ip address of blog context(datanode), hostip==0 means all connected blog contexts
 * port:    the port where blog daemon thread is listening on
 * RETURN VALUE
 * 0 for success
 * others for failure
 */
extern int disconnect(RDMACtxt *ctxt, const uint32_t hostip, const uint16_t port);
/* rdmaWrite(): write a list of pages using RDMA.
 * PARAMETERS
 * ctxt:    the pointer pointing to an initialized blog context.
 * hostip:  the ip address of the client
 * r_vaddr: the remote buffer address
 * pagelist:pages to be written
 * RETURN VALUE
 * 0 for success
 * others for failure
 */
extern int rdmaWrite(RDMACtxt *ctxt, const uint32_t hostip, const uint64_t r_vaddr, const void **pagelist);
  ////////////////////////////////////////////////
 // Definition of internal tools               //
////////////////////////////////////////////////
inline int isBlogCtxt(const RDMACtxt * ctxt){return (ctxt->bitmap==NULL);}

#endif//__INFINIBANDRDMA_H__
