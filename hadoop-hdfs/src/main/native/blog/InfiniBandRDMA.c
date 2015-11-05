#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <errno.h>
#include <InfiniBandRDAM.h>

  ////////////////////////////////////////////////
 // Internal tools.                            //
////////////////////////////////////////////////
#define TEST_NZ(x,y) do { if ((x)) die(y); } while (0)
#define TEST_Z(x,y) do { if (!(x)) die(y); } while (0)
#define TEST_N(x,y) do { if ((x)<0) die(y); } while (0)
static int die(const char *reason){
  fprintf(stderr, "Err: %s - %s \n ", strerror(errno), reason);
  exit(EXIT_FAILURE);
  return -1;
}

#define MAP_DEFINE(con,RDMAConnection);

#define MAX_SQE (100)

static int qp_change_state_init(struct ibv_qp *qp, int port){
  struct ibv_qp_attr *attr;
  attr = malloc(sizeof *attr);
  memset(attr, 0, sizeof *attr);
  attr->qp_state=IBV_QPS_INIT;
  attr->pkey_index=0;
  attr->port_num=port;
  attr->qp_access_flags=IBV_ACCESS_REMOTE_WRITE;
  TEST_NZ(ibv_modify_qp(qp,attr,
    IBV_QP_STATE|IBV_QP_PKEY_INDEX|IBV_QP_PORT|IBV_QP_ACCESS_FLAGS),
    "Could not modify QP to INIT, ibv_modify_qp");
  return 0;
}

static int qp_change_state_rtr(struct ibv_qp *qp, RDMAConnection * conn){
  struct ibv_qp_attr *attr;
  attr = malloc(sizeof *attr);
  memset(attr, 0, sizeof *attr);

  attr->qp_state = IBV_QPS_RTR;
  attr->path_mtu = IBV_MTU_2048;
  attr->dest_qp_num = conn->r_qpn;
  attr->rq_psn = conn->r_psn;
  attr->max_dest_rd_atomic = 1;
  attr->min_rnr_timer = 12;
  attr->ah_attr.is_global = 0;
  attr->ah_attr.dlid = conn->r_lid;
  attr->ah_attr.sl = 1;
  attr->ah_attr.src_path_bits = 0;
  attr->ah_attr.port_num = conn->port;

  TEST_NZ(ibv_modify_qp(qp, attr,
    IBV_QP_STATE|
    IBV_QP_AV|
    IBV_QP_PATH_MTU|
    IBV_QP_DEST_QPN|
    IBV_QP_RQ_PSN|
    IBV_QP_MAX_DEST_RD_ATOMIC|
    IBV_QP_MIN_RNR_TIMER),
    "Could not modify QP to RTR state");
  free(attr);
  return 0;
}


static void setibcfg(char *ibcfg, RDMAConnection *conn){
  sprintf(ibcfg, "%04x:%06x:%06x:%08x:%016Lx",
    conn->l_lid, conn->l_qpn, conn->l_psn, conn->l_rkey, conn->l_vaddr);
}

static void destroyRDMAConn(RDMAConnection *conn){
  if(conn->scq)ibv_destroy_cq(conn->scq);
  if(conn->rcq)ibv_destroy_cq(conn->rcq);
  if(conn->qp)ibv_destroy_qp(conn->qp);
  if(conn->comp_channel)ibv_destroy_comp_channel(conn->ch);
}

  ////////////////////////////////////////////////
 // RDMA Library APIs                          //
////////////////////////////////////////////////
static void* blog_rdma_daemon_routine(void* param){
  RDMACtxt* ctxt = (RDMACtxt*)param;
  // STEP 1 initialization
  struct addrinfo *res;
  struct addrinfo hints = {
    .ai_flags = AI_PASSIVE,
    .ai_family = AF_UNSPEC,
    .ai_socktype = SOCK_STREAM
  };
  char *service;
  int sockfd = -1;
  int n,connfd;
  ///struct sockaddr_in sin;
  TEST_N(asprintf(&service,"%d", ctxt->port), "ERROR writing port number to port string.");
  TEST_N(n=getaddrinfo(NULL,service,&hints,&res), "getaddrinfo threw error");
  TEST_N(sockfd=socket(res->ai_family, res->ai_socktype, res->ai_protocol), "Could not create server socket");
  setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);
  // STEP 2 binding
  TEST_N(bind(sockfd,res->ai_addr,res->ai_addrlen), "Could not bind addr to socket");
  listen(sockfd, 1);
  // STEP 3 waiting for requests
  while(1){
    struct sockaddr_in clientAddr;
    socklen_t addrLen;
    uint32_t clientip;
    TEST_N(connfd = accept(sockfd, (struct sockaddr*)&clientAddr, &addrLen), "server accept failed.");
    // STEP 3 - get remote IP
    clientip = clientAddr.sin_addr.s_addr;
    uint64_t cipkey = (uint64_t)clientip;
    int bDup;
    // STEP 4 - if this is included in map
    MAP_LOCK(con, ctxt->con_map, cipkey, "r");
    RDMAConnection *rdmaConn = NULL, *readConn = NULL;
    bDup = (MAP_READ(con, ctxt->con_map, cipkey, &readConn)==0);
    MAP_UNLOCK(con, ctxt->con_map, cipkey);
    if(bDup)continue;
    // STEP 5 - setup connection
    rdmaConn = (RDMAConnection*)malloc(sizeof(RDMAConnection));
    rdmaConn->scq  = NULL;
    rdmaConn->rcq  = NULL;
    rdmaConn->qp   = NULL;
    rdmaConn->ch   = NULL;
    rdmaConn->port = 1; // always use 1?
    TEST_Z(rdmaConn->ch=ibv_create_comp_channel(ctxt->ctxt),"Could not create completion channel, ibv_create_comp_channel");
    TEST_Z(rdmaConn->rcq=ibv_create_cq(ctxt->ctxt,1,NULL,NULL,0),"Could not create receive completion queue, ibv_create_cq");
    TEST_Z(rdmaConn->scq=ibv_create_cq(ctxt->ctxt,MAX_SQE,rdmaConn,rdmaConn->ch,0),"Could not create send completion queue, ibv_create_cq");
    struct ibv_qp_init_attr qp_init_attr = {
      .send_cq = rdmaConn->scq,
      .recv_cq = rdmaConn->rcq,
      .qp_type = IBV_QPT_RC,
      .cap = {
        .max_send_wr = MAX_SQE, // MAX_SQE must be less than ctxt->max_cqe
        .max_recv_wr = 1,
        .max_send_sge = ctxt->max_sge,
        .max_recv_sge = 1,
        .max_inline_data = 0
      }
    };
    TEST_Z(ramdConn->qp=ibv_create_qp(ctxt->pd,&qp_init_attr),"Could not create queue pair, ibv_create_qp");
    qp_change_state_init(rdmaConn->qp,rdmaConn->port);
    struct ibv_port_attr port_attr;
    TEST_Z(ibv_query_port(ctxt->ctxt,rdmaConn->port,&port_attr),"Could not get port attributes, ibv_query_port");
    rdmaConn->l_lid = port_attr.lid;
    rdmaConn->l_qpn = rdmaConn->qp->qp_num;
    rdmaConn->l_psn = lrand48() & 0xffffff;
    rdmaConn->l_rkey = ctxt->mr->rkey;
    rdmaConn->l_vaddr = (uintptr_t)ctxt->pool;
    char msg[sizeof "0000:000000:000000:00000000:0000000000000000"];
    // STEP 6 Exchange connection information(initialize client connection first.)
    /// server --> client | connection string
    setibcfg(msg,rdmaConn);
    if(write(connfd, msg, sizeof msg) != sizeof msg){
      perror("Could not send ibcfg to peer");
      return NULL;
    }
    /// client --> server | connection string | put connection to map
    if(read(connfd, msg, sizeof msg)!= sizeof msg){
      perror("Could not receive ibcfg from peer");
      return NULL;
    }
    int parsed = sscanf(msg, "%x:%x:%x:%x:%Lx",
    &rdmaConn->r_lid,&rdmaConn->r_qpn,&rdmaConn->r_psn,&rdmaConn->r_rkey,&rdmaConn->r_vaddr);
    if(parsed!=5){
      fprintf(stderr, "Could not parse message from peer.");
      return NULL;
    }
    /// change pair to RTS
    qp_change_state_rts(rdmaConn->qp,rdmaConn);
    // STEP 7 Put the connection to map.
    MAP_LOCK(con, ctxt->con_map, cipkey, "w");
    if(MAP_CREATE_AND_WRITE(con, ctxt->con_map, cipkey, &rdmaConn)==0){
      MAP_UNLOCK(con, ctxt->con_map, cipkey);
      close(connfd);
      destroyRDMAConn(rdmaConn);
      free(rdmaConn);
      continue;
    }
    MAP_UNLOCK(con, ctxt->con_map, cipkey);
  }
  return NULL;
}

int initializeContext(
  RDMACtxt *ctxt,
  const uint32_t psz,   // pool size
  const uint32_t align, // alignment
  const uint16_t port){ // port number
  ibv_device_attr dev_attr;
  // client/blog test
  int bClient = (port==0);
  // initialize sizes and counters
  ctxt->psz   = psz;
  ctxt->align = align;
  ctxt->cnt   = 0;
  // malloc pool
  TEST_NZ(posix_memalign(&ctxt->pool,
      bClient?RDMA_CTXT_BUF_SIZE(ctxt):RDMA_CTXT_PAGE_SIZE(ctxt),
      RDMA_CTXT_POOL_SIZE(ctxt)),
    "Cannot Allocate Pool Memory");
  // clear the data
  memset(ctxt->pool, 0, RDMA_CTXT_POOL_SIZE(ctxt));
  // get InfiniBand data structures
  struct ibv_device **dev_list;
  TEST_Z(dev_list = ibv_get_device_list(NULL),"No IB-device available. get_device_list returned NULL");
  TEST_Z(dev_list[0],"IB-device could not be assigned. Maybe dev_list array is empty");
  TEST_Z(ctxt->ctxt=ibv_open_device(dev_list[0]),"Could not create context, ibv_open_device");
  ibv_free_device_list(dev_list);
  TEST_Z(ctxt->pd=ibv_alloc_pd(ctxt->ctxt),"Could not allocate protection domain, ibv_alloc_pd");
  TEST_Z(ctxt->mr=ibv_reg_mr(
      ctxt->pd,
      ctxt->pool,
      RDMA_CTXT_POOL_SIZE(ctxt),
      bClient?(IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE):(IBV_ACCESS_REMOTE_READ | IBV_ACCESS_LOCAL_WRITE)),
    "Could not allocate mr, ibv_reg_mr. Do you have root privileges?");
  TEST_Z(ibv_query_device(ctxt->ctxt,&dev_attr),"Could not query device values");
  ctxt->max_sge = dev_attr.max_sge;
  ctxt->max_mr  = dev_attr.max_mr;
  ctxt->max_cq  = dev_attr.max_cq;
  ctxt->max_cqe = dev_attr.max_cqe;
  // initialize mutex lock
  TEST_Z(pthread_mutex_init(&ctxt->lock,NULL));
  // con_map
  TEST_NZ(ctxt->con_map = MAP_INITIALIZE(con), "Could not initialize RDMA connection map");
  if(bClient){
    // client: bitmap
    TEST_NZ(ctxt->bitmap = (uint8_t*)malloc(RDMA_CTXT_BYTES_BITMAP(c)));
    memset(ctxt->bitmap, 0, RDMA_CTXT_BYTES_BITMAP(c));
  }else{
    // blog: daemon thread
    TEST_Z(pthread_create(&ctxt->daemon, NULL,
        blog_rdma_daemon_routine, (void*)ctxt),
      "Could not initialize the daemon routine");
  }
  return 0;
}

int destroyContext(RDMACtxt *ctxt){
  // STEP 1 - destroy Connections
  uint64_t len = MAP_LENGTH(con,ctxt->con_map);
  if(len > 0){
    uint64_t *keys = MAP_GET_IDS(con,ctxt->con_map,len);
    while(len--){
      RDMAConnection *conn;
      MAP_READ(con,ctxt->con_map,keys[len],&conn);
      destroyRDMAConn(conn);
      MAP_DELETE(con,ctxt->con_map,keys[len]);
    }
    free(keys);
  }
  // STEP 2 - destroy Context
  if(ctxt->mr)ibv_dereg_mr(ctxt->mr);
  if(ctxt->pd)ibv_alloc_pd(ctxt->pd);
  if(ctxt->ctxt)ibv_close_device(ctxt->ctxt);
  if(pool)free(pool);
  if(port){//blog
    pthread_kill(ctxt->daemon, 9);
  }else{//client
    free(bitmap);
  }
  return 0;
}

int allocatePageArray(RDMACtxt *ctxt, void **pages, int num){
  // STEP 1 test Context mode, quit for client context
  if(ctxt->port==0){ /// client context
    fprintf(stderr,"Could not allocate page array in client mode");
    return -1;
  }
  // STEP 2 allocate pages
  if(num>RDMA_CTXT_NFPAGE(ctxt)){//we don't have enough space
    fprintf(stderr,"allocatePageArray():Could not allocate %d pages, because we have only %d pages left.\n",num,RDMA_CTXT_NFPAGE(ctxt));
    return -2;
  }
  if(pthread_mutex_lock(&ctxt->lock)!=0){
    fprintf(stderr,"allocatePageArray():Could not get lock.");
    return -3;
  };

  if(num>RDMA_CTXT_NFPAGE(ctxt)){//we don't have enough pages
    pthread_mutex_unlock(&ctxt->lock);
    fprintf(stderr,"allocatePageArray():Could not allocate %d pages, because we have only %d pages left.\n",num);
    return -2;
  }

  &pages = ctxt->pool + (ctxt->cnt << ctxt->align);
  ctxt->cnt += num;
  
  if(pthread_mutex_unlock(&ctxt->lock)!=0){
    fprintf(stderr,"allocatePageArray():Could not release lock.");
    return -3;
  }
  return 0;
}

int allocateBuffer(RDMACtxt *ctxt, void **buf){
  // STEP 1 test context mode, quit for blog context
  if(ctxt->port){ /// blog context
    fprintf(stderr,"Could not allocate buffer in blog mode");
    return -1;
  }
  // STEP 2 test if we have enough space
  if(!RDMA_CTXT_NFBUF(ctxt)){//we don't have enough space
    fprintf(stderr,"allocateBuffer():Could not allocate buffer because we have %d pages left.\n",RDMA_CTXT_NFPAGE(ctxt));
    return -2;
  }
  
  if(pthread_mutex_lock(&ctxt->lock)!=0){
    fprintf(stderr,"allocatePageArray():Could not get lock.");
    return -3;
  };

  if(!RDMA_CTXT_NFBUF(ctxt)){//we don't have enough space
    pthread_mutex_unlock(&ctxt->lock);
    fprintf(stderr,"allocateBuffer():Could not allocate buffer because we have %d pages left.\n",RDMA_CTXT_NFPAGE(ctxt));
    return -2;
  }

  // STEP 3 find the buffer
  uint32_t nbyte,nbit;
  /// 3.1 find the byte
  for(nbyte=0;nbyte<RDMA_CTXT_BYTES_BITMAP(ctxt);nbyte++){
    if(bitmap[nbyte]==0xff || 
      (ctxt->psz-ctxt->align == 2 && bitmap[nbyte] == 0xf0) ||
      (ctxt->psz-ctxt->align == 1 && bitmap[nbyte] == 0xc0) ||
      (ctxt->psz-ctxt->align == 0 && bitmap[nbyte] == 0xa0))
      continue;
  }
  if(nbyte==RDMA_CTXT_BYTES_BITMAP(ctxt)){
    pthread_mutex_unlock(&ctxt->lock);
    fprintf(stderr,"allocateBuffer():Could not allocate buffer: we should have %d pages left buf found none [type I].\n",RDMA_CTXT_NFPAGE(ctxt));
    return -4;
  }
  /// 3.2 find the bit
  nbit=RDMA_CTXT_BITS_BITMAP(ctxt)&0xf;
  while(--nbit >= 0)
    if((0x1<<nbit)&bitmap[nbyte])continue;
  if(nbit<0){
    pthread_mutex_unlock(&ctxt->lock);
    fprintf(stderr,"allocateBuffer():Could not allocate buffer: we should have %d pages left buf found none [type II].\n",RDMA_CTXT_NFPAGE(ctxt));
    return -4;
  }
  *buf = ctxt->pool+(8*nbyte+(8-nbit))*RDMA_CTXT_BUF_SIZE(ctxt);
  ctxt->cnt++;

  if(pthread_mutex_unlock(&ctxt->lock)!=0){
    fprintf(stderr,"allocatePageArray():Could not release lock.");
    return -3;
  }
  return 0;
}

int releaseBuffer(RDMACtxt *ctxt, const void *buf){
  // STEP 1 check mode
  if(ctxt->port){ /// blog context
    fprintf(stderr,"Could not allocate buffer in blog mode");
    return -1;
  }
  // STEP 2 validate buf address
  uint32_t nbyte,nbit;
  nbyte = ((buf-pool)/RDMA_CTXT_BUF_SIZE(ctxt))>>3;
  nbit  = ((buf-pool)/RDMA_CTXT_BUF_SIZE(ctxt))&7;
  if(((uint64_t)(buf-pool))%RDMA_CTXT_BUF_SIZE(ctxt)!=0 ||
      buf<pool ||
      buf>=pool+RDMA_CTXT_POOL_SIZE(ctxt)){
    fprintf(stderr,"releaseBuffer():invalid buf pointer=%p\n",buf);
    return -2;
  }
  if(pthread_mutex_lock(&ctxt->lock)!=0){
    fprintf(stderr,"releaseBuffer():Could not get lock.");
    return -3;
  };
  if(!ctxt->bitmap[nbyte]&(0xA0>>nbit)){
    pthread_mutex_unlock(&ctxt->lock);
    fprintf(stderr,"releaseBuffer():buffer[%p] is not allocated.\n",buf);
    return -4;
  }else{
    ctxt->bitmap[nbyte]^=(0xA0>>nbit);
    ctxt->cnt--;
  }
  if(pthread_mutex_unlock(&ctxt->lock)!=0){
    fprintf(stderr,"releaseBuffer():Could not release lock.");
    return -3;
  }
  return 0;
}
