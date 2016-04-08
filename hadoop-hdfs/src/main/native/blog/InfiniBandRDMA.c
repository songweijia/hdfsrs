#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <errno.h>
#include <netdb.h>
#include <pthread.h>
#include <infiniband/verbs.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include "InfiniBandRDMA.h"

  ////////////////////////////////////////////////
 // Internal tools.                            //
////////////////////////////////////////////////
#define TEST_NZ(x,y) do { if ((x)) die(y); } while (0)
#define TEST_Z(x,y) do { if (!(x)) die(y); } while (0)
#define TEST_N(x,y) do { if ((x)<0) die(y); } while (0)
#define MAKE_CON_KEY(host,port_or_pid) ((((long)(host))<<32)|(port_or_pid))
#define GET_IP_FROM_KEY(key)	((const uint32_t)(((key)>>32)&0x00000000FFFFFFFF))

static int die(const char *reason){
  fprintf(stderr, "Err: %s - %s \n ", strerror(errno), reason);
  exit(EXIT_FAILURE);
  return -1;
}
/*
#ifdef DEBUG
  #define DEBUG_PRINT(fmt, args...) fprintf(stderr, fmt, ## args);
#else
  #define DEBUG_PRINT(fmt, args...)
#endif//DEBUG
*/
MAP_DEFINE(con,RDMAConnection,10);

#define MAX_SQE (1024)  // MAX_SQE * MAX_SGE * PAGE_SIZE = 128M

static int qp_change_state_init(struct ibv_qp *qp, int port){
  struct ibv_qp_attr *attr;
  attr = malloc(sizeof *attr);
  memset(attr, 0, sizeof *attr);
  attr->qp_state=IBV_QPS_INIT;
  attr->pkey_index=0;
  attr->port_num=port;
  attr->qp_access_flags=IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_READ;
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
  attr->ah_attr.grh.dgid = conn->r_gid;
  attr->ah_attr.grh.flow_label = 0;
  attr->ah_attr.grh.sgid_index = 0;
  attr->ah_attr.grh.hop_limit = 10; // allow 10 hops
  attr->ah_attr.grh.traffic_class = 0;
  attr->ah_attr.is_global = 1;
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

static int qp_change_state_rts(struct ibv_qp *qp, RDMAConnection * conn){
  qp_change_state_rtr(qp, conn);
  struct ibv_qp_attr *attr;
  attr = malloc(sizeof *attr);
  memset(attr, 0, sizeof *attr);

  attr->qp_state = IBV_QPS_RTS;
  attr->timeout = 4;
  attr->retry_cnt = 6;
  attr->rnr_retry = 6;
  attr->sq_psn = conn->l_psn;
  attr->max_rd_atomic = 1;

  TEST_NZ(ibv_modify_qp(qp, attr,
    IBV_QP_STATE |
    IBV_QP_TIMEOUT |
    IBV_QP_RETRY_CNT |
    IBV_QP_RNR_RETRY |
    IBV_QP_SQ_PSN |
    IBV_QP_MAX_QP_RD_ATOMIC),
    "Could not modify QP to RTS State");
  free(attr);
  return 0;
}

static void setibcfg(IbConEx *ex, RDMAConnection *conn){
  ex->req = REQ_CONNECT;
  ex->gid = conn->l_gid;
  ex->lid = conn->l_lid;
  ex->qpn = conn->l_qpn;
  ex->psn = conn->l_psn;
  ex->rkey = conn->l_rkey;
  ex->pid = getpid();
  ex->vaddr = conn->l_vaddr;
}

static void destroyRDMAConn(RDMAConnection *conn){
  if(conn->scq)ibv_destroy_cq(conn->scq);
  if(conn->rcq)ibv_destroy_cq(conn->rcq);
  if(conn->qp)ibv_destroy_qp(conn->qp);
  if(conn->ch)ibv_destroy_comp_channel(conn->ch);
}

static int serverConnectInternal(RDMACtxt *ctxt, int connfd, const IbConEx * r_exm, int clientip){
    int bDup;
    // STEP 0 - get rpid
    int rpid = r_exm->pid;
    uint64_t cipkey = MAKE_CON_KEY(clientip,rpid);
    // STEP 1 - if this is included in map
    MAP_LOCK(con, ctxt->con_map, cipkey, 'r');
    RDMAConnection *rdmaConn = NULL, *readConn = NULL;
    bDup = (MAP_READ(con, ctxt->con_map, cipkey, &readConn)==0);
    MAP_UNLOCK(con, ctxt->con_map, cipkey);
    if(bDup)return 0;// return success for duplicated connect...

    // STEP 2 - setup connection
    rdmaConn = (RDMAConnection*)malloc(sizeof(RDMAConnection));
    rdmaConn->scq  = NULL;
    rdmaConn->rcq  = NULL;
    rdmaConn->qp   = NULL;
    rdmaConn->ch   = NULL;
    rdmaConn->port = 1; // always use port 1? so far so good. let's make it configurable later.
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
    TEST_Z(rdmaConn->qp=ibv_create_qp(ctxt->pd,&qp_init_attr),"Could not create queue pair, ibv_create_qp");
    qp_change_state_init(rdmaConn->qp,rdmaConn->port);
    struct ibv_port_attr port_attr;
    TEST_NZ(ibv_query_port(ctxt->ctxt,rdmaConn->port,&port_attr),"Could not get port attributes, ibv_query_port");
    rdmaConn->global=1;
    TEST_NZ(ibv_query_gid(ctxt->ctxt,rdmaConn->port,0,&rdmaConn->l_gid),"Could not get gid from port");
    rdmaConn->l_lid = port_attr.lid;
    rdmaConn->l_qpn = rdmaConn->qp->qp_num;
    rdmaConn->l_psn = lrand48() & 0xffffff;
    rdmaConn->l_rkey = ctxt->mr->rkey;
    rdmaConn->l_vaddr = (uintptr_t)ctxt->pool;
    // STEP 6 Exchange connection information(initialize client connection first.)
    IbConEx l_exm; // exchange message
    /// server --> client | connection string
    setibcfg(&l_exm,rdmaConn);
    if(write(connfd, &l_exm, sizeof l_exm) != sizeof l_exm){
      perror("Could not send ibcfg to peer");
      return -1;
    }
    /// copy from remote configuration message
    rdmaConn->r_lid = r_exm->lid;
    rdmaConn->r_gid = r_exm->gid;
    rdmaConn->r_qpn = r_exm->qpn;
    rdmaConn->r_psn = r_exm->psn;
    rdmaConn->r_rkey = r_exm->rkey;
    rdmaConn->r_vaddr = r_exm->vaddr;
    /// change pair to RTS
    qp_change_state_rts(rdmaConn->qp,rdmaConn);
    // STEP 7 Put the connection to map.
    MAP_LOCK(con, ctxt->con_map, cipkey, 'w');
    if(MAP_CREATE_AND_WRITE(con, ctxt->con_map, cipkey, rdmaConn)!=0){
      MAP_UNLOCK(con, ctxt->con_map, cipkey);
      destroyRDMAConn(rdmaConn);
      free(rdmaConn);
      perror("Could not put rdmaConn to map.");
      return -3;
    }
    MAP_UNLOCK(con, ctxt->con_map, cipkey);
    DEBUG_PRINT("new client:%lx\n",cipkey);
    // end setup connection
    return 0;
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
  TEST_NZ(n=getaddrinfo(NULL,service,&hints,&res), "getaddrinfo threw error");
  TEST_N(sockfd=socket(res->ai_family, res->ai_socktype, res->ai_protocol), "Could not create server socket");
  setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);
  // STEP 2 binding
  TEST_NZ(bind(sockfd,res->ai_addr,res->ai_addrlen), "Could not bind addr to socket");
  listen(sockfd, 1);
  // STEP 3 waiting for requests
  while(1){
    struct sockaddr_in clientAddr;
    socklen_t addrLen = sizeof(struct sockaddr_in);
    uint32_t clientip;
    TEST_N(connfd = accept(sockfd, (struct sockaddr*)&clientAddr, &addrLen), "server accept failed.");
    // STEP 3.1 - get remote IP
    clientip = clientAddr.sin_addr.s_addr;
    //uint64_t cipkey = (uint64_t)clientip;
    // STEP 3.2 - read connection string...
    IbConEx exm;
    /// client --> server | connection string | put connection to map
    if(read(connfd, &exm, sizeof exm)!= sizeof exm){
      perror("Could not receive ibcfg from peer");
      return NULL;
    }
    if(exm.req == REQ_DISCONNECT){//only pid for disconnection.
      DEBUG_PRINT("Client %x:%x trying to disconnect.\n", clientip, exm.pid);
      uint64_t cipkey = MAKE_CON_KEY(clientip,exm.pid);
      RDMAConnection * readConn = NULL;
      // find the connection, destroy and clean it up.
      MAP_LOCK(con, ctxt->con_map, cipkey, 'w');
      if(MAP_READ(con, ctxt->con_map, cipkey, &readConn) == 0){
        MAP_DELETE(con,ctxt->con_map,cipkey);
      }
      MAP_UNLOCK(con, ctxt->con_map, cipkey);
      DEBUG_PRINT("deleted ipkey:%lx from con map.\n",cipkey);
      destroyRDMAConn(readConn);
      DEBUG_PRINT("destroyed connection for %lx\n",cipkey);
      free(readConn);
      DEBUG_PRINT("released space for %lx\n",cipkey);
      DEBUG_PRINT("Client %x:%x successfully disconnected!\n",clientip, exm.pid);
    }else{
      // setup connection.
      if(serverConnectInternal(ctxt,connfd,&exm,clientip)!=0)
        return NULL;
    }
    close(connfd);
  }
  return NULL;
}

int initializeContext(
  RDMACtxt *ctxt,
  const uint32_t psz,   // pool size
  const uint32_t align, // alignment
  const char* dev, // RDMA device name, if null, we use the first one we see.
  const uint16_t port, // port number
  const uint16_t bClient){ // is client or not?
  struct ibv_device_attr dev_attr;
  int i,num_device;
  ctxt->port = port;
  ctxt->isClient = bClient;
  // initialize sizes and counters
  ctxt->psz   = psz;
  ctxt->align = align;
  ctxt->cnt   = 0;
  // malloc pool
  DEBUG_PRINT("debug-bClient=%d,psz=%d,align=%d\n",bClient,psz,align);
  DEBUG_PRINT("debug-RDMA_CTXT_BUF_SIZE=%ld\n",RDMA_CTXT_BUF_SIZE(ctxt));
  DEBUG_PRINT("debug-RDMA_CTXT_PAGE_SIZE=%ld\n",RDMA_CTXT_PAGE_SIZE(ctxt));
  DEBUG_PRINT("debug-RDMA_CTXT_POOL_SIZE=%ld\n",RDMA_CTXT_POOL_SIZE(ctxt));
  TEST_NZ(posix_memalign(&ctxt->pool,
      bClient?RDMA_CTXT_BUF_SIZE(ctxt):RDMA_CTXT_PAGE_SIZE(ctxt),
      RDMA_CTXT_POOL_SIZE(ctxt)),
    "Cannot Allocate Pool Memory");
  DEBUG_PRINT("debug:posix_memalign() get ctxt->pool:%p\n",ctxt->pool);
  // clear the data
  memset(ctxt->pool, 0, RDMA_CTXT_POOL_SIZE(ctxt));
  // get InfiniBand data structures
  DEBUG_PRINT("debug:before initialize infiniband data structures\n");
  struct ibv_device **dev_list;
  TEST_Z(dev_list = ibv_get_device_list(&num_device),"No IB-device available. get_device_list returned NULL");
  for(i = 0;i<num_device;i++){
    const char * devname = ibv_get_device_name(dev_list[i]);
    DEBUG_PRINT("check device:%s\n",devname);
    if(dev==NULL || strcmp(devname,dev)==0){
      break;
    }
    DEBUG_PRINT("skip.\n");
  }
  if(i == num_device)// cannot find device
    return -1;
  TEST_Z(dev_list[i],"IB-device could not be assigned. Maybe dev_list array is empty");
  DEBUG_PRINT("using dev:%s\n%s\n%s\n",dev_list[i]->dev_name,dev_list[i]->dev_path,dev_list[i]->ibdev_path);
  TEST_Z(ctxt->ctxt=ibv_open_device(dev_list[i]),"Could not create context, ibv_open_device");
  ibv_free_device_list(dev_list);
  TEST_Z(ctxt->pd=ibv_alloc_pd(ctxt->ctxt),"Could not allocate protection domain, ibv_alloc_pd");
  TEST_Z(ctxt->mr=ibv_reg_mr(
      ctxt->pd,
      ctxt->pool,
      RDMA_CTXT_POOL_SIZE(ctxt),
      bClient?(IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_LOCAL_WRITE):(IBV_ACCESS_REMOTE_READ | IBV_ACCESS_LOCAL_WRITE )),
    "Could not allocate mr, ibv_reg_mr. Do you have root privileges?");
  TEST_NZ(ibv_query_device(ctxt->ctxt,&dev_attr),"Could not query device values");
  // ctxt->max_sge = dev_attr.max_sge;
  ctxt->max_sge = 16; // it turns out that if we use a number greater than 16, Mellanox Infiniband card will report error.
  // TYPE of the card: InfiniBand: Mellanox Technologies MT26428 [ConnectX VPI PCIe 2.0 5GT/s - IB QDR / 10GigE] (rev a0)
  
  DEBUG_PRINT("initialization:max_sge=%d,max_cqe=%d\n",ctxt->max_sge,dev_attr.max_cqe);
  ctxt->max_mr  = dev_attr.max_mr;
  ctxt->max_cq  = dev_attr.max_cq;
  ctxt->max_cqe = dev_attr.max_cqe;
  // initialize mutex lock
  TEST_NZ(pthread_mutex_init(&ctxt->lock,NULL), "Could not initialize context mutex");
  // con_map
  TEST_Z(ctxt->con_map = MAP_INITIALIZE(con), "Could not initialize RDMA connection map");
  if(bClient){
    // client: bitmap
    TEST_Z(ctxt->bitmap = (uint8_t*)malloc(RDMA_CTXT_BYTES_BITMAP(ctxt)), "Could not allocate ctxt bitmap");
    memset(ctxt->bitmap, 0, RDMA_CTXT_BYTES_BITMAP(ctxt));
  }else{
    // blog: daemon thread
    TEST_NZ(pthread_create(&ctxt->daemon, NULL,
        blog_rdma_daemon_routine, (void*)ctxt),
      "Could not initialize the daemon routine");
  }
  return 0;
}

int destroyContext(RDMACtxt *ctxt){
  uint64_t len = MAP_LENGTH(con,ctxt->con_map);
  // STEP 1 - destroy Connections
  if(len > 0){
    uint64_t *keys = MAP_GET_IDS(con,ctxt->con_map,len);
    while(len--){
      RDMAConnection *conn;
      MAP_READ(con,ctxt->con_map,keys[len],&conn);
      MAP_DELETE(con,ctxt->con_map,keys[len]);
      if(ctxt->isClient){
        if(rdmaDisconnect(ctxt,GET_IP_FROM_KEY(keys[len]))!=0)
          fprintf(stderr, "Couldn't disconnect from server %lx:%d\n",keys[len],ctxt->port);
      }
      destroyRDMAConn(conn);
      free(conn);
    }
    free(keys);
  }
  // STEP 2 - destroy Context
  if(ctxt->mr)ibv_dereg_mr(ctxt->mr);
  if(ctxt->pd)ibv_dealloc_pd(ctxt->pd);
  if(ctxt->ctxt)ibv_close_device(ctxt->ctxt);
  if(ctxt->pool)free(ctxt->pool);
  if(!ctxt->isClient){//server
    pthread_kill(ctxt->daemon, 9);
  }else{//client
    free(ctxt->bitmap);
  }
  return 0;
}

int allocatePageArray(RDMACtxt *ctxt, void **pages, int num){
  // STEP 1 test Context mode, quit for client context
  if(ctxt->isClient){ /// client context
    fprintf(stderr,"Could not allocate page array in client mode");
    return -1;
  }
  // STEP 2 allocate pages
  if(num>RDMA_CTXT_NFPAGE(ctxt)){//we don't have enough space
    fprintf(stderr,"allocatePageArray():Could not allocate %d pages, because we have only %ld pages left.\n",num,RDMA_CTXT_NFPAGE(ctxt));
    return -2;
  }
  if(pthread_mutex_lock(&ctxt->lock)!=0){
    fprintf(stderr,"allocatePageArray():Could not get lock.");
    return -3;
  };

  if(num>RDMA_CTXT_NFPAGE(ctxt)){//we don't have enough pages
    pthread_mutex_unlock(&ctxt->lock);
    fprintf(stderr,"allocatePageArray():Could not allocate %d pages, because we have only %ld pages left.\n",num,RDMA_CTXT_NFPAGE(ctxt));
    return -2;
  }

  *pages = ctxt->pool + (ctxt->cnt << ctxt->align);
  ctxt->cnt += num;
  
  if(pthread_mutex_unlock(&ctxt->lock)!=0){
    fprintf(stderr,"allocatePageArray():Could not release lock.");
    return -3;
  }
  return 0;
}

int allocateBuffer(RDMACtxt *ctxt, void **buf){
  DEBUG_PRINT("allocateBuffer() begin:%ld buffers allocated.\n",ctxt->cnt);
  // STEP 1 test context mode, quit for blog context
  if(!ctxt->isClient){ /// blog context
    fprintf(stderr,"Could not allocate buffer in blog mode\n");
    return -1;
  }
  // STEP 2 test if we have enough space
  if(!RDMA_CTXT_NFBUF(ctxt)){//we don't have enough space
    fprintf(stderr,"allocateBuffer():Could not allocate buffer because we have %ld pages left.\n",RDMA_CTXT_NFPAGE(ctxt));
    return -2;
  }
  
  if(pthread_mutex_lock(&ctxt->lock)!=0){
    fprintf(stderr,"allocatePageArray():Could not get lock.");
    return -3;
  };

  if(!RDMA_CTXT_NFBUF(ctxt)){//we don't have enough space
    pthread_mutex_unlock(&ctxt->lock);
    fprintf(stderr,"allocateBuffer():Could not allocate buffer because we have %ld pages left.\n",RDMA_CTXT_NFPAGE(ctxt));
    return -2;
  }

  // STEP 3 find the buffer
  int32_t nbyte,nbit;
  /// 3.1 find the byte
  for(nbyte=0;nbyte<RDMA_CTXT_BYTES_BITMAP(ctxt);nbyte++){
    if(ctxt->bitmap[nbyte]==0xff || 
      (ctxt->psz-ctxt->align == 2 && ctxt->bitmap[nbyte] == 0xf0) ||
      (ctxt->psz-ctxt->align == 1 && ctxt->bitmap[nbyte] == 0xc0) ||
      (ctxt->psz-ctxt->align == 0 && ctxt->bitmap[nbyte] == 0xa0))
      continue;
    break;
  }
  if(nbyte==RDMA_CTXT_BYTES_BITMAP(ctxt)){
    pthread_mutex_unlock(&ctxt->lock);
    fprintf(stderr,"allocateBuffer():Could not allocate buffer: we should have %ld pages left buf found none [type I].\n",RDMA_CTXT_NFPAGE(ctxt));
    return -4;
  }
  /// 3.2 find the bit
  nbit=0;
  while(nbit < 8 && nbit < RDMA_CTXT_BITS_BITMAP(ctxt))
    if((0x1<<nbit)&ctxt->bitmap[nbyte])nbit++;
    else break;
  if(nbit==8 || nbit == RDMA_CTXT_BITS_BITMAP(ctxt)){
    pthread_mutex_unlock(&ctxt->lock);
    fprintf(stderr,"allocateBuffer():Could not allocate buffer: we should have %ld pages left buf found none [type II].\n",RDMA_CTXT_NFPAGE(ctxt));
    return -4;
  }
  // STEP 4 allocate the buffer
  ctxt->bitmap[nbyte] = ctxt->bitmap[nbyte]|(1<<nbit); /// fill bitmap
  *buf = ctxt->pool+(8*nbyte+nbit)*RDMA_CTXT_BUF_SIZE(ctxt);
  DEBUG_PRINT("allocateBuffer() found buffer slot[%p] @bitmap byte[%d], bit[%d].\n",(void*)(*buf - ctxt->pool),nbyte,nbit);
  ctxt->cnt++;

  if(pthread_mutex_unlock(&ctxt->lock)!=0){
    fprintf(stderr,"allocatePageArray():Could not release lock.");
    return -3;
  }
  DEBUG_PRINT("allocateBuffer() end:%ld buffers allocated.\n",ctxt->cnt);
  return 0;
}

int releaseBuffer(RDMACtxt *ctxt, const void *buf){
  // STEP 1 check mode
  if(!ctxt->isClient){ /// blog context
    fprintf(stderr,"Could not release buffer in blog mode");
    return -1;
  }
  // STEP 2 validate buf address
  uint32_t nbyte,nbit;
  nbyte = ((buf-ctxt->pool)/RDMA_CTXT_BUF_SIZE(ctxt))>>3;
  nbit  = ((buf-ctxt->pool)/RDMA_CTXT_BUF_SIZE(ctxt))&7;
  if(((uint64_t)(buf-ctxt->pool))%RDMA_CTXT_BUF_SIZE(ctxt)!=0 ||
      buf<ctxt->pool ||
      buf>=ctxt->pool+RDMA_CTXT_POOL_SIZE(ctxt)){
    fprintf(stderr,"releaseBuffer():invalid buf pointer=%p\n",buf);
    return -2;
  }
  if(pthread_mutex_lock(&ctxt->lock)!=0){
    fprintf(stderr,"releaseBuffer():Could not get lock.");
    return -3;
  };
  if(!ctxt->bitmap[nbyte]&(1<<nbit)){
    pthread_mutex_unlock(&ctxt->lock);
    fprintf(stderr,"releaseBuffer():buffer[%p] is not allocated.\n",buf);
    return -4;
  }else{
    ctxt->bitmap[nbyte]^=(1<<nbit);
    ctxt->cnt--;
  }
  if(pthread_mutex_unlock(&ctxt->lock)!=0){
    fprintf(stderr,"releaseBuffer():Could not release lock.");
    return -3;
  }
  return 0;
}

int rdmaConnect(RDMACtxt *ctxt, const uint32_t hostip){
  DEBUG_PRINT("connecting to:%d:%d\n",hostip,ctxt->port);
  // STEP 0: if connection exists?
  const uint64_t cipkey = (const uint64_t)MAKE_CON_KEY(hostip,ctxt->port);
  int bDup;
  MAP_LOCK(con, ctxt->con_map, cipkey, 'r' );
  RDMAConnection *rdmaConn = NULL, *readConn = NULL;
  bDup = (MAP_READ(con,ctxt->con_map,cipkey,&readConn)==0);
  MAP_UNLOCK(con, ctxt->con_map, cipkey);
  if(bDup){
    //fprintf(stderr,"Cannot connect because the connection is established already.");
    return -1; // the connection exists already.
  }
  // STEP 1: connect to server
  struct sockaddr_in svraddr;
  int connfd = socket(AF_INET, SOCK_STREAM, 0);
  bzero((char*)&svraddr,sizeof(svraddr));
  svraddr.sin_family = AF_INET;
  svraddr.sin_port = htons(ctxt->port);
  svraddr.sin_addr.s_addr = hostip;
  int retry = 2;
  while(retry){
    retry --;
    if(connect(connfd,(const struct sockaddr *)&svraddr,sizeof(svraddr))<0){
      if(!retry){
        fprintf(stderr,"cannot connect to server:%x:%d, errno=%d\n",hostip,ctxt->port,errno);
        return -2;
      }
      sleep(30); // sleep 30 sec.
    }else // succeed
      break;
  }
  // STEP 2: setup connection
  rdmaConn = (RDMAConnection*)malloc(sizeof(RDMAConnection));
  rdmaConn->scq  = NULL;
  rdmaConn->rcq  = NULL;
  rdmaConn->qp   = NULL;
  rdmaConn->ch   = NULL;
  rdmaConn->port = 1; // always use 1? // lets make it configurable later.
  TEST_Z(rdmaConn->ch=ibv_create_comp_channel(ctxt->ctxt),"Could not create completion channel, ibv_create_comp_channel");
  TEST_Z(rdmaConn->rcq=ibv_create_cq(ctxt->ctxt,1,NULL,NULL,0),"Could not create receive completion queue, ibv_create_cq");
  TEST_Z(rdmaConn->scq=ibv_create_cq(ctxt->ctxt,MAX_SQE,rdmaConn,rdmaConn->ch,0),"Could not create send completion queue, ibv_create_cq");
  struct ibv_qp_init_attr qp_init_attr = {
    .send_cq = rdmaConn->scq,
    .recv_cq = rdmaConn->rcq,
    .qp_type = IBV_QPT_RC,
    .cap = {
      .max_send_wr = 1, // actuall the client does not send
      .max_recv_wr = MAX_SQE, // MAX_SQE must less than ctxt->max_cqe
      .max_send_sge = 1,
      .max_recv_sge = ctxt->max_sge,
      .max_inline_data = 0
    }
  };
  TEST_Z(rdmaConn->qp=ibv_create_qp(ctxt->pd,&qp_init_attr),"Could not create queue pair for read, ibv_create_qp");
  qp_change_state_init(rdmaConn->qp,rdmaConn->port);
  struct ibv_port_attr port_attr;
  TEST_NZ(ibv_query_port(ctxt->ctxt,rdmaConn->port,&port_attr),"Could get port attributes, ibv_query_port");
  rdmaConn->global=1;
  TEST_NZ(ibv_query_gid(ctxt->ctxt,rdmaConn->port,0,&rdmaConn->l_gid),"Could not get gid from port");
  rdmaConn->l_lid = port_attr.lid;
  rdmaConn->l_qpn = rdmaConn->qp->qp_num;
  rdmaConn->l_psn = lrand48() & 0xffffff;
  rdmaConn->l_rkey = ctxt->mr->rkey;
  rdmaConn->l_vaddr = (uintptr_t)ctxt->pool;
  // STEP 3: exchange the RDMA info
  IbConEx l_exm,r_exm; // echange message
  //// client --> server | connection string
  setibcfg(&l_exm,rdmaConn);
  if(write(connfd, &l_exm, sizeof l_exm)!=sizeof l_exm){
    perror("Could not send ibcfg to peer");
    return -3;
  }
  //// server --> client | connection string | put connection to map
  if(read(connfd, &r_exm, sizeof r_exm)!=sizeof r_exm){
    perror("Could not receive ibcfg from peer.");
    return -4;
  }
  rdmaConn->r_gid = r_exm.gid;
  rdmaConn->r_lid = r_exm.lid;
  rdmaConn->r_qpn = r_exm.qpn;
  rdmaConn->r_psn = r_exm.psn;
  rdmaConn->r_rkey = r_exm.rkey;
  rdmaConn->r_vaddr = r_exm.vaddr;
  close(connfd);
  /// change pair to RTR
  qp_change_state_rtr(rdmaConn->qp,rdmaConn);
  // STEP 4: setup the RDMA map
  MAP_LOCK(con, ctxt->con_map, cipkey, 'w');
  if(MAP_CREATE_AND_WRITE(con, ctxt->con_map, cipkey, rdmaConn)!=0){
    MAP_UNLOCK(con, ctxt->con_map, cipkey);
    close(connfd);
    destroyRDMAConn(rdmaConn);
    free(rdmaConn);
    return -6;
  }
  MAP_UNLOCK(con, ctxt->con_map, cipkey);
  DEBUG_PRINT("client %d is connected.\n",hostip);
  return 0;
}

int rdmaDisconnect(RDMACtxt *ctxt, const uint32_t hostip){
  DEBUG_PRINT("disconnecting from:%d:%d\n",hostip,ctxt->port);
  // STEP 1: connect to server
  struct sockaddr_in svraddr;
  int connfd = socket(AF_INET, SOCK_STREAM, 0);
  bzero((char*)&svraddr,sizeof(svraddr));
  svraddr.sin_family = AF_INET;
  svraddr.sin_port = htons(ctxt->port);
  svraddr.sin_addr.s_addr = hostip;
  if(connect(connfd,(const struct sockaddr *)&svraddr,sizeof(svraddr))<0){
    fprintf(stderr,"cannot connect to server:%x:%d\n",hostip,ctxt->port);
    return -1;
  }
  int flag = 1;
  if(setsockopt(connfd,IPPROTO_TCP,TCP_NODELAY,(char*)&flag,sizeof(int)) < 0){
    fprintf(stderr,"cannot set tcp nodelay.\n");
    return -2;
  }
  
  // STEP 2: send an empty cfg
  IbConEx exm;
  exm.req = REQ_DISCONNECT;
  exm.pid = getpid();
  if(write(connfd, &exm, sizeof exm) != sizeof exm){
    perror("Could not send ibcfg to peer");
    return -1;
  }
  fsync(connfd);

  // finish
  close(connfd);
  DEBUG_PRINT("disconnected from host %x.\n",hostip);
  return 0;
}

int rdmaTransfer(RDMACtxt *ctxt, const uint32_t hostip, const uint32_t pid, const uint64_t r_vaddr, const void **pagelist, int npage,int iswrite, int pagesize){
DEBUG_PRINT("rdmaTransfer:\n\tisWrite=%d\n\tnpage=%d\n\tfirst page=%p\n\tr_vaddr=%p\nhostip=%x,pid=%d\n",iswrite,npage,*pagelist,(void*)r_vaddr,hostip,pid);
  struct timeval tv1,tv2,tv3,tv4,tv21,tv22,tv23,tv221,tv222;
  const uint64_t cipkey = (const uint64_t)MAKE_CON_KEY(hostip,pid);
  RDMAConnection *rdmaConn = NULL;
  // STEP 1: get the connection
  gettimeofday(&tv1,NULL);
  MAP_LOCK(con, ctxt->con_map, cipkey, 'w');
  if(MAP_READ(con, ctxt->con_map, cipkey, &rdmaConn)!=0){
    fprintf(stderr, "Cannot find the context for ip:%lx",cipkey);
    return -1;
  }
  // STEP 2: finish transfer and return
  gettimeofday(&tv2,NULL);
  struct ibv_sge *sge_list = malloc(sizeof(struct ibv_sge)*ctxt->max_sge);
  struct ibv_send_wr wr;
  wr.wr.rdma.remote_addr = r_vaddr;
  wr.wr.rdma.rkey = rdmaConn->r_rkey;
  wr.wr_id = iswrite?RDMA_WRID:RDMA_RDID;
  wr.sg_list = sge_list;
//  wr.num_sge = npage;
  wr.opcode = iswrite?IBV_WR_RDMA_WRITE:IBV_WR_RDMA_READ;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.next = NULL;
  struct ibv_send_wr *bad_wr;
  TEST_NZ(ibv_req_notify_cq(rdmaConn->scq,0),"Could not request notification from sending completion queue, ibv_req_notify_cq()");
  int opCnt=0;//number of transfer
  int npageToProcess=npage;
  gettimeofday(&tv21,NULL);
  while(npageToProcess > 0){
    int i;
    int batchSize = (npageToProcess > ctxt->max_sge)?ctxt->max_sge:npageToProcess;
    // prepare the sge_list
    for(i=0;i<batchSize;i++){
      (sge_list+i)->addr = (uintptr_t)pagelist[npage-npageToProcess+i];
      (sge_list+i)->length = (pagesize==0)?RDMA_CTXT_PAGE_SIZE(ctxt):pagesize;
      (sge_list+i)->lkey = rdmaConn->l_rkey;
    }
    wr.num_sge = batchSize;
    npageToProcess-=batchSize;
    opCnt ++;
    // 
    int rSend = ibv_post_send(rdmaConn->qp,&wr,&bad_wr);
    if(rSend!=0){
      fprintf(stderr,"ibv_post_send failed with error code:%d\n",rSend);
      return -1; // write failed.
    }
    wr.wr.rdma.remote_addr += batchSize * RDMA_CTXT_PAGE_SIZE(ctxt);
  }
  gettimeofday(&tv22,NULL);
  DEBUG_PRINT("rdmaTransfer: batchSize=%d\n",opCnt);
  struct ibv_wc *wc = (struct ibv_wc*)malloc(opCnt*sizeof(struct ibv_wc));
  gettimeofday(&tv221,NULL);
  // wait on completion queue
  void *cq_ctxt;
  TEST_NZ(ibv_get_cq_event(rdmaConn->ch, &rdmaConn->scq, &cq_ctxt), "ibv_get_cq_event failed!");
  gettimeofday(&tv222,NULL);
  ibv_ack_cq_events(rdmaConn->scq,1);
  gettimeofday(&tv23,NULL);
  // get completion event.
  int ne=0;
  while(ne<opCnt){
    int i,npe = 0; // polled entry;
    do{
      npe = ibv_poll_cq(rdmaConn->scq,opCnt,wc);
    }while(npe==0);
    if(npe<0){
      fprintf(stderr, "%s: poll CQ failed %d\n", __func__, ne);
      return -2;
    }
    for(i=0;i<npe;i++)
    if((wc+i)->status != IBV_WC_SUCCESS) {
      fprintf(stderr, "%s: rdma transfer failed,wc[%d]->status=%d.\n",__func__,i,(wc+i)->status);
      return -3;
    }
    ne += npe;
  }
  gettimeofday(&tv3,NULL);
  free(sge_list);
  free(wc);
  MAP_UNLOCK(con, ctxt->con_map, cipkey);
  gettimeofday(&tv4,NULL);
  DEBUG_PRINT("rdmaTransfer:\t%ld\t%ld\n",
    (tv2.tv_sec-tv1.tv_sec)*1000000+tv2.tv_usec-tv1.tv_usec,
    (tv3.tv_sec-tv2.tv_sec)*1000000+tv3.tv_usec-tv2.tv_usec);
  DEBUG_PRINT("break down:\t%ld\t%ld\t%ld\t%ld\n",
    (tv21.tv_sec-tv2.tv_sec)*1000000+tv21.tv_usec-tv2.tv_usec,
    (tv22.tv_sec-tv21.tv_sec)*1000000+tv22.tv_usec-tv21.tv_usec,
    (tv23.tv_sec-tv22.tv_sec)*1000000+tv23.tv_usec-tv22.tv_usec,
    (tv3.tv_sec-tv23.tv_sec)*1000000+tv3.tv_usec-tv23.tv_usec);
  DEBUG_PRINT("bbd:\t%ld\t%ld\t%ld\n",
    (tv221.tv_sec-tv22.tv_sec)*1000000+tv221.tv_usec-tv21.tv_usec,
    (tv222.tv_sec-tv221.tv_sec)*1000000+tv222.tv_usec-tv221.tv_usec,
    (tv23.tv_sec-tv222.tv_sec)*1000000+tv23.tv_usec-tv222.tv_usec);
  return 0;
}

inline int isBlogCtxt(const RDMACtxt * ctxt){
  return (ctxt->bitmap==NULL);
}

inline const uint32_t getip(const char* ipstr){
  return (const uint32_t)inet_addr(ipstr);
}
