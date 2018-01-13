#include <stdlib.h>
#include <string.h>
#include <netinet/tcp.h>
#include "LibFabricRDMA.h"

////////////////////////////////////////////////
// Internal static data                       //
////////////////////////////////////////////////
MAP_DEFINE(con,LFConn,10);  

////////////////////////////////////////////////
// Internal tools.                            //
// DIE_NZ means "make sure return non-zero"  //
// DIE_Z means "make sure return zero"       //
// DIE_N means "make sure return NonNegtive" //
////////////////////////////////////////////////
#define DIE_NZ(x,y) do { if ((x)) die(y); } while (0)
#define DIE_Z(x,y) do { if ((x)==0) die(y); } while (0)
#define DIE_N(x,y) do { if ((x)<0) die(y); } while (0)
#define MAKE_CON_KEY(host,port_or_pid) ((((long)(host))<<32)|(port_or_pid))
#define GET_IP_FROM_KEY(key)    ((const uint32_t)(((key)>>32)&0x00000000FFFFFFFF))
#define GET_SVR_PORT_FROM_KEY(key)    ((const uint32_t)(key)&0x00000000FFFFFFFF)
#define GET_PID_FROM_KEY(key)    GET_SVR_PORT_FROM_KEY(key)
#define MIN(x,y) ((x)>(y)?(y):(x))
#define LF_VERSION FI_VERSION(1,5)

static int die(const char *reason){
  fprintf(stderr, "Err: %s - %s \n ", strerror(errno), reason);
  exit(EXIT_FAILURE);
  return -1;
}

#define CALL_FI_API(x,desc,ret) \
  do { \
    ret = (x); \
    if (ret != 0) {\
      fprintf(stderr,"%s:call LibFabric API (%s) failed with error(%d), %s.\n",__func__,desc,ret,fi_strerror(-ret)); \
      return ret; \
    } \
  } while (0)

// default the value of a context
static int default_context(struct lf_ctxt *ct) {
  ct->hints = fi_allocinfo();
  if(!ct->hints) {
    fprintf(stderr, "%s:fi_allocinfo() failed.\n", __func__ );
    return -1;
  }

  ct->hints->caps = FI_MSG|FI_RMA|FI_READ|FI_WRITE|FI_REMOTE_READ|FI_REMOTE_WRITE; // should support fi_send/fi_recv/fi_read,fi_write
  ct->hints->mode = ~0; // all mode

  ct->hints->ep_attr->type = FI_EP_MSG; // use connection based endpoint by default.

  ct->hints->domain_attr->mode = ~0;
  ct->hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_VIRT_ADDR | FI_MR_ALLOCATED | FI_MR_PROV_KEY;

  ct->hints->tx_attr->rma_iov_limit = DEFAULT_SGE_BAT_SIZE;
  ct->hints->tx_attr->iov_limit = DEFAULT_SGE_BAT_SIZE;
  ct->hints->rx_attr->iov_limit = DEFAULT_SGE_BAT_SIZE;

  if (ct->cq_attr.format == FI_CQ_FORMAT_UNSPEC)
    ct->cq_attr.format = FI_CQ_FORMAT_CONTEXT;
  ct->cq_attr.wait_obj = FI_WAIT_NONE;

  // TODO: more configurations could be found at
  // ct->hintS->tx/rx_attr,ep_attr,domain_attr,fabric_addr;

  ct->port = LFPF_SERVER_PORT;
  ct->local_ep_addr_len = MAX_LF_ADDR_SIZE;
  ct->sge_bat_size = DEFAULT_SGE_BAT_SIZE;
  ct->tx_depth = DEFAULT_TRANS_DEPTH;

  return 0;
}

// apply extra config
static void apply_extra_config(struct lf_ctxt *ct, const struct lf_extra_config * conf) {
  if (conf == NULL){
    return;
  }
  if (conf->mask&EXTRA_CONFIG_TX_DEPTH){
    ct->tx_depth = conf->tx_depth;
  }
  if (conf->mask&EXTRA_CONFIG_SGE_BAT_SIZE){
    ct->sge_bat_size = conf->sge_bat_size;
  }
}

//get the peer ip address represented by uint32_t, 0 for error.
static inline uint32_t get_peer_ip(int sockfd) {
  struct sockaddr_in addr;
  socklen_t addrLen = sizeof(struct sockaddr_in);
  if (getpeername(sockfd, (struct sockaddr*)&addr, &addrLen) == -1) {
    fprintf(stderr, "%s:getpeername failed with error %d\n",__func__,errno);
    return 0;
  }
  return (uint32_t)addr.sin_addr.s_addr;
}

static int destroyLFConn(LFConn *lfconn) {
  int ret;
  DEBUG_PRINT("%s:destroyLFConn:lfconn=%p,lfconn->txcq=%p\n",__func__,lfconn,lfconn->txcq);
  CALL_FI_API(fi_close(&lfconn->ep->fid),"close endpoint",ret);
  CALL_FI_API(fi_close(&lfconn->txcq->fid),"close txcq",ret);
  CALL_FI_API(fi_close(&lfconn->rxcq->fid),"close rxcq",ret);
  CALL_FI_API(fi_close(&lfconn->eq->fid),"close eventqueue",ret);
  return ret;
}

// Initialize the fabric and domains on server or client side.
static int init_fabric_and_domain(struct lf_ctxt *ct) {
  int ret = 0;
  DEBUG_PRINT("Context hints:%s\n",fi_tostr(ct->hints,FI_TYPE_INFO));
  CALL_FI_API(fi_getinfo(LF_VERSION,NULL,NULL,0,ct->hints,&(ct->fi)),"fi_getinfo()",ret);
  DEBUG_PRINT("Context getinfo:%s\n",fi_tostr(ct->fi,FI_TYPE_INFO));
  CALL_FI_API(fi_fabric(ct->fi->fabric_attr, &(ct->fabric), NULL),"fi_fabric()",ret);
  CALL_FI_API(fi_domain(ct->fabric, ct->fi, &(ct->domain), NULL), "fi_domain()",ret);
  return ret;
}

// Initialize the passive endpoint on the server side:
static int init_server_pep(struct lf_ctxt *ct) {
  int ret = 0;

  CALL_FI_API(fi_eq_open(ct->fabric, &(ct->eq_attr), &(ct->eq), NULL),"fi_eq_open()",ret);
  CALL_FI_API(fi_passive_ep(ct->fabric,ct->fi,&(ct->pep),NULL),"fi_passive_ep()",ret);
  CALL_FI_API(fi_pep_bind(ct->pep, &(ct->eq->fid), 0),"fi_pep_bind()",ret);
  CALL_FI_API(fi_listen(ct->pep),"fi_listen",ret);
  CALL_FI_API(fi_getname(&ct->pep->fid, ct->local_ep_addr, &ct->local_ep_addr_len),"fi_getname()",ret);
  if (ct->local_ep_addr_len > sizeof ct->local_ep_addr) {
    fprintf(stderr, "%s:local name is too long(%ld) to fit in ct->local_ep_addr(%ld bytes).\n",
      __func__,ct->local_ep_addr_len,sizeof(ct->local_ep_addr));
    ret = -1;
  }
  return ret;
}

/**
 * initialize endpoint
 */
static int init_endpoint(struct lf_ctxt *ct, struct lf_conn *conn, struct fi_info *fi) {
  int ret;
  struct fi_cq_attr cq_attr = ct->cq_attr;

  // 1 - open completion queues
  cq_attr.size = fi->tx_attr->size;
  ret = fi_cq_open(ct->domain, &cq_attr, &(conn->txcq), &(conn->txcq));
  if (ret) {
    fprintf(stderr, "fi_cq_open txcq failed with error:%d, %s\n", -ret, fi_strerror(-ret));
    return ret;
  }
  DEBUG_PRINT("%s:conn->txcq = %p.\n",__func__,conn->txcq);
  ret = fi_cq_open(ct->domain, &cq_attr, &(conn->rxcq), &(conn->rxcq));
  if (ret) {
    fprintf(stderr, "fi_cq_open txcq failed with error:%d, %s\n", -ret, fi_strerror(-ret));
    return ret;
  }
  // 2 - open endpoint
  ret = fi_endpoint(ct->domain, fi, &(conn->ep), NULL);
  if (ret) {
    fprintf(stderr, "fi_endpoint failed with error: %d, %s\n", -ret, fi_strerror(-ret));
    return ret;
  }
  // 3 - open event queue
  ret = fi_eq_open(ct->fabric,&(ct->eq_attr),&(conn->eq), NULL);
  if (ret) {
    fprintf(stderr, "fi_eq_open failed with error :%d, %s\n", -ret, fi_strerror(-ret));
    return ret;
  }
  // 3 - bind them together
  ret = fi_ep_bind(conn->ep, &(conn->eq)->fid, 0);
  if (ret) {
    fprintf(stderr, "binding event queue failed: %d\n",ret);
    return ret;
  }
  if(conn->txcq)
    ret = fi_ep_bind(conn->ep, &(conn->txcq)->fid, FI_TRANSMIT);
  if (ret) {
    fprintf(stderr, "binding tx completion queue queue failed: %d, %s\n",ret, fi_strerror(-ret));
    return ret;
  }
  if(conn->rxcq)
    ret = fi_ep_bind(conn->ep, &(conn->rxcq)->fid, FI_RECV);
  if (ret) {
    fprintf(stderr, "binding rx completion queue queue failed: %d\n",ret);
    return ret;
  }
  ret = fi_enable(conn->ep); // enable endpoint
  if (ret) {
    fprintf(stderr, "enable endpoint failed with error: %d\n",ret);
    return ret;
  }
  return 0;
}

// server responds to client connection request
void lf_server_connect(const uint64_t cipkey, int connfd, struct lf_ctxt *ct) {
  ssize_t nRead,nWrite;
  LFConn *lfconn;
  struct fi_eq_cm_entry entry;
  uint32_t event;
  int fi_ret,is_dup;

  DEBUG_PRINT("Received connect from client cipkey = 0x%lx  \n",cipkey);
  MAP_LOCK(con, ct->con_map, cipkey, 'r');
  is_dup = (MAP_READ(con, ct->con_map, cipkey, &lfconn)==0);
  MAP_UNLOCK(con, ct->con_map, cipkey);
  if(is_dup){
    fprintf(stderr, "%s:connection with key:%lx already exists...skip new connection.\n",__func__,cipkey);
    return;// return success for duplicated connect...
  }
  lfconn = (LFConn*)malloc(sizeof(LFConn));
  if (lfconn==NULL) {
    fprintf(stderr, "%s:failed to allocate memory for new LFConn with error=%d.\n",__func__,errno);
    return;
  }

  // 2. CLIENT  <=[ PEP ADDR    ]==  SERVER
  nWrite = send(connfd,(void *)&ct->local_ep_addr_len,sizeof(ct->local_ep_addr_len),0);
  if (nWrite >= 0){
    nWrite = send(connfd,(void *)ct->local_ep_addr,ct->local_ep_addr_len,0);
  }
  if (nWrite < 0) {
    fprintf(stderr, "%s:failed to send PEP ADDR len to client with errno:%d\n",__func__,errno);
    free(lfconn);
    return;
  }

  // 7. SERVER waits for connection requests from LibFabrics
  DEBUG_PRINT("waiting for connections.\n");
  nRead = fi_eq_sread(ct->eq, &event, &entry, sizeof(entry), -1, 0);
  if (nRead != sizeof(entry)) {
    fprintf(stderr,"%s,failed to read connection state with error:%ld\n",__func__,nRead);
    free(lfconn);
    return; 
  }
  if (event != FI_CONNREQ) {
    fprintf(stderr, "Unexpected CM event %d, skip it.\n", event);
    fi_reject(ct->pep, entry.info->handle, NULL, 0);
    fi_freeinfo(entry.info);
    free(lfconn);
    return;
  }

  // 8. SERVER creates the endpoint context(LFConn)
  fi_ret = init_endpoint(ct,lfconn,entry.info);
  if (fi_ret != 0) {
    fprintf(stderr, "Failed to create LFConn contexts...skip it\n");
    fi_reject(ct->pep, entry.info->handle, NULL, 0);
    fi_freeinfo(entry.info);
    free(lfconn);
    return;
  }
  //// accept it.
  fi_ret = fi_accept(lfconn->ep, NULL, 0);
  if (fi_ret != 0) {
    fprintf(stderr, "fi_accept failed with error: %d, %s\n", fi_ret, fi_strerror(-fi_ret));
    fi_reject(ct->pep, entry.info->handle, NULL, 0);
    fi_freeinfo(entry.info);
    free(lfconn);
    return;
  }
  //// release the entry.info...
  fi_freeinfo(entry.info);

  //10. CLIENT <=[ MKey, PID, and VADDR]== SERVER
  LF_HS_III hsiii,rhsiii;
  hsiii.mr_key = ct->local_mr_key;
  hsiii.vaddr = (fi_addr_t)ct->pool;
  nWrite = write(connfd,(void *)&hsiii,sizeof(hsiii));
  //10. CLIENT ==[ MKey, PID, and VADDR]=> SERVER
  nRead = recv(connfd, (void *)&rhsiii,sizeof(rhsiii), MSG_WAITALL);
  if (nRead != sizeof(rhsiii)) {
    fprintf(stderr, "%s,Failed to read LF_HS_III from client.\n",__func__);
    free(lfconn);
    return;
  }
  lfconn->r_rkey = rhsiii.mr_key;
  lfconn->remote_fi_addr = rhsiii.vaddr;
  lfconn->pid = GET_PID_FROM_KEY(cipkey);
  //11. Both sides insert this to LFConn
  //// we just skip the pid in LF_HS_III
  // insert it into map
  MAP_LOCK(con, ct->con_map, cipkey, 'w');
  if(MAP_CREATE_AND_WRITE(con, ct->con_map, cipkey, lfconn)!=0){
    MAP_UNLOCK(con, ct->con_map, cipkey);
    destroyLFConn(lfconn);
    free(lfconn);
    fprintf(stderr, "%s:Cannot insert lfconn to map.\n",__func__);
    return;
  }
  MAP_UNLOCK(con, ct->con_map, cipkey);
  //DEBUG_PRINT("Connection established for client cipkey = 0x%lx, pid=0x%x, remote_fi_addr=0x%lx, vaddr=0x%lx, \n",
  fprintf(stdout,"Connection established for client cipkey = 0x%lx, pid=0x%x, remote_fi_addr=0x%lx, vaddr=0x%lx, \n",
    cipkey,lfconn->pid,lfconn->remote_fi_addr,lfconn->r_rkey);
}

// server responds gto client disconnection request
void lf_server_disconnect(const uint64_t cipkey, int connfd, struct lf_ctxt *ct, LF_HS_I1 * req) {
  LFConn *lfconn = NULL;

  MAP_LOCK(con, ct->con_map, cipkey, 'w');
  if(MAP_READ(con, ct->con_map, cipkey, &lfconn) == 0){
    MAP_DELETE(con,ct->con_map,cipkey);
  }
  MAP_UNLOCK(con, ct->con_map, cipkey);

  if (lfconn) {
    destroyLFConn(lfconn);
    free(lfconn);
  }

  if (send(connfd,req,sizeof(LF_HS_I1),0) < 0) {
    fprintf(stderr,"%s:Failed to write echo to the other side with error %d.\n",__func__,errno);
    return;
  }
/*
  if (fsync(connfd) < 0) {
    fprintf(stderr, "%s:Failed to flush echo to the other side with error %d.\n",__func__,errno);
  }
*/
}

// client connect procedure
int lf_client_connect(const uint64_t cipkey, int connfd, struct lf_ctxt *ct) {
  LFConn *lfconn = NULL;
  int ret = 0;
  ssize_t nRead,nWrite;
  ssize_t remote_ep_addr_len;
  char remote_ep_addr[MAX_LF_ADDR_SIZE];
  struct fi_eq_cm_entry entry;
  uint32_t event;
  LF_HS_I1 hsi1;

  lfconn = (LFConn*)malloc(sizeof(LFConn));
  if (lfconn == NULL) {
    fprintf(stderr, "%s:Cannot allocate LFConn data structure with error %d...connection failed.\n",__func__,errno);
    MAP_UNLOCK(con, ct->con_map, cipkey);
    return -2;
  }
  // 1. CLIENT  ==[ CONNECT REQ ]=>  SERVER
  hsi1.req = REQ_CONNECT;
  hsi1.pid = getpid();
  nWrite = send(connfd,(void*)&hsi1,sizeof(LF_HS_I1),0);
  if (nWrite < 0) {
    fprintf(stderr, "%s:cannot set connection request 1 to server, error=%d.\n",__func__,errno);
    MAP_UNLOCK(con, ct->con_map, cipkey);
    free(lfconn);
    return -3;
  }

  // 2. CLIENT  <=[ PEP ADDR    ]==  SERVER
  nRead = recv(connfd,&remote_ep_addr_len,sizeof(remote_ep_addr_len),MSG_WAITALL);
  if (nRead != sizeof(remote_ep_addr_len)) {
    fprintf(stderr, "%s:cannot read the endpoint address length from the server.errno=%d.\n",__func__,errno);
    free(lfconn);
    return -4;
  }
  nRead = recv(connfd,&remote_ep_addr,remote_ep_addr_len,MSG_WAITALL);
  if (nRead != remote_ep_addr_len) {
    fprintf(stderr, "%s:cannot read the endpoint address from the server.errno=%d.\n",__func__,errno);
    free(lfconn);
    return -5;
  }
  DEBUG_PRINT("remote_ep_addr_len=%ld\n",remote_ep_addr_len);
  DEBUG_PRINT("remote_ep_addr= [%x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x]\n",
    remote_ep_addr[0]&0xff,remote_ep_addr[1]&0xff,remote_ep_addr[2]&0xff,remote_ep_addr[3]&0xff,
    remote_ep_addr[4]&0xff,remote_ep_addr[5]&0xff,remote_ep_addr[6]&0xff,remote_ep_addr[7]&0xff,
    remote_ep_addr[8]&0xff,remote_ep_addr[9]&0xff,remote_ep_addr[10]&0xff,remote_ep_addr[11]&0xff,
    remote_ep_addr[12]&0xff,remote_ep_addr[13]&0xff,remote_ep_addr[14]&0xff,remote_ep_addr[15]&0xff); 
  // * PHASE II: establish LibFabric connection
  struct fi_info * client_hints = fi_dupinfo(ct->hints);
  struct fi_info * client_info = NULL;
  if(client_hints == NULL) {
    fprintf(stderr,"%s:Cannot duplicate client_hints.",__func__);
    free(lfconn);
    return -8;
  }
  client_hints->dest_addr = malloc(remote_ep_addr_len);
  if(client_hints->dest_addr == NULL) {
    fprintf(stderr,"%s:Cannot malloc dest_addr.",__func__);
    fi_freeinfo(client_hints);
    free(lfconn);
    return -9;
  }
  memcpy(client_hints->dest_addr,remote_ep_addr,remote_ep_addr_len);
  client_hints->dest_addrlen = remote_ep_addr_len;
  ret = fi_getinfo(LF_VERSION,NULL,NULL,0,client_hints,&client_info);
  if (ret != 0) {
    fprintf(stderr,"%s:fi_getinfo() failed with err:%d,%s",__func__,-ret,fi_strerror(-ret));
    fi_freeinfo(client_hints);
    free(lfconn);
    return -10;
  }
  // * 5. CLIENT initializes the endpoint
  if (init_endpoint(ct,lfconn,client_info) != 0) {
    fprintf(stderr, "%s:failed to initialize endpoint.",__func__);
    fi_freeinfo(client_hints);
    fi_freeinfo(client_info);
    free(lfconn);
    return -6;
  }
  // * 6. CLIENT  --[ fi_connect  ]->  SERVER
  CALL_FI_API(fi_connect(lfconn->ep, remote_ep_addr, NULL, 0),"fi_connect()",ret);
  nRead = fi_eq_sread(lfconn->eq, &event, &entry, sizeof(entry), -1, 0);
  if (nRead != sizeof(entry)) {
    fprintf(stderr, "%s: Unknown CM event: fi_eq_sread() return ret(%ld)!=sizeof(entry)\n",__func__,nRead);
    fi_freeinfo(client_hints);
    fi_freeinfo(client_info);
    free(lfconn);
    return -7;
  }
  // * 9. CLIENT  <-[ fi_accept   ]--  SERVER
  if (event != FI_CONNECTED || entry.fid != &(lfconn->ep->fid)) {
    fprintf(stderr,"%s,Unexpected CM event %d fid %p (ep %p)\n",
      __func__, event, entry.fid, lfconn->ep);
    fi_freeinfo(client_hints);
    fi_freeinfo(client_info);
    free(lfconn);
    return -8;
  }

  //10. CLIENT ==[ MKey, PID, and VADDR]=> SERVER
  LF_HS_III hsiii,rhsiii;
  hsiii.mr_key = ct->local_mr_key;
  hsiii.vaddr = (fi_addr_t)ct->pool;
  nWrite = write(connfd,(void *)&hsiii,sizeof(hsiii));
  //10. CLIENT <=[ MKey, PID, and VADDR]== SERVER
  nRead = recv(connfd, (void *)&rhsiii,sizeof(rhsiii), MSG_WAITALL);
  if (nRead != sizeof(hsiii)) {
    fprintf(stderr, "%s,Failed to read LF_HS_III from server.\n",__func__);
    fi_freeinfo(client_hints);
    fi_freeinfo(client_info);
    free(lfconn);
    return -9;
  }

  //11. Both sides insert this to LFConn
  // insert it into map
  if(MAP_CREATE_AND_WRITE(con, ct->con_map, cipkey, lfconn)!=0){
    MAP_UNLOCK(con, ct->con_map, cipkey);
    destroyLFConn(lfconn);
    fi_freeinfo(client_hints);
    fi_freeinfo(client_info);
    free(lfconn);
    fprintf(stderr, "%s:Cannot insert lfconn to map.\n",__func__);
    return -10;
  }

  // release client info
  fi_freeinfo(client_hints);
  fi_freeinfo(client_info);
  return ret;
}

// client disconnect procedure
#define lf_client_disconnect(ct,ip) LFDisconnect(ct,ip)
// void lf_client_disconnect(int connfd, uint32_t serverip) {
// }

// the libfabric server daemon routine
void* lf_daemon_routine(void *param) {
  DEBUG_PRINT("%s:Start the server daemon.\n",__func__);

  struct lf_ctxt *ct = (struct lf_ctxt *)param;
  int sockfd = -1;
  struct addrinfo *res;
  struct addrinfo hints = {
    .ai_flags = AI_PASSIVE,
    .ai_family = AF_UNSPEC,
    .ai_socktype = SOCK_STREAM
  };
  char *service;
  int n,connfd;

  // STEP 1: initialize the passive endpoints
  DIE_NZ(init_server_pep(ct),"ERROR init_server_pep()");

  // STEP 2: initialize the socket server port
  DIE_N(asprintf(&service,"%d", ct->port), "ERROR writing port number to port string.");
  DIE_NZ(n=getaddrinfo(NULL,service,&hints,&res), "getaddrinfo threw error");
  DIE_N(sockfd=socket(res->ai_family, res->ai_socktype, res->ai_protocol), "Could not create server socket");
  setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);
  DIE_NZ(bind(sockfd,res->ai_addr,res->ai_addrlen), "Could not bind addr to socket");
  listen(sockfd, 10);
  while(1) {
    struct sockaddr_in clientAddr;
    LF_HS_I1 hsi1;
    socklen_t addrLen = sizeof(struct sockaddr_in);
    uint32_t clientip;
    uint64_t cipkey;
    ssize_t nRead;
    DIE_N(connfd = accept(sockfd, (struct sockaddr*)&clientAddr, &addrLen), "server accept failed.");

    // 1. CLIENT  ==[ DIS/CONNECT REQ ]=>  SERVER
    nRead = recv(connfd,(void *)&hsi1,sizeof hsi1,MSG_WAITALL);
    if (nRead != sizeof hsi1) {
      fprintf(stderr, "%s:failed to read PHASE I request...skip it.\n", __func__);
      close(connfd);
      continue;
    }
    clientip = get_peer_ip(connfd);
    if (clientip == 0) {
      fprintf(stderr, "%s:failed to get client ip address...skip it.\n",__func__);
      close(connfd);
      continue;
    }
    cipkey = MAKE_CON_KEY(clientip,hsi1.pid);

    switch (hsi1.req){
    case REQ_CONNECT:
      lf_server_connect(cipkey,connfd,ct);
      break;
    case REQ_DISCONNECT:
      lf_server_disconnect(cipkey,connfd,ct,&hsi1);
      break;
    default:
      fprintf(stderr,"%s:unknown request received:%d...skip it.\n",__func__,hsi1.req);
      continue;
    }
    close(connfd);
  }
  
  DEBUG_PRINT("%s:The server daemon terminated.\n",__func__);
  return ct;
}

////////////////////////////////////////////////
// API Implementation                         //
////////////////////////////////////////////////
int initializeLFContext(
  struct lf_ctxt *ct,
  void * pool,
  const uint32_t psz,
  const uint32_t align,
  const char * provider,
  const char * domain,
  const uint16_t port,
  const uint16_t isClient,
  const struct lf_extra_config * conf) {
  DEBUG_PRINT("Enter %s\n",__func__);
  int ret = 0;

  if (domain == NULL) {
    fprintf(stderr,"%s:failed to tinitialize LibFabric context because domain is NULL.\n",__func__);
    return -1;
  };

  // STEP 1 initialize context from user provided info.
  DEBUG_PRINT("%s:initialize context from user provided info,\n",__func__);
  memset((void*)ct,0,sizeof(struct lf_ctxt));
  ct->psz = psz;
  ct->align = align;
  ct->cnt = 0;
  ct->port = port;

  DIE_NZ(default_context(ct),"initialize libfabric hints.");
  if (provider != NULL) {
    DIE_Z(ct->hints->fabric_attr->prov_name = strdup(provider),"ct->provider_str = strdup(provider)");
  }

  apply_extra_config(ct,conf);

  if (domain != NULL) {
    DIE_Z(ct->hints->domain_attr->name = strdup(domain),"ct->domain_str = strdup(domain)");
  }
  ct->port = port; 
  DIE_NZ(pthread_mutex_init(&ct->lock,NULL), "Could not initialize context mutex");
  DIE_Z(ct->con_map = MAP_INITIALIZE(con), "Could not initialize LF connection map");

  // STEP 2 allocate memory
  DEBUG_PRINT("%s:allocate %ldKiB memory,\n",__func__,(1l<<(psz-10)));
  if(pool == NULL){
    DIE_NZ(posix_memalign(&ct->pool,
        isClient?LF_CTXT_BUF_SIZE(ct):LF_CTXT_PAGE_SIZE(ct),
        LF_CTXT_POOL_SIZE(ct)),
      "Cannot Allocate Pool Memory");
    DEBUG_PRINT("%s:posix_memalign() get ct->pool:%p\n",__func__,ct->pool);
  } else {
    ct->pool = pool;
  }
  memset(ct->pool, 0, LF_CTXT_POOL_SIZE(ct));

  // STEP 3: initialize fabric and domain
  DIE_NZ(init_fabric_and_domain(ct),"ERROR init_fabric_and_domain()");

  // STEP 4: register memory
  CALL_FI_API( fi_mr_reg(ct->domain, ct->pool, 1l<<ct->psz,
    isClient? (FI_READ|FI_WRITE|FI_REMOTE_READ|FI_REMOTE_WRITE) : (FI_READ|FI_WRITE),
    0, LFPF_MR_KEY, 0, &(ct->mr), NULL), "fi_mr_reg()" ,ret );
  ct->local_mr_key = fi_mr_key(ct->mr);
  if (ct->local_mr_key == FI_KEY_NOTAVAIL) {
    fprintf(stderr,"%s:fail to get mr_key.\n",__func__);
    fi_freeinfo(ct->hints);
    ct->hints = NULL;
    return -2;
  }

  if (isClient) {
  // STEP 5.A For Client: initialize the bitmap
    DEBUG_PRINT("%s:initialize a client.",__func__);
    ct->is_client = true;
    DIE_Z(ct->extra_opts.client.bitmap = (uint8_t*)malloc(LF_CTXT_BYTES_BITMAP(ct)), "Could not allocate ct bitmap");
    memset(ct->extra_opts.client.bitmap, 0, LF_CTXT_BYTES_BITMAP(ct));
  } else {
  // STEP 5.B For Server:
    DEBUG_PRINT("%s:initialize a server.",__func__);
    ct->is_client = false;
    DIE_NZ(pthread_create(&ct->extra_opts.server.daemon, NULL,
      lf_daemon_routine, (void*)ct),
      "Could not initialize the daemon routine");
  }

  DEBUG_PRINT("QUIT %s\n",__func__);
  return ret;
}

int destroyLFContext(
  struct lf_ctxt *ct) {

  int ret = 0;
  // STEP 1 destroy connections
  uint64_t len = MAP_LENGTH(con,ct->con_map);
  if(len > 0){
    uint64_t *keys = MAP_GET_IDS(con,ct->con_map,len);
    while(len--){
      LFConn *conn = NULL;
      MAP_READ(con,ct->con_map,keys[len],&conn);
      MAP_DELETE(con,ct->con_map,keys[len]);
      if(ct->is_client){
        if(lf_client_disconnect(ct,GET_IP_FROM_KEY(keys[len]))!=0) {
          fprintf(stderr, "%s:Failed to disconnect from server %lx:%d\n",__func__,keys[len],ct->port);
        }
      }
      destroyLFConn(conn);
      free(conn);
    }
    free(keys);
  }
  // STEP 2 destroy fabrics
  if (ct->mr) {
    CALL_FI_API(fi_close(&ct->mr->fid),"close mr",ret);
  }
  if (ct->pep) {
    CALL_FI_API(fi_close(&ct->pep->fid),"close passive endpoint",ret);
  }
  if (ct->eq) {
    CALL_FI_API(fi_close(&ct->eq->fid),"close address vector",ret);
  }
  if (ct->domain) {
    CALL_FI_API(fi_close(&ct->domain->fid),"close domain",ret);
  }
  if (ct->fabric) {
    CALL_FI_API(fi_close(&ct->fabric->fid),"close fabric",ret);
  }
  if (ct->fi) {
    fi_freeinfo(ct->fi);
    ct->fi = NULL;
  }
  if (ct->hints) {
    fi_freeinfo(ct->hints);
    ct->hints = NULL;
  }
  // STEP 3 free extra resources for client/server
  if (ct->is_client) {
    free(ct->extra_opts.client.bitmap);
  } else {
    pthread_kill(ct->extra_opts.server.daemon, 9);
  }
  // STEP 4 release memory
  free(ct->pool);
  pthread_mutex_destroy(&ct->lock);
  return ret;
}

int allocateLFBuffer(
  struct lf_ctxt *ct,
  void **buf) {
  uint32_t buf_size = LF_CTXT_BUF_SIZE(ct);

  DEBUG_PRINT("%s:allocateLFBuffer() begin:%ld buffers allocated.\n",__func__,ct->cnt);
  // STEP 1 test context mode, quit for blog context
  if(!ct->is_client){ /// blog context
    fprintf(stderr,"%s:Could not allocate buffer in blog mode.\n",__func__);
    return -1;
  }
  // STEP 2 test if we have enough space
  if(!LF_CTXT_NFBUF(ct)){//we don't have enough space
    fprintf(stderr,"%s:Could not allocate buffer because we have %ld pages left.\n",__func__,LF_CTXT_NFPAGE(ct));
    return -2;
  }

  if(pthread_mutex_lock(&ct->lock)!=0){
    fprintf(stderr,"%s:Could not get lock.\n",__func__);
    return -3;
  };

  if(!LF_CTXT_NFBUF(ct)){//we don't have enough space
    pthread_mutex_unlock(&ct->lock);
    fprintf(stderr,"%s:Could not allocate buffer because we have %ld pages left.\n",__func__,LF_CTXT_NFPAGE(ct));
    return -4;
  }

  // STEP 3 find the buffer
  int32_t nbyte,nbit;
  /// 3.1 find the byte
  for(nbyte=0;nbyte<LF_CTXT_BYTES_BITMAP(ct);nbyte++){
    if(ct->extra_opts.client.bitmap[nbyte]==0xff ||
      (ct->psz-ct->align == 2 && ct->extra_opts.client.bitmap[nbyte] == 0xf0) ||
      (ct->psz-ct->align == 1 && ct->extra_opts.client.bitmap[nbyte] == 0xc0) ||
      (ct->psz-ct->align == 0 && ct->extra_opts.client.bitmap[nbyte] == 0xa0))
      continue;
    break;
  }
  if(nbyte==LF_CTXT_BYTES_BITMAP(ct)){
    pthread_mutex_unlock(&ct->lock);
    fprintf(stderr,"%s:Could not allocate buffer: we should have %ld pages left buf found none [type I].\n",__func__,LF_CTXT_NFPAGE(ct));
    return -5;
  }
  /// 3.2 find the bit
  nbit=0;
  while(nbit < 8 && nbit < LF_CTXT_BITS_BITMAP(ct))
    if((0x1<<nbit)&ct->extra_opts.client.bitmap[nbyte])nbit++;
    else break;
  if(nbit==8 || nbit == LF_CTXT_BITS_BITMAP(ct)){
    pthread_mutex_unlock(&ct->lock);
    fprintf(stderr,"%s:Could not allocate buffer: we should have %ld pages left buf found none [type II].\n",__func__,LF_CTXT_NFPAGE(ct));
    return -6;
  }
  // STEP 4 allocate the buffer
  ct->extra_opts.client.bitmap[nbyte] = ct->extra_opts.client.bitmap[nbyte]|(1<<nbit); /// fill bitmap
  *buf = (void*)((char*)ct->pool + (8*nbyte+nbit)*buf_size);
  DEBUG_PRINT("%s:found buffer slot[%p] @bitmap byte[%d], bit[%d].\n",__func__,(void *)((long) *buf - (long) ct->pool),nbyte,nbit);
  ct->cnt++;

  if(pthread_mutex_unlock(&ct->lock)!=0){
    fprintf(stderr,"%s:Could not release lock.",__func__);
    return -7;
  }
  DEBUG_PRINT("%s ends:%ld buffers allocated.\n",__func__,ct->cnt);
  return 0;
}

int releaseLFBuffer(
  struct lf_ctxt *ct,
  void *buf) {
  // STEP 1 check mode
  if(!ct->is_client){ /// blog context
    fprintf(stderr,"%s:Could not release buffer in blog mode.",__func__);
    return -1;
  }
  // STEP 2 validate buf address
  uint32_t nbyte,nbit;
  uint64_t temp = (uint64_t) buf - (uint64_t) ct->pool;
  nbyte = (temp/LF_CTXT_BUF_SIZE(ct))>>3;
  nbit  = (temp/LF_CTXT_BUF_SIZE(ct))&7;
  if(temp%LF_CTXT_BUF_SIZE(ct)!=0 ||
      buf<ct->pool ||
      (char *) buf >= (char *) ct->pool+LF_CTXT_POOL_SIZE(ct)){
    fprintf(stderr,"%s:invalid buf pointer=%p\n.",__func__,buf);
    return -2;
  }
  if(pthread_mutex_lock(&ct->lock)!=0){
    fprintf(stderr,"%s:Could not get lock.\n.",__func__);
    return -3;
  };
  if(!(ct->extra_opts.client.bitmap[nbyte]&(1<<nbit))){
    pthread_mutex_unlock(&ct->lock);
    fprintf(stderr,"%s:buffer[%p] is not allocated.\n",__func__,buf);
    return -4;
  }else{
    ct->extra_opts.client.bitmap[nbyte]^=(1<<nbit);
    ct->cnt--;
  }
  if(pthread_mutex_unlock(&ct->lock)!=0){
    fprintf(stderr,"%s:Could not release lock.",__func__);
    return -3;
  }
  return 0;
}

int LFConnect(
  struct lf_ctxt *ct,
  const uint32_t hostip) {
  // On client side, we use host and port to identify a server.
  const uint64_t cipkey = (const uint64_t)MAKE_CON_KEY(hostip,ct->port);
  int is_dup;
  struct sockaddr_in svraddr;
  int connfd;
  int ret = 0;
  LFConn * lf_conn = NULL;

  DEBUG_PRINT("%s:connect to server:0x%x.\n",__func__,hostip);
  // test duplication
  MAP_LOCK(con, ct->con_map, cipkey, 'w' );
  is_dup = (MAP_READ(con,ct->con_map,cipkey,&lf_conn)==0);
  if(is_dup){
    MAP_UNLOCK(con, ct->con_map, cipkey);
    return -1; // the connection exists already.
  }

  // connect
  connfd = socket(AF_INET, SOCK_STREAM, 0);
  bzero((char*)&svraddr,sizeof(svraddr));
  svraddr.sin_family = AF_INET;
  svraddr.sin_port = htons(ct->port);
  svraddr.sin_addr.s_addr = hostip;
  int retry = 2;
  while(retry){
    retry --;
    DEBUG_PRINT("%s:Try %d.\n",__func__,retry);
    if(connect(connfd,(const struct sockaddr *)&svraddr,sizeof(svraddr))<0){
      if(!retry){
        MAP_UNLOCK(con, ct->con_map, cipkey);
        fprintf(stderr,"%s:cannot connect to server:%x:%d, errno=%d\n",__func__,hostip,ct->port,errno);
      }
      sleep(20); // sleep 30 sec.
    }else // succeed
      break;
  }

  ret = lf_client_connect(cipkey,connfd,ct);
  close(connfd);

  MAP_UNLOCK(con, ct->con_map, cipkey);
  DEBUG_PRINT("%s:connected to server:0x%x.\n",__func__,hostip);
  return ret;
}

// Note: since this is only called from the destroyLFContext API, 
// so we don't change the MAP here.
int LFDisconnect(
  struct lf_ctxt *ct,
  const uint32_t hostip) {

  // STEP 1: connect to server
  struct sockaddr_in svraddr;
  int connfd = socket(AF_INET, SOCK_STREAM, 0);
  bzero((char*)&svraddr,sizeof(svraddr));
  svraddr.sin_family = AF_INET;
  svraddr.sin_port = htons(ct->port);
  svraddr.sin_addr.s_addr = hostip;
  if(connect(connfd,(const struct sockaddr *)&svraddr,sizeof(svraddr))<0){
    fprintf(stderr,"cannot connect to server:%x:%d\n",hostip,ct->port);
    return -1;
  }
  int flag = 1;
  if(setsockopt(connfd,IPPROTO_TCP,TCP_NODELAY,(char*)&flag,sizeof(int)) < 0){
    fprintf(stderr,"cannot set tcp nodelay.\n");
    return -2;
  }

  // STEP 2: send a disconnect request
  LF_HS_I1 hsi1;
  hsi1.req = REQ_DISCONNECT;
  hsi1.pid = getpid();
  if(send(connfd, &hsi1, sizeof(hsi1), 0) != sizeof(hsi1)){
    perror("Could not send ibcfg to peer");
    return -3;
  }
  // fsync(connfd);
  if(recv(connfd, &hsi1, sizeof(hsi1), MSG_WAITALL) != sizeof(hsi1)){
    fprintf(stderr, "%s:I didn't get the echo to disconnect request.\n",__func__);
  }

  // finish
  close(connfd);
  return 0;
}

int LFTransfer(
  struct lf_ctxt *ct,
  const uint32_t hostip,
  const uint32_t pid,
  const uint64_t r_vaddr,
  void **pagelist,
  int npage,
  int isWrite,
  int pageSize) {

  DEBUG_PRINT("%s:begin transfer: hostip=%x,pid=%x,r_vaddr=%lx,npage=%d,isWrite=%d,pageSize=%d\n",
    __func__,hostip,pid,r_vaddr,npage,isWrite,pageSize);

  LFConn *lfconn = NULL;
  uint32_t nr_pages_to_transfer = (uint32_t)npage;
  uint32_t i,ops_in_pipe = 0;
  ssize_t num_comp;
  const uint64_t cipkey = (const uint64_t)MAKE_CON_KEY(hostip,pid);
  int pagesize = pageSize;
  int ret = 0;

  if(pagesize == 0) {
    pagesize = LF_CTXT_PAGE_SIZE(ct);
  }

  // STEP 1: get the connection
  DEBUG_PRINT("%s:Try LOCK...\n",__func__);
  MAP_LOCK(con, ct->con_map, cipkey, 'w');
  DEBUG_PRINT("%s:Locked...\n",__func__);
  if(MAP_READ(con, ct->con_map, cipkey, &lfconn)!=0){
    fprintf(stderr, "%s:Cannot find the context for ip:%lx",__func__,cipkey);
    MAP_UNLOCK(con, ct->con_map, cipkey);
    return -1;
  }
  // STEP 2: finish the transfer and return.
  struct iovec *iov = (struct iovec *) malloc(sizeof(struct iovec)*LF_CTXT_ACTIVE_SGE_BAT_SIZE(ct));
  if (iov == NULL) {
    fprintf(stderr, "%s:Allocate iov vector failed with error:%d\n.",__func__,errno);
    MAP_UNLOCK(con, ct->con_map, cipkey);
    return -2;
  }
  void** desc = (void**) malloc(sizeof(void*)*LF_CTXT_ACTIVE_SGE_BAT_SIZE(ct));
  if (desc == NULL) {
    fprintf(stderr, "%s:Allocate iov vector failed with error:%d\n.",__func__,errno);
    free(iov);
    MAP_UNLOCK(con, ct->con_map, cipkey);
    return -3;
  }
  void *cq_buffer = malloc(sizeof(struct fi_cq_entry)*ct->tx_depth);
  if (desc == NULL) {
    fprintf(stderr, "%s:Allocate iov vector failed with error:%d\n.",__func__,errno);
    free(iov);
    free(desc);
    MAP_UNLOCK(con, ct->con_map, cipkey);
    return -3;
  }
  DEBUG_PRINT("%s:buffers allocated, now trying to send.\n",__func__);
  while (nr_pages_to_transfer) {
    DEBUG_PRINT("%s:nr_pages_to_transfer:%d\n",__func__,nr_pages_to_transfer);
    //// fill the sge list.
    for (i=0;i<MIN(nr_pages_to_transfer,LF_CTXT_ACTIVE_SGE_BAT_SIZE(ct));i++) {
      iov[i].iov_base = pagelist[npage-nr_pages_to_transfer+i];
      iov[i].iov_len = pagesize;
      desc[i] = (void*)ct->local_mr_key;
      DEBUG_PRINT("%s:iov_base=%p,iov_len=%ld,desc=%lx\n",__func__,iov[i].iov_base,iov[i].iov_len,ct->local_mr_key);
    }

    DEBUG_PRINT("%s:count=%d,addr=%lx,rkey=%lx\n",__func__,i,r_vaddr + (npage - nr_pages_to_transfer)*pagesize,lfconn->r_rkey);
    if(isWrite){
      ret = fi_writev(lfconn->ep,iov,desc,i,0,
        (LF_CTXT_USE_VADDR(ct)?r_vaddr:(r_vaddr-lfconn->remote_fi_addr)) + (npage - nr_pages_to_transfer)*pagesize,
        lfconn->r_rkey,NULL);
      // ret = fi_write(lfconn->ep,iov[0].iov_base,iov[0].iov_len,(void*)ct->local_mr_key,0,r_addr + (npage - nr_pages_to_transfer)*pagesize, lfconn->r_rkey,NULL);
    } else {
      ret = fi_readv(lfconn->ep,iov,desc,i,0,
        (LF_CTXT_USE_VADDR(ct)?r_vaddr:(r_vaddr-lfconn->remote_fi_addr)) + (npage - nr_pages_to_transfer)*pagesize,
        lfconn->r_rkey,NULL);
      // ret = fi_read(lfconn->ep,iov[0].iov_base,iov[0].iov_len,(void*)ct->local_mr_key,0,r_addr + (npage - nr_pages_to_transfer)*pagesize, lfconn->r_rkey,NULL);
    }
    if (ret < 0) {
      fprintf(stderr,"%s:Failed to transfer data with error:%d(%s)\n",__func__,-ret,fi_strerror(-ret));
      free(iov);
      free(desc);
      free(cq_buffer);
      MAP_UNLOCK(con, ct->con_map, cipkey);
      return ret;
    }
    nr_pages_to_transfer -= i;
    ops_in_pipe ++;
    DEBUG_PRINT("%s:ops_in_pipe:%d\n",__func__,ops_in_pipe);

    //// conitnue to send as long as pipeline is not full and we have data.
    if ( (ops_in_pipe<ct->tx_depth) && (nr_pages_to_transfer>0)) {
      continue;
    }

    //// waiting for ack as much as possible
    //// TODO: use fi_cq_sread.
    do {
      num_comp = fi_cq_read(lfconn->txcq, cq_buffer, ops_in_pipe);
    } while(num_comp == -FI_EAGAIN);
    if (num_comp < 0) {
      fprintf(stderr,"%s:Failed to read ack data with error:%ld(%s)\n",__func__,-num_comp,fi_strerror(-num_comp));
      free(iov);
      free(desc);
      free(cq_buffer);
      MAP_UNLOCK(con, ct->con_map, cipkey);
      return ret;
    }
    ops_in_pipe -= num_comp;
    DEBUG_PRINT("%s:ops_in_pipe:%d,num_comp=%ld\n",__func__,ops_in_pipe,num_comp);
  }
  //// wait for all the ACKs.
  while (ops_in_pipe) {
    do {
      num_comp = fi_cq_read(lfconn->txcq, cq_buffer, ops_in_pipe);
    } while(num_comp == -FI_EAGAIN);
    if (num_comp < 0) {
      fprintf(stderr,"%s:Failed to read ack data with error:%d(%s)\n",__func__,-ret,fi_strerror(-ret));
      free(iov);
      free(desc);
      free(cq_buffer);
      MAP_UNLOCK(con, ct->con_map, cipkey);
      return ret;
    }
    ops_in_pipe -= num_comp;
    DEBUG_PRINT("%s:ops_in_pipe:%d\n",__func__,ops_in_pipe);
  }
  //// release sge list and return
  free(iov);
  free(desc);
  free(cq_buffer);
  MAP_UNLOCK(con, ct->con_map, cipkey);
  return ret;
}
