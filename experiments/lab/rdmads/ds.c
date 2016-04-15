#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <netdb.h>
#include <sys/time.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <infiniband/verbs.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <signal.h>
#define RDMA_WRID 3



#define TEST_NZ(x,y) do { if ((x)) die(y); } while (0)
#define TEST_Z(x,y) do { if (!(x)) die(y); } while (0)
#define TEST_N(x,y) do { if ((x)<0) die(y); } while (0)

#define SPAN(tv1,tv2) (((tv2).tv_sec-(tv1).tv_sec)*1000000 + (tv2).tv_usec - (tv1).tv_usec)
#define THP(npage,us) (((double)(npage)*page_size)/(us))
#define NREQ(n,us) ((double)(n)/(us))

#define MIN(x,y) ((x)<(y)?(x):(y))

//64MB page
#define MAX_PAGE (16)
#define	SVR_PORT (18515)
#define MAX_SQE (64)

/*
 * All clients and server has 16 pages. The client requests
 * the server to fill a set of pages.
 */

const char * hlp_info = " Usage: \n \
 ds svr <dev> <pagesize> <nloop> \n \
 ds cli <dev> <server> <pagesize> 0 3 6 9 12 ... \n";

static int die(const char *reason){
  fprintf(stderr, "Err: %s - %s \n ", strerror(errno), reason);
  exit(EXIT_FAILURE);
  return -1;
}

typedef struct app_context{
  struct ibv_device	*dev;
  struct ibv_context	*context;
  struct ibv_pd		*pd;
  struct ibv_mr		*mr;
  void			*pages;

  struct ibv_device_attr  dev_attr;
} AppCtxt;

static AppCtxt ctxt;
static char *dev = NULL;
static int page_size = 0;
static int nloop = 0;

static void list_devices(void){
  struct ibv_device ** dev_list = NULL;
  int num_device,i;
  TEST_Z(dev_list = ibv_get_device_list(&num_device),"No IB-device available. get_device_list returned NULL");

  for(i=0;i<num_device;i++){
    const char * devname = ibv_get_device_name(dev_list[i]);
    printf("[%d]\t%s\n",i,devname);
  }

  ibv_free_device_list(dev_list);
}

static void init_ctxt(int page_size){

  // STEP 1 context
  TEST_NZ(posix_memalign(&ctxt.pages, page_size, page_size*MAX_PAGE),"could not allocate working buffer ctx->pages");
  memset(ctxt.pages, 0, page_size*MAX_PAGE);

  int num_device,i;
  struct ibv_device **dev_list;
  TEST_Z(dev_list = ibv_get_device_list(&num_device),"No IB-device available. get_device_list returned NULL");

  for(i=0;i<num_device;i++){
    const char * devname = ibv_get_device_name(dev_list[i]);
    if(strcmp(devname,dev)){
      printf("skipping device:%s...\n",devname);
    }else
      break;
  }
  if(i==num_device){
    die("Cannot find RDMA device.");
  }

  TEST_Z(ctxt.dev=dev_list[i],"IB-device could not be assigned. Maybe dev_list array is empty");
  TEST_Z(ctxt.context=ibv_open_device(ctxt.dev),"Could not create context, ibv_open_device");
  TEST_NZ(ibv_query_device(ctxt.context,&ctxt.dev_attr), "Could not query device attributes");
  TEST_Z(ctxt.pd=ibv_alloc_pd(ctxt.context),"Could not allocate protection domain, ibv_alloc_pd");
  TEST_Z(ctxt.mr=ibv_reg_mr(ctxt.pd, ctxt.pages, page_size*MAX_PAGE, 
         IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC ), "Could not allocate mr, ibv_reg_mr. Do you have root access?");
  ibv_free_device_list(dev_list);
}

static void init_pages(int isServer){
  if(ctxt.pages==NULL)return;
  if(isServer){
    printf("--init server pages--\n");
    int i=0,j=0;
    char ch = 'A';
    for(i=0;i<MAX_PAGE;i++){
      printf("[%c]",ch);
      for(j=0;j<page_size;j++){
        ((char*)ctxt.pages)[i*page_size+j]=ch;
      }
      ch ++;
    }
    printf("\n");
    for(i=0;i<MAX_PAGE;i++)
      printf("[%c]",((char*)ctxt.pages)[i*page_size]);
    printf("\n");
  }else{
    memset(ctxt.pages,'Z',MAX_PAGE*page_size);
  }
}

typedef struct _request{
  uint8_t page_flags[MAX_PAGE];
}Request;

typedef struct _response{
  uint8_t err_code;
} Response;

typedef struct _connection{
  struct ibv_cq *rcq;
  struct ibv_cq *scq;
  struct ibv_qp *qp;
  struct ibv_comp_channel *ch;
  int port;
//  uint32_t l_lid,r_lid;
  union ibv_gid l_gid,r_gid;
  uint32_t l_lid,r_lid;
  uint32_t l_qpn,r_qpn;
  uint32_t l_psn,r_psn;
  uint32_t l_rkey,r_rkey;
  uint64_t l_vaddr,r_vaddr; // we dont need l_vaddr because it is decided by request?
  uint32_t l_mtu,r_mtu; // we chose the minimum of the two
  uint32_t l_qp_rd_atom,r_qp_rd_atom; //qp read out standing
} Connection;

static void print_ib_con(Connection *ibcon){
  printf("Local:\nLID\t%#8x\nGID\t%#16Lx.%#16Lx\nQPN\t%#08x\nPSN\t%#08x\nRKey\t%#08x\nVAddr\t%#016Lx\nMTU\t%#08x\nRD_ATOM\t%#08x\n",
    ibcon->l_lid,
    (long long unsigned)ibcon->l_gid.global.subnet_prefix,
    (long long unsigned)ibcon->l_gid.global.interface_id,
    ibcon->l_qpn, ibcon->l_psn, ibcon->l_rkey, (long long unsigned)ibcon->l_vaddr,
    ibcon->l_mtu, ibcon->l_qp_rd_atom);
  printf("Local:\nLID\t%#8x\nGID\t%#16Lx.%#16Lx\nQPN\t%#08x\nPSN\t%#08x\nRKey\t%#08x\nVAddr\t%#016Lx\nMTU\t%#08x\nRD_ATOM\t%#08x\n",
    ibcon->r_lid,
    (long long unsigned)ibcon->r_gid.global.subnet_prefix,
    (long long unsigned)ibcon->r_gid.global.interface_id,
    ibcon->r_qpn, ibcon->r_psn, ibcon->r_rkey, (long long unsigned)ibcon->r_vaddr,
    ibcon->r_mtu, ibcon->r_qp_rd_atom);
}

#define LID_SZ  (sizeof(uint32_t))
#define GID_SZ	(sizeof(union ibv_gid))
#define PSN_SZ	(sizeof(uint32_t))
#define QPN_SZ	(sizeof(uint32_t))
#define RKEY_SZ	(sizeof(uint32_t))
#define VADDR_SZ        (sizeof(uint64_t))
#define MTU_SZ  (sizeof(uint32_t))
#define RDATOM_SZ       (sizeof(uint32_t))
#define CONF_SZ (LID_SZ + PSN_SZ + QPN_SZ + GID_SZ + RKEY_SZ + VADDR_SZ + MTU_SZ + RDATOM_SZ)
#define LOC_LID (0)
#define LOC_GID (LOC_LID + LID_SZ)
#define LOC_QPN (LOC_GID + GID_SZ)
#define LOC_PSN (LOC_QPN + QPN_SZ)
#define LOC_RKEY (LOC_PSN + PSN_SZ)
#define LOC_VADDR (LOC_RKEY + RKEY_SZ)
#define LOC_MTU (LOC_VADDR + VADDR_SZ)
#define LOC_RDATOM (LOC_MTU + MTU_SZ)
char ibcfg[CONF_SZ];

static void setibcfg(char *ibcfg, Connection *ibcon){
  // 0 LID
  *((uint32_t *)(ibcfg + LOC_LID)) = ibcon->l_lid;
  // 1 GID
  *((union ibv_gid *)(ibcfg + LOC_GID)) = ibcon->l_gid;
  // 2 QPN
  *((uint32_t *)(ibcfg + LOC_QPN)) = ibcon->l_qpn;
  // 3 PSN
  *((uint32_t *)(ibcfg + LOC_PSN)) = ibcon->l_psn;
  // 4 RKEY
  *((uint32_t *)(ibcfg + LOC_RKEY)) = ibcon->l_rkey;
  // 5 VADDR
  *((uint64_t *)(ibcfg + LOC_VADDR)) = ibcon->l_vaddr;
  // 6 MTU
  *((uint32_t *)(ibcfg + LOC_MTU)) = ibcon->l_mtu;
  // 7 RD_ATOM
  *((uint32_t *)(ibcfg + LOC_RDATOM)) = ibcon->l_qp_rd_atom;
}

static void getibcfg(const char *ibcfg, Connection *ibcon){
  // 0 LID
  ibcon->r_lid = *((uint32_t *)(ibcfg + LOC_LID));
  // 1 GID
  ibcon->r_gid = *((union ibv_gid *)(ibcfg + LOC_GID));
  // 2 QPN
  ibcon->r_qpn = *((uint32_t *)(ibcfg + LOC_QPN));
  // 3 PSN
  ibcon->r_psn = *((uint32_t *)(ibcfg + LOC_PSN));
  // 4 RKEY
  ibcon->r_rkey = *((uint32_t *)(ibcfg + LOC_RKEY));
  // 5 VADDR
  ibcon->r_vaddr = *((uint64_t *)(ibcfg + LOC_VADDR));
  // 6 MTU
  ibcon->r_mtu = *((uint32_t *)(ibcfg + LOC_MTU));
  // 7 RD_ATOM
  ibcon->r_qp_rd_atom = *((uint32_t *)(ibcfg + LOC_RDATOM));
}

static int qp_change_state_init(struct ibv_qp *qp, int port){
  struct ibv_qp_attr *attr;
  attr = malloc(sizeof *attr);
  memset(attr, 0, sizeof *attr);
  attr->qp_state=IBV_QPS_INIT;
  attr->pkey_index=0;
  attr->port_num=port;
  attr->qp_access_flags=IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_READ|IBV_ACCESS_REMOTE_ATOMIC;
  TEST_NZ(ibv_modify_qp(qp,attr,
    IBV_QP_STATE|IBV_QP_PKEY_INDEX|IBV_QP_PORT|IBV_QP_ACCESS_FLAGS),
    "Could not modify QP to INIT, ibv_modify_qp");
  return 0;
}

static int qp_change_state_rtr(struct ibv_qp *qp, Connection * ibcon){
  struct ibv_qp_attr *attr;
  attr = malloc(sizeof *attr);
  memset(attr, 0, sizeof *attr);

  attr->qp_state = IBV_QPS_RTR;
  attr->path_mtu = MIN(ibcon->l_mtu,ibcon->r_mtu);
  attr->dest_qp_num = ibcon->r_qpn;
  attr->rq_psn = ibcon->r_psn;
  attr->max_dest_rd_atomic = ibcon->r_qp_rd_atom;
  attr->min_rnr_timer = 12;
  attr->ah_attr.dlid = ibcon->r_lid;
  // we use port 1, gid index 0
  attr->ah_attr.grh.dgid = ibcon->r_gid;
  attr->ah_attr.grh.flow_label = 0;
  attr->ah_attr.grh.sgid_index = 0;
  attr->ah_attr.grh.hop_limit = 10; // allow 10 hops
  attr->ah_attr.grh.traffic_class = 0;
  attr->ah_attr.is_global = 1;
  attr->ah_attr.sl = 0;
  attr->ah_attr.src_path_bits = 0;
  attr->ah_attr.port_num = ibcon->port;

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

static int qp_change_state_rts(struct ibv_qp *qp, Connection * ibcon){
  qp_change_state_rtr(qp, ibcon);
  struct ibv_qp_attr *attr;
  attr = malloc(sizeof *attr);
  memset(attr, 0, sizeof *attr);

  attr->qp_state = IBV_QPS_RTS;
  attr->timeout = 4;
  attr->retry_cnt = 6; // try at most 6 times on timeout.
  attr->rnr_retry = 6; // try at most 6 times on NACK
  attr->sq_psn = ibcon->l_psn;
  attr->max_rd_atomic = ibcon->l_qp_rd_atom;

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

/**
 * server_routine:
 * args - connection socket
 * return value - nothing
 */
static void* server_routine(void *args){
  int connfd = *(int*)args;
  Request req;
  Response res;
  res.err_code = 0;
struct timeval tv1,tv2,tv3;
gettimeofday(&tv1,NULL);
  // STEP 1 - setup connection
  Connection ibcon = {
    .scq = NULL,
    .rcq = NULL,
    .qp = NULL,
    .ch = NULL,
    .port = 1
  };
  TEST_Z(ibcon.ch=ibv_create_comp_channel(ctxt.context),"Could not create completion channel, ibv_create_comp_channel");
  TEST_Z(ibcon.rcq=ibv_create_cq(ctxt.context,1,NULL,NULL,0),"Could not create receive completion queue, ibv_create_cq");
  TEST_Z(ibcon.scq=ibv_create_cq(ctxt.context,MAX_SQE,&ibcon,ibcon.ch,0),"Could not create send completion queue, ibv_create_cq");
  struct ibv_qp_init_attr qp_init_attr = {
    .send_cq = ibcon.scq,
    .recv_cq = ibcon.rcq,
    .qp_type = IBV_QPT_RC,
    .cap = {
      .max_send_wr = MAX_SQE,
      .max_recv_wr = MAX_SQE,
      .max_send_sge = MAX_PAGE,
      .max_recv_sge = MAX_PAGE,
      .max_inline_data = 0
    }
  };
  TEST_Z(ibcon.qp=ibv_create_qp(ctxt.pd,&qp_init_attr),"Could not create queue pair, ibv_create_qp");
  qp_change_state_init(ibcon.qp,ibcon.port);
  struct ibv_port_attr attr;
  TEST_NZ(ibv_query_port(ctxt.context,ibcon.port,&attr),"Could not get port attributes, ibv_query_port");
  ibcon.l_lid = attr.lid;
  TEST_NZ(ibv_query_gid(ctxt.context,ibcon.port,0,&ibcon.l_gid),"Could not get gid of local port, ibv_query_port");
  ibcon.l_qpn = ibcon.qp->qp_num;
  ibcon.l_psn = lrand48() & 0xffffff;
  ibcon.l_rkey = ctxt.mr->rkey;
  ibcon.l_vaddr = (uintptr_t)ctxt.pages;
  ibcon.l_mtu = attr.active_mtu;
  ibcon.l_qp_rd_atom = ctxt.dev_attr.max_qp_rd_atom;
  char msg[CONF_SZ];
  setibcfg(msg,&ibcon);
  //Exchange connection information(initialize client connection first.)
  if(write(connfd, msg, sizeof ibcfg) != sizeof ibcfg){
    perror("Could not send ibcfg to peer");
    return NULL;
  }
  if(read(connfd, msg, sizeof msg)!= sizeof msg){
    perror("Could not receive ibcfg from peer");
    return NULL;
  }
  getibcfg(msg,&ibcon);
  print_ib_con(&ibcon);
  //change to RTS
  qp_change_state_rts(ibcon.qp,&ibcon);
  // read command: a fixed-length byte array: byte[MAX_PAGE]
  recv(connfd,(void*)&req.page_flags,MAX_PAGE,MSG_WAITALL);
  struct ibv_sge *sge_list = malloc(sizeof(struct ibv_sge)*MAX_PAGE);
  int i;
  //RDMA write
  for(i=0;i<MAX_PAGE;i++){
    if(req.page_flags[i]==255)break;
    (sge_list+i)->addr = (uintptr_t)ctxt.pages+page_size*req.page_flags[i];
    (sge_list+i)->length = page_size;
    (sge_list+i)->lkey = ctxt.mr->lkey;
  }
  struct ibv_send_wr wr;
  wr.wr.rdma.remote_addr = ibcon.r_vaddr;
  wr.wr.rdma.rkey = ibcon.r_rkey;
  wr.wr_id = RDMA_WRID;
  wr.sg_list = sge_list;
  wr.num_sge = i;
  wr.opcode = IBV_WR_RDMA_WRITE;//IBV_WR_RDMA_READ
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.next = NULL;
  struct ibv_send_wr *bad_wr;
gettimeofday(&tv2,NULL);
  int nl=nloop,pcnt=0,rcnt=0;
printf("wr.num_sge=%d,nloop=%d\n",i,nloop);
  int nwr=0;
  do{
#ifdef PRINT_WAITTIME
    struct timeval tx1,tx2;
#endif//PRINT_WAITTIME
/////////////////////////////////////////////////////
// use completion queue
    TEST_NZ(ibv_req_notify_cq(ibcon.scq,0),"Could not request notify from sending completion queue, ibv_req_notify_cq");
/////////////////////////////////////////////////////
    int rSend = 0;
    while(rSend==0 && nl>0 && nwr<MAX_SQE){
      rSend = ibv_post_send(ibcon.qp,&wr,&bad_wr);
      if(rSend==0){
        nl--;nwr++;
        pcnt += wr.num_sge;
        rcnt ++;
      }
    }
    // if(rSend!=0 && rSend!=ENOMEM){
    if(rSend!=0){
      fprintf(stderr,"ERROR in ibv_post_send:%d\n",rSend);
      exit(-1);
    }
    int ne;
#ifdef PRINT_WAITTIME
    gettimeofday(&tx1,NULL);
#endif//PRINT_WAITTIME
    struct ibv_wc *wc = (struct ibv_wc*)malloc(MAX_SQE*sizeof(struct ibv_wc));
//////////////////////////////////////////////////////////
// wait on completion queue
    void *cq_ctxt;
    TEST_NZ(ibv_get_cq_event(ibcon.ch,&ibcon.scq,&cq_ctxt),"ibv_get_cq_event failed");
    ibv_ack_cq_events(ibcon.scq,1);
//////////////////////////////////////////////////////////
    do{
      ne = ibv_poll_cq(ibcon.scq,MAX_SQE,wc);
    }while(ne==0);
    if(ne<0){
      fprintf(stderr, "%s: poll CQ failed %d\n", __func__, ne);
    }else{
      nwr -= ne;
      //fprintf(stdout, "I received %d wc entries.\n", ne);
    }
  int i;
  for(i=0;i<ne;i++)
    if((wc+i)->status != IBV_WC_SUCCESS) {
      fprintf(stderr, "%d:%s: Completion with error at %s:\n", getpid(), __func__, "server");
      fprintf(stderr, "%d:%s: Failed status %d: wr_id %ld, qp_num = %d, vendor_err = %d\n", getpid(), __func__, (wc+i)->status, (wc+i)->wr_id, (wc+i)->qp_num, (wc+i)->vendor_err);
      res.err_code = 1;
    }
#ifdef PRINT_WAITTIME
    gettimeofday(&tx2,NULL);
//    printf("[%d] %ld nl=%d\n",ne,SPAN(tx1,tx2),nl);
#endif//PRINT_WAITTIME
  }while(nl>0||nwr>0);
  gettimeofday(&tv3,NULL);
  free(sge_list);
  // write notification: a byte: 0 for success, otherwise failure.
  send(connfd,(void*)&res.err_code,1,0);
  close(connfd);
  // clean
  TEST_NZ(ibv_destroy_qp(ibcon.qp),"Could not destroy queue pair, ibv_destroy_qp");
  TEST_NZ(ibv_destroy_cq(ibcon.scq),"Could not destroy send completion queue, ibv_destroy_cq");
  TEST_NZ(ibv_destroy_cq(ibcon.rcq),"Could not destroy receive completion queue, ibv_destroy_cq");
  TEST_NZ(ibv_destroy_comp_channel(ibcon.ch),"Cloud not destroy completion channel, ibv_destroy_comp_channel");
/*
printf("[%d] %ld %ld %ld.%ld %ld.%ld %ld.%ld %.3f\n", connfd,
  SPAN(tv1,tv2),SPAN(tv2,tv3),
  tv1.tv_sec,tv1.tv_usec,
  tv2.tv_sec,tv2.tv_usec,
  tv3.tv_sec,tv3.tv_usec,
  THP(pcnt,SPAN(tv2,tv3)));
*/
printf("thp=%.3fGb/s rate=%.3fMT/s dur=%ldus\n",THP(pcnt,SPAN(tv2,tv3))*8/1000, NREQ(rcnt,SPAN(tv2,tv3)), SPAN(tv2,tv3));
  return NULL;
}


int sockfd = -1; //the server socket.

sighandler_t int_handler; //the original SIGINT handler
sighandler_t quit_handler; //the original SIGQUIT handler
sighandler_t kill_handler; //the original SIGKILL handler
sighandler_t term_handler; //the original SIGTERM handler

void sig_handler(int sig){
  fprintf(stdout,"closing server socket...\n");
  if(sockfd != -1)
    close(sockfd);
  switch(sig){
    case SIGINT:
      signal(sig, int_handler);
      break;
    case SIGQUIT:
      signal(sig, quit_handler);
      break;
    case SIGKILL:
      signal(sig, kill_handler);
      break;
    case SIGTERM:
      signal(sig, term_handler);
      break;
    default:
      fprintf(stderr, "unknown singal:%d", sig);
  }
  raise(sig);
}

static void doServer(int argc, char ** argv){
  if(argc!=5){
    fprintf(stderr,"Error: we expected 5 args, but we got %d\n", argc);
    return;
  }

  dev = argv[2];
  page_size = atoi(argv[3]);
  nloop = atoi(argv[4]);
  // STEP 1 initialization
  struct addrinfo *res;
  struct addrinfo hints = {
    .ai_flags = AI_PASSIVE,
    .ai_family = AF_UNSPEC,
    .ai_socktype = SOCK_STREAM
  };
  char *service;
  int n,connfd,reuse=1;
  // initialize context
  init_ctxt(page_size);
  // initialize page data;
  init_pages(1);
  ///struct sockaddr_in sin;
  TEST_N(asprintf(&service,"%d", SVR_PORT), "ERROR writing port number to port string.");
  TEST_N(n=getaddrinfo(NULL,service,&hints,&res), "getaddrinfo threw error");
  TEST_N(sockfd=socket(res->ai_family, res->ai_socktype, res->ai_protocol), "Could not create server socket");
  setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof reuse);
#ifdef SO_REUSEPORT
  setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof reuse);
#endif
  // STEP 2 binding
  TEST_N(bind(sockfd,res->ai_addr,res->ai_addrlen), "Could not bind addr to socket");
  listen(sockfd, 1);
  // STEP 2.9 register signal handler
  int_handler = signal(SIGINT,sig_handler);
  quit_handler = signal(SIGQUIT,sig_handler);
  kill_handler = signal(SIGKILL,sig_handler);
  term_handler = signal(SIGTERM,sig_handler);
  // STEP 3 receiving requests
  while(1){
    pthread_t tid;
    TEST_N(connfd = accept(sockfd, NULL, 0), "server accept failed.");
    setsockopt(connfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof reuse);
#ifdef SO_REUSEPORT
    setsockopt(connfd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof reuse);
#endif
    pthread_create(&tid,NULL,server_routine,&connfd);
  }
}

static void doClient(int argc, char ** argv){
  if(argc<6){
    fprintf(stderr,"Error: we expected 6 args, but we got %d\n", argc);
    return;
  }

  dev = argv[2];
  char * server = argv[3];
  int page_size = atoi(argv[4]);
  // STEP 1 initialization
  init_ctxt(page_size);
  init_pages(0);
  // STEP 1.1 - setup connection
  Connection ibcon = {
    .scq = NULL,
    .rcq = NULL,
    .qp = NULL,
    .ch = NULL,
    .port = 1
  };
  TEST_Z(ibcon.ch=ibv_create_comp_channel(ctxt.context),"Could not create completion channel, ibv_create_comp_channel");
  TEST_Z(ibcon.rcq=ibv_create_cq(ctxt.context,1,NULL,NULL,0),"Could not create receive completion queue, ibv_create_cq");
  TEST_Z(ibcon.scq=ibv_create_cq(ctxt.context,MAX_SQE,&ibcon,ibcon.ch,0),"Could not create send completion queue, ibv_create_cq");
  struct ibv_qp_init_attr qp_init_attr = {
    .send_cq = ibcon.scq,
    .recv_cq = ibcon.rcq,
    .qp_type = IBV_QPT_RC,
    .cap = {
      .max_send_wr = MAX_SQE,
      .max_recv_wr = MAX_SQE,
      .max_send_sge = MAX_PAGE,
      .max_recv_sge = MAX_PAGE,
      .max_inline_data = 0
    }
  };
  TEST_Z(ibcon.qp=ibv_create_qp(ctxt.pd,&qp_init_attr),"Could not create queue pair, ibv_create_qp");
  qp_change_state_init(ibcon.qp,ibcon.port);
  struct ibv_port_attr attr;
  TEST_NZ(ibv_query_port(ctxt.context,ibcon.port,&attr),"Could not get port attributes, ibv_query_port");
  ibcon.l_lid = attr.lid;
  TEST_NZ(ibv_query_gid(ctxt.context,ibcon.port,0,&ibcon.l_gid),"Could not get gid of local port, ibv_query_port");
  ibcon.l_qpn = ibcon.qp->qp_num;
  ibcon.l_psn = lrand48() & 0xffffff;
  ibcon.l_rkey = ctxt.mr->rkey;
  ibcon.l_vaddr = (uintptr_t)ctxt.pages;
  ibcon.l_mtu = attr.active_mtu;
  ibcon.l_qp_rd_atom = ctxt.dev_attr.max_qp_rd_atom;
  //STEP 1.2 prepare my ib connection info
  setibcfg(ibcfg,&ibcon);
  
  //STEP 2 connect
  struct addrinfo *res, *t;
  struct addrinfo hints = {
    .ai_family = AF_UNSPEC,
    .ai_socktype = SOCK_STREAM
  };

  char *service;
  //int n;
  int sockfd = -1;
  //struct sockaddr_in sin;
  TEST_N(asprintf(&service, "%d", SVR_PORT), "Error writing port number to port string.");
  TEST_N(getaddrinfo(server, service, &hints, &res), "getaddrinfo threw error");
  //STEP 2 connect
  for(t = res; t; t=t->ai_next){
    TEST_N(sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol),"Could not create client socket");
    TEST_N(connect(sockfd,t->ai_addr,t->ai_addrlen),"Could not connect to server");
    break;
  }
  freeaddrinfo(res);
  //STEP 3.0 exchange ibcfg
  if(write(sockfd, ibcfg, sizeof ibcfg) != sizeof ibcfg){
    perror("Could not send ibcfg to peer");
    return;
  }
  if(read(sockfd, ibcfg, sizeof ibcfg)!= sizeof ibcfg){
    perror("Could not receive ibcfg from peer");
    return;
  }
  getibcfg(ibcfg,&ibcon);
  print_ib_con(&ibcon);
  
  //STEP 3.1 change to RTR
  qp_change_state_rtr(ibcon.qp,&ibcon);
  //STEP 3 send Request
  Request req;
  uint8_t err_code=0;
  int i = 0;
  for(;i<argc-5;i++)
    req.page_flags[i]=(uint8_t)atoi(argv[i+5]);
  if(i<MAX_PAGE)req.page_flags[i]=255;
  send(sockfd,(void*)&req.page_flags,16,0);
  recv(sockfd,(void*)&err_code,1,MSG_WAITALL);
  close(sockfd);
  printf("err_code=%d\n",err_code);
  if(err_code==0){
    for(i=0;i<argc-5;i++)
      printf("%c%c%c\n",
        ((char*)ctxt.pages)[i*page_size],
        ((char*)ctxt.pages)[i*page_size+page_size/2],
        ((char*)ctxt.pages)[i*page_size+page_size-1]);
  }
  //STEP 4 clean and destroy connection.
  TEST_NZ(ibv_destroy_qp(ibcon.qp),"Could not destroy queue pair, ibv_destroy_qp");
  TEST_NZ(ibv_destroy_cq(ibcon.scq),"Could not destroy send completion queue, ibv_destroy_cq");
  TEST_NZ(ibv_destroy_cq(ibcon.rcq),"Could not destroy receive completion queue, ibv_destroy_cq");
  TEST_NZ(ibv_destroy_comp_channel(ibcon.ch),"Cloud not destroy completion channel, ibv_destroy_comp_channel");
  TEST_NZ(ibv_dereg_mr(ctxt.mr), "Could not de-registermemory region, ibv_dereg_mr");
  TEST_NZ(ibv_dealloc_pd(ctxt.pd), "Could not deallocate protection domain, ibv_dealloc_pd");
}


int main(int argc, char ** argv){
  if(argc < 4){
    fprintf(stderr, hlp_info, NULL);
    fprintf(stderr,"available devices:\n");
    list_devices();
    fprintf(stderr,"please use `ibstat` to show port details.\n");
    return -1;
  }

  if(strcmp(argv[1],"svr")==0){
    doServer(argc,argv);
  }else if(strcmp(argv[1],"cli")==0){
    doClient(argc,argv);
  }else{
    fprintf(stderr, "cannot recognize command '%s'\n", argv[1]);
    fprintf(stderr, hlp_info, NULL);
    return -1;
  }
  return 0;
}
