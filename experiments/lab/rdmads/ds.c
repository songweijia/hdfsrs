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

#define RDMA_WRID 3



#define TEST_NZ(x,y) do { if ((x)) die(y); } while (0)
#define TEST_Z(x,y) do { if (!(x)) die(y); } while (0)
#define TEST_N(x,y) do { if ((x)<0) die(y); } while (0)

#define PAGE_SIZE (4096)
#define MAX_PAGE (16)
#define	SVR_PORT (18515)

/*
 * All clients and server has 16 pages. The client requests
 * the server to fill a set of pages.
 */

const char * hlp_info = " Usage: \n \
 ds svr \n \
 ds cli <sip> 0 3 6 9 \n";

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
} AppCtxt;

static AppCtxt ctxt;

static void init_ctxt(){
  // STEP 1 context
  TEST_NZ(posix_memalign(&ctxt.pages, PAGE_SIZE, PAGE_SIZE*MAX_PAGE),"could not allocate working buffer ctx->pages");
  memset(ctxt.pages, 0, PAGE_SIZE*MAX_PAGE);
  struct ibv_device **dev_list;
  TEST_Z(dev_list = ibv_get_device_list(NULL),"No IB-device available. get_device_list returned NULL");
  TEST_Z(ctxt.dev=dev_list[0],"IB-device could not be assigned. Maybe dev_list array is empty");
  TEST_Z(ctxt.context=ibv_open_device(ctxt.dev),"Could not create context, ibv_open_device");
  TEST_Z(ctxt.pd=ibv_alloc_pd(ctxt.context),"Could not allocate protection domain, ibv_alloc_pd");
  TEST_Z(ctxt.mr=ibv_reg_mr(ctxt.pd, ctxt.pages, PAGE_SIZE*MAX_PAGE, 
         IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE), "Could not allocate mr, ibv_reg_mr. Do you have root access?");
}

static void init_pages(int isServer){
  if(ctxt.pages==NULL)return;
  if(isServer){
    printf("--init server pages--\n");
    int i=0,j=0;
    char ch = 'A';
    for(i=0;i<MAX_PAGE;i++){
      printf("[%c]",ch);
      for(j=0;j<PAGE_SIZE;j++){
        ((char*)ctxt.pages)[i*PAGE_SIZE+j]=ch;
      }
      ch ++;
    }
    printf("\n");
    for(i=0;i<MAX_PAGE;i++)
      printf("[%c]",((char*)ctxt.pages)[i*PAGE_SIZE]);
    printf("\n");
  }else{
    memset(ctxt.pages,'Z',MAX_PAGE*PAGE_SIZE);
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
  int l_lid,r_lid;
  int l_qpn,r_qpn;
  int l_psn,r_psn;
  unsigned l_rkey,r_rkey;
  unsigned long long l_vaddr,r_vaddr; // we dont need l_vaddr because it is decided by request?
} Connection;

static void print_ib_con(Connection *ibcon){
  printf("Local:  LID %#04x, QPN %#06x, PSN %#06x RKey %#08x VAddr %#016Lx\n",
    ibcon->l_lid, ibcon->l_qpn, ibcon->l_psn, ibcon->l_rkey, ibcon->l_vaddr);
  printf("Remote: LID %#04x, QPN %#06x, PSN %#06x RKey %#08x VAddr %#016Lx\n",
    ibcon->r_lid, ibcon->r_qpn, ibcon->r_psn, ibcon->r_rkey, ibcon->r_vaddr);
}

char ibcfg[sizeof "0000:000000:000000:00000000:0000000000000000"];

static void setibcfg(char *ibcfg, Connection *ibcon){
  sprintf(ibcfg, "%04x:%06x:%06x:%08x:%016Lx", 
    ibcon->l_lid, ibcon->l_qpn, ibcon->l_psn, ibcon->l_rkey, ibcon->l_vaddr);
}

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

static int qp_change_state_rtr(struct ibv_qp *qp, Connection * ibcon){
  struct ibv_qp_attr *attr;
  attr = malloc(sizeof *attr);
  memset(attr, 0, sizeof *attr);

  attr->qp_state = IBV_QPS_RTR;
  attr->path_mtu = IBV_MTU_2048;
  attr->dest_qp_num = ibcon->r_qpn;
  attr->rq_psn = ibcon->r_psn;
  attr->max_dest_rd_atomic = 1;
  attr->min_rnr_timer = 12;
  attr->ah_attr.is_global = 0;
  attr->ah_attr.dlid = ibcon->r_lid;
  attr->ah_attr.sl = 1;
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
  attr->timeout = 14;
  attr->retry_cnt = 7;
  attr->rnr_retry = 7;
  attr->sq_psn = ibcon->l_psn;
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
  TEST_Z(ibcon.scq=ibv_create_cq(ctxt.context,100,&ibcon,ibcon.ch,0),"Could not create send completion queue, ibv_create_cq");
  struct ibv_qp_init_attr qp_init_attr = {
    .send_cq = ibcon.scq,
    .recv_cq = ibcon.rcq,
    .qp_type = IBV_QPT_RC,
    .cap = {
      .max_send_wr = 100,
      .max_recv_wr = 1,
      .max_send_sge = MAX_PAGE,
      .max_recv_sge = 1,
      .max_inline_data = 0
    }
  };
  TEST_Z(ibcon.qp=ibv_create_qp(ctxt.pd,&qp_init_attr),"Could not create queue pair, ibv_create_qp");
  qp_change_state_init(ibcon.qp,ibcon.port);
  struct ibv_port_attr attr;
  TEST_NZ(ibv_query_port(ctxt.context,ibcon.port,&attr),"Could not get port attributes, ibv_query_port");
  ibcon.l_lid = attr.lid;
  ibcon.l_qpn = ibcon.qp->qp_num;
  ibcon.l_psn = lrand48() & 0xffffff;
printf("l_psn=%d\n",ibcon.l_psn);
  ibcon.l_rkey = ctxt.mr->rkey;
  ibcon.l_vaddr = (uintptr_t)ctxt.pages;
  char msg[sizeof "0000:000000:000000:00000000:0000000000000000"];
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
  int parsed = sscanf(msg, "%x:%x:%x:%x:%Lx",
    &ibcon.r_lid,&ibcon.r_qpn,&ibcon.r_psn,&ibcon.r_rkey,&ibcon.r_vaddr);
  if(parsed!=5){
    fprintf(stderr, "Could not parse message from peer.");
  }
  print_ib_con(&ibcon);
  //change to RTS
  qp_change_state_rts(ibcon.qp,&ibcon);

  // read command: a fixed-length byte array: byte[MAX_PAGE]
  recv(connfd,(void*)&req.page_flags,MAX_PAGE,MSG_WAITALL);
  int i=0;
  //RDMA write
  struct ibv_sge *sge_list = malloc(sizeof(struct ibv_sge)*MAX_PAGE);
  for(;i<MAX_PAGE;i++){
    if(req.page_flags[i]==255)break;
    printf("%d ",req.page_flags[i]);
    (sge_list+i)->addr = (uintptr_t)ctxt.pages+PAGE_SIZE*req.page_flags[i];
    (sge_list+i)->length = PAGE_SIZE;
    (sge_list+i)->lkey = ctxt.mr->lkey;
  }
  printf("\n");
  struct ibv_send_wr wr;
  wr.wr.rdma.remote_addr = ibcon.r_vaddr;
  wr.wr.rdma.rkey = ibcon.r_rkey;
  wr.wr_id = RDMA_WRID;
  wr.sg_list = sge_list;
  wr.num_sge = i;
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.next = NULL;
  struct ibv_send_wr *bad_wr;
  TEST_NZ(ibv_post_send(ibcon.qp,&wr,&bad_wr),"ibv_post_send failed. This is bad mkay");
  int ne;
  struct ibv_wc wc;
  do{
    ne = ibv_poll_cq(ibcon.scq,100,&wc);
  }while(ne==0);

  if(ne<0){
    fprintf(stderr, "%s: poll CQ failed %d\n", __func__, ne);
  }else
    fprintf(stdout, "I received %d wc entries.\n", ne);

  if(wc.status != IBV_WC_SUCCESS) {
    fprintf(stderr, "%d:%s: Completion with error at %s:\n", getpid(), __func__, "server");
    fprintf(stderr, "%d:%s: Failed status %d: wr_id %ld\n", getpid(), __func__, wc.status, wc.wr_id);
    res.err_code = 1;
  }

  // write notification: a byte: 0 for success, otherwise failure.
  send(connfd,(void*)&res.err_code,1,0);
  close(connfd);
  // clean
  TEST_NZ(ibv_destroy_qp(ibcon.qp),"Could not destroy queue pair, ibv_destroy_qp");
  TEST_NZ(ibv_destroy_cq(ibcon.scq),"Could not destroy send completion queue, ibv_destroy_cq");
  TEST_NZ(ibv_destroy_cq(ibcon.rcq),"Could not destroy receive completion queue, ibv_destroy_cq");
  TEST_NZ(ibv_destroy_comp_channel(ibcon.ch),"Cloud not destroy completion channel, ibv_destroy_comp_channel");

  return NULL;
}

static void doServer(){
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
  // initialize context
  init_ctxt();
  // initialize page data;
  init_pages(1);
  ///struct sockaddr_in sin;
  TEST_N(asprintf(&service,"%d", SVR_PORT), "ERROR writing port number to port string.");
  TEST_N(n=getaddrinfo(NULL,service,&hints,&res), "getaddrinfo threw error");
  TEST_N(sockfd=socket(res->ai_family, res->ai_socktype, res->ai_protocol), "Could not create server socket");
  setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);
  // STEP 2 binding
  TEST_N(bind(sockfd,res->ai_addr,res->ai_addrlen), "Could not bind addr to socket");
  listen(sockfd, 1);
  // STEP 3 receiving requests
  while(1){
    pthread_t tid;
    TEST_N(connfd = accept(sockfd, NULL, 0), "server accept failed.");
    pthread_create(&tid,NULL,server_routine,&connfd);
  }
  
}

static void doClient(int argc, char ** argv){
  // STEP 1 initialization
  init_ctxt();
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
  TEST_Z(ibcon.scq=ibv_create_cq(ctxt.context,100,&ibcon,ibcon.ch,0),"Could not create send completion queue, ibv_create_cq");
  struct ibv_qp_init_attr qp_init_attr = {
    .send_cq = ibcon.scq,
    .recv_cq = ibcon.rcq,
    .qp_type = IBV_QPT_RC,
    .cap = {
      .max_send_wr = 100,
      .max_recv_wr = 1,
      .max_send_sge = MAX_PAGE,
      .max_recv_sge = 1,
      .max_inline_data = 0
    }
  };
  TEST_Z(ibcon.qp=ibv_create_qp(ctxt.pd,&qp_init_attr),"Could not create queue pair, ibv_create_qp");
  qp_change_state_init(ibcon.qp,ibcon.port);
  struct ibv_port_attr attr;
  TEST_NZ(ibv_query_port(ctxt.context,ibcon.port,&attr),"Could not get port attributes, ibv_query_port");
  ibcon.l_lid = attr.lid;
  ibcon.l_qpn = ibcon.qp->qp_num;
  ibcon.l_psn = lrand48() & 0xffffff;
printf("l_psn=%d\n",ibcon.l_psn);
  ibcon.l_rkey = ctxt.mr->rkey;
  ibcon.l_vaddr = (uintptr_t)ctxt.pages;
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
  TEST_N(getaddrinfo(argv[2], service, &hints, &res), "getaddrinfo threw error");
  //STEP 2 connect
  for(t = res; t; t=t->ai_next){
    TEST_N(sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol),"Could not create client socket");
    TEST_N(connect(sockfd,t->ai_addr,t->ai_addrlen),"Could not connect to server");
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
  int parsed = sscanf(ibcfg, "%x:%x:%x:%x:%Lx",
    &ibcon.r_lid,&ibcon.r_qpn,&ibcon.r_psn,&ibcon.r_rkey,&ibcon.r_vaddr);
  if(parsed!=5){
    fprintf(stderr, "Could not parse message from peer.");
  }
  print_ib_con(&ibcon);
  
  //STEP 3.1 change to RTR
  qp_change_state_rtr(ibcon.qp,&ibcon);
  //STEP 3 send Request
  Request req;
  uint8_t err_code=0;
  int i = 0;
  for(;i<argc-3;i++)
    req.page_flags[i]=(uint8_t)atoi(argv[i+3]);
  if(i<MAX_PAGE)req.page_flags[i]=255;
  send(sockfd,(void*)&req.page_flags,16,0);
  recv(sockfd,(void*)&err_code,1,MSG_WAITALL);
  close(sockfd);
  printf("err_code=%d\n",err_code);
  if(err_code==0){
    for(i=0;i<argc-3;i++)
      printf("%c%c%c\n",
        ((char*)ctxt.pages)[i*PAGE_SIZE],
        ((char*)ctxt.pages)[i*PAGE_SIZE+2048],
        ((char*)ctxt.pages)[i*PAGE_SIZE+4095]);
  }
  //STEP 4 clean and destroy connection.
  // TODO
  TEST_NZ(ibv_destroy_qp(ibcon.qp),"Could not destroy queue pair, ibv_destroy_qp");
  TEST_NZ(ibv_destroy_cq(ibcon.scq),"Could not destroy send completion queue, ibv_destroy_cq");
  TEST_NZ(ibv_destroy_cq(ibcon.rcq),"Could not destroy receive completion queue, ibv_destroy_cq");
  TEST_NZ(ibv_destroy_comp_channel(ibcon.ch),"Cloud not destroy completion channel, ibv_destroy_comp_channel");
  TEST_NZ(ibv_dereg_mr(ctxt.mr), "Could not de-registermemory region, ibv_dereg_mr");
  TEST_NZ(ibv_dealloc_pd(ctxt.pd), "Could not deallocate protection domain, ibv_dealloc_pd");
}


int main(int argc, char ** argv){
  if(argc < 2){
    fprintf(stderr, hlp_info, NULL);
    return -1;
  }

  if(strcmp(argv[1],"svr")==0){
    doServer();
  }else if(strcmp(argv[1],"cli")==0){
    doClient(argc,argv);
  }else{
    fprintf(stderr, "cannot recognize command '%s'\n", argv[1]);
    fprintf(stderr, hlp_info, NULL);
    return -1;
  }
  return 0;
}
