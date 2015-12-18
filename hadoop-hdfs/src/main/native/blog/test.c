#include <stdio.h>
#include <unistd.h>
#include "InfiniBandRDMA.h"

#define TEST_NZ(x,y) do { if ((x)) die(y); } while (0)
#define TEST_Z(x,y) do { if (!(x)) die(y); } while (0)
#define TEST_N(x,y) do { if ((x)<0) die(y); } while (0)
static int die(const char *reason){                                                                     
  fprintf(stderr, "Err: %s - %s \n ", strerror(errno), reason);
  exit(EXIT_FAILURE);
  return -1;
}

// test -c -h host -p port
// test -s -p port
// options:
// -z poolsize order, default is 20 for 1MB
// -a page/buffer size order, default value is 12 for 4KB
int main(int argc, char **argv){
  int c;
  int mode = -1; // 0 - client; 1 - writing server; 2 - reading server; -1 - init
  char *host=NULL;
  unsigned short port=0;
  uint32_t psz=20;// the default pool size is 1MB.
  uint32_t align=12;// the default page or buffer size is 4KB.
  while((c=getopt(argc,argv,"cs:h:p:a:z:"))!=-1){
    switch(c){
    case 'c':
      if(mode!=-1){
        fprintf(stderr,"Please only specify -c or -s once.\n");
        return -1;
      }
      mode = 0;
      break;
    case 's':
      if(mode!=-1){
        fprintf(stderr,"Please only specify -c or -s once.\n");
        return -1;
      }
      if(optarg[0]=='w')
        mode = 1;
      else//reading
        mode = 2;
      break;
    case 'h':
      host = optarg;
      break;
    case 'p':
      port = (unsigned short)atoi(optarg);
      break;
    case 'z':
      psz = (uint32_t)atoi(optarg);
      break;
    case 'a':
      align = (uint32_t)atoi(optarg);
      break;
    }
  }
  printf("mode=%d,host=%s,port=%d,psz=%d,align=%d\n",mode,host,port,psz,align);
  if(mode == 0){
    // client
    RDMACtxt rdma_ctxt;
    int i;
    // step 1: initialize client
    TEST_NZ(initializeContext(&rdma_ctxt,psz,align,0),"initializeContext");
    for(i=0;i<(1<<(psz-align));i++)
      memset((void*)rdma_ctxt.pool+(i<<align),'A'+i,1<<align);
    // step 2: connect
    TEST_NZ(rdmaConnect(&rdma_ctxt,inet_addr(host),port),"rdmaConnect");
    // step 3: allocate buffer
    void *buf;
    while(1){
      TEST_NZ(allocateBuffer(&rdma_ctxt, &buf),"allocateBuffer");
      // step 4: initialize buffer
      bzero(buf,1<<align);
      printf("ipkey=%llx,vaddr=%p\n",(unsigned long long)inet_addr(host),buf);
      getchar();
      // step 5: wait for data being transfered.
      printf("Data(len=%d):[%c...%c...%c...%c]\n",1<<align,
        *(char *)buf,
        *(char *)(buf+(1<<12)),
        *(char *)(buf+(2<<12)),
        *(char *)(buf+(3<<12)));
    }
  }else if(mode == 1/*writing*/||mode == 2/*reading*/){
    // server
    RDMACtxt rdma_ctxt;
    int i;
    // step 1: initialize server
    TEST_NZ(initializeContext(&rdma_ctxt,psz,align,port),"initializeContext");
    for(i=0;i<(1<<(psz-align));i++)
      memset((void*)rdma_ctxt.pool+(i<<align),'0'+i,1<<align);
    // step 2:
    while(1){
      int pns[4];
      uint64_t len, *ids, rvaddr;
      int cipkey;
      int i,j;
      RDMAConnection *conn;
      printf("please give the remote ip(like:):\n");
      scanf("%x",&cipkey);
      printf("please give the remote address(like:):\n");
      scanf("%p",(void **)&rvaddr);
      printf("please name four pages to transfer (like 1 2 3 4):\n");
      scanf("%d %d %d %d",&pns[0],&pns[1],&pns[2],&pns[3]);
      printf("OK, I'm talking to client:%x\n",cipkey);
      // step 3: do transfer
      if(MAP_READ(con,rdma_ctxt.con_map,(uint64_t)cipkey,&conn) != 0){
        fprintf(stderr, "cannot get connection %x from connection map.\n",cipkey);
        return -1;
      }
      const void *pagelist[4];
      for(j=0;j<4;j++)
      {
        pagelist[j] = (void*)conn->l_vaddr+(pns[j]<<align);
        printf("transfer pagelist[%d]=%p\n",j,pagelist[j]);
      }
      if(mode == 1)
        rdmaWrite(&rdma_ctxt,cipkey,rvaddr,pagelist,4);
      else
        rdmaRead(&rdma_ctxt,cipkey,rvaddr,pagelist,4);
      fprintf(stdout, "transfer with client %x ... done.\n",cipkey);
      // step 5: wait for data being transfered.
      printf("Data(len=%d):[%c...%c...%c...%c]\n",1<<align,
        *(char *)pagelist[0],
        *(char *)pagelist[1],
        *(char *)pagelist[2],
        *(char *)pagelist[3]);
    }
  }else{
    fprintf(stderr,"USAGE: -c for client -s for blog server\n");
    fprintf(stderr,"\t-c client mode\n");
    fprintf(stderr,"\t-s <r|w> reading or writing server mode\n");
    fprintf(stderr,"\t-h <hostip>\n");
    fprintf(stderr,"\t-p <port>\n");
    fprintf(stderr,"\t-z <psz> pool size, default to 20 (1MB)\n");
    fprintf(stderr,"\t-a <align> page/buf size, default to 12 (4KB page/buffer)\n");
    return -1;
  }
  exit(0);
}
