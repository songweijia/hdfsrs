#include <stdint.h>
#include <sys/queue.h>
#include <semaphore.h>
#include "map.h"
#include "InfiniBandRDMA.h"

#define BLOCK_MAP_SIZE 1024
#define LOG_MAP_SIZE 64
#define SNAPSHOT_MAP_SIZE 64

typedef char* page_t;
typedef struct log log_t;
typedef struct block block_t;
typedef struct filesystem filesystem_t;
typedef struct snapshot snapshot_t;
typedef struct pers_queue_entry pers_event_t;

#ifndef DEBUG_PRINT

  #ifdef DEBUG
  #define DEBUG_PRINT(arg,fmt...) {fprintf(stdout,arg, ##fmt );fflush(stdout);}
  #else
  #define DEBUG_PRINT(arg,fmt...)
  #endif

#endif

#ifndef DEBUG_TIMESTAMP

  #ifdef DEBUG
  #define DEBUG_TIMESTAMP(t) gettimeofday(&t,NULL)
  #else
  #define DEBUG_TIMESTAMP(t)
  #endif

  #define TIMESPAN(t1,t2) ((t2.tv_sec-t1.tv_sec)*1000000+(t2.tv_usec-t1.tv_usec))

#endif

MAP_DECLARE(block, block_t);
MAP_DECLARE(log, uint64_t);
MAP_DECLARE(snapshot, snapshot_t);

enum OPERATION {
  BOL = 0,
  CREATE_BLOCK = 1,
  DELETE_BLOCK = 2,
  WRITE = 3,
  SET_GENSTAMP = 4
};

enum STATUS {
  ACTIVE,
  NON_ACTIVE
};

/**
 * Log Entry structure.
 * op           :   operation
 * block_length :   length of the block at point of log time
 * start_page   :   number of the first page in the block
 * nr_pages     :   number of pages
 * r            :   real-time component of HLC.
 * l            :   logical component of HLC.
 * u            :   user defined timestamp.
 * first_pn     :   page number of the first page we write.
 *              :   reused as generation time stamp for SET_GENSTAMP log
 */
struct log {
  uint32_t op : 4;
  uint32_t block_length : 28;
  uint32_t start_page;
  uint32_t nr_pages;
  uint32_t reserved; // padding
  uint64_t r;
  uint64_t l;
  uint64_t u;
  uint64_t first_pn;
};

/**
 * Block Entry structure.
 * id           :   block id
 * log_length   :   number of the log entries
 * log_cap      :   log capacity
 * status       :   status of the block
 * length       :   length of the block in bytes
 * cap          :   storage allocated for this block in memory.
 * last_entry   :   last Log Entry for this block.
 * pages        :   index array that stores pointers of different pages.
 * log          :   log entry array
 * log_map_hlc  :   map from hlc to log entry
 * log_map_ut   :   map from ut to log entry
 * snapshot_map :   map from log entry to snapshot
 * blog_fd      :   blog file descriptor
 * page_fd      :   page file descriptor
 */
struct block {
  uint64_t id;
  uint64_t log_length;
  uint64_t log_cap;
  uint32_t status : 4;
  uint32_t length : 28;
  uint32_t pages_cap;
  uint64_t *pages;
  volatile log_t *log;
  BLOG_MAP_TYPE(log) *log_map_hlc; // by hlc
  BLOG_MAP_TYPE(log) *log_map_ut; // by user timestamp
  BLOG_MAP_TYPE(snapshot) *snapshot_map;
  //The following members are for data persistent routine
  uint64_t log_length_pers;
#define BLOG_RDLOCK(b) pthread_rwlock_rdlock(&(b)->blog_rwlock)
#define BLOG_WRLOCK(b) pthread_rwlock_wrlock(&(b)->blog_rwlock)
#define BLOG_UNLOCK(b) pthread_rwlock_unlock(&(b)->blog_rwlock)
  pthread_rwlock_t blog_rwlock; // protect blog of a block.
  int blog_fd;
  int page_fd;
};

struct snapshot {
  uint64_t ref_count;
  uint32_t status : 4;
  uint32_t length : 28;
  uint64_t *pages;
};

TAILQ_HEAD(_pers_queue, pers_queue_entry);
struct pers_queue_entry {
  block_t *block;   // block pointer; NULL for End-OF-Queue
  uint64_t log_length; // The latest log updated to this length
  TAILQ_ENTRY(pers_queue_entry) lnk; // link pointers
}; 
#define IS_EOQ(e) (e->block == NULL)

/**
 * Filesystem structure.
 * block_size   :   blocksize in bytes.
 * page_size    :   pagesize in bytes.
 * block_map    :   hash map that contains all the blocks in the current state
 *                  (key: block id).
 * page_base    :   base address for the page
 * nr_pages     :   number of pages in the filesystem
 * page_shm_fd  :   ramdisk(tmpfs) page file descriptor
 * nr_pages_pers:   number of pages in persistent state.
 * pers_thrd    :   thread responsible for pushing the blog to the disk for
 *                  persistance.
 * pers_queue   :   persistent message queue.
 * pers_queue_sem : semaphore for the persistent message queue.
 */
struct filesystem {
  size_t block_size;
  size_t page_size;
  BLOG_MAP_TYPE(block) *block_map;
  pthread_spinlock_t pages_spinlock;
  pthread_spinlock_t clock_spinlock;
#define PAGE_NR_TO_PTR(fs,nr) ((void*)((char *)(fs)->page_base+((fs)->page_size*(nr))))
#define PAGE_PTR_TO_NR(fs,ptr) (((char*)(ptr) - (char*)(fs)->page_base)/(fs)->page_size)
#define INVALID_PAGE_NO  (~0x0L)
  void *page_base;
  uint64_t pool_size; // memory pool size
  uint64_t nr_pages;
  //The following members are for data persistent routine
  uint32_t page_shm_fd;
  pthread_t pers_thrd;
  char pers_path[256];
  #define PERS_ENQ(fs,e) do{ \
    if (e->block != NULL) { \
        DEBUG_PRINT("Enqueue: Block %" PRIu64 " Operation %" PRIu32 "\n", e->block->id,  e->block->log[e->log_length-1].op); \
    } else { \
        DEBUG_PRINT("Enqueue: Null Event\n"); \
    } \
    pthread_spin_lock(&(fs)->queue_spinlock); \
    TAILQ_INSERT_TAIL(&(fs)->pers_queue,e,lnk); \
    pthread_spin_unlock(&(fs)->queue_spinlock); \
    if(sem_post(&fs->pers_queue_sem) !=0){ \
      fprintf(stderr,"Error: failed post semphore, Error: %s\n", strerror(errno)); \
      exit(1); \
    } \
  }while(0)
  #define PERS_DEQ(fs,pe) do{ \
    if(sem_wait(&fs->pers_queue_sem) != 0){ \
      fprintf(stderr,"Error: failed waiting on semphore, Error: %s\n", strerror(errno)); \
      exit(1); \
    } \
    pthread_spin_lock(&(fs)->queue_spinlock); \
    if(!TAILQ_EMPTY(&(fs)->pers_queue)){ \
      *(pe) = TAILQ_FIRST(&(fs)->pers_queue); \
      TAILQ_REMOVE(&(fs)->pers_queue,*(pe),lnk); \
    } else *pe = NULL; \
    pthread_spin_unlock(&(fs)->queue_spinlock); \
  }while(0)
  struct _pers_queue pers_queue;
  pthread_spinlock_t queue_spinlock;
  sem_t pers_queue_sem;

  RDMACtxt *rdmaCtxt; // RDMA Context
};

