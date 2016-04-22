#include <stdint.h>
#include "map.h"

#define BLOCK_MAP_SIZE 1024
#define LOG_MAP_SIZE 64
#define SNAPSHOT_MAP_SIZE 64

typedef char* page_t;
typedef struct log log_t;
typedef struct block block_t;
typedef struct filesystem filesystem_t;
typedef struct snapshot snapshot_t;

#ifdef DEBUG
#define DEBUG_PRINT(arg,fmt...) {fprintf(stdout,arg, ##fmt );fflush(stdout);}
#else
#define DEBUG_PRINT(arg,fmt...)
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
 * blog_fd      :   fd to write to each of the block.
 */
struct block {
  uint64_t id;
  uint64_t log_length;
  uint64_t log_cap;
  uint32_t status : 4;
  uint32_t length : 28;
  uint32_t pages_cap;
  uint64_t *pages;
  log_t *log;
  BLOG_MAP_TYPE(log) *log_map_hlc; // by hlc
  BLOG_MAP_TYPE(log) *log_map_ut; // by user timestamp
  BLOG_MAP_TYPE(snapshot) *snapshot_map;
  //The following members are for data persistent routine
  uint32_t log_length_pers;
#define BLOG_RDLOCK(b) pthread_rwlock_rdlock(&(b)->blog_rwlock)
#define BLOG_WRLOCK(b) pthread_rwlock_wrlock(&(b)->blog_rwlock)
#define BLOG_UNLOCK(b) pthread_rwlock_unlock(&(b)->blog_rwlock)
  pthread_rwlock_t blog_rwlock; // protect blog of a block.
};

struct snapshot {
  uint64_t ref_count;
  uint32_t status : 4;
  uint32_t length : 28;
  uint64_t *pages;
};

/**
 * Filesystem structure.
 * block_size   :   blocksize in bytes.
 * page_size    :   pagesize in bytes.
 * block_map    :   hash map that contains all the blocks in the current state
 *                  (key: block id).
 * page_base    :   base address for the page
 * nr_pages     :   number of pages in the filesystem
 * page_shm_fd  :   ramdisk(tmpfs) page file descriptor
 * page_fd      :   on-disk page file descriptor
 * alive        :   if the persistent thread is alive or not?
 * nr_pages_pers:   number of pages in persistent state.
 * pers_thrd    :   thread responsible for pushing the blog to the disk for
 *                  persistance.
 */
struct filesystem {
  size_t block_size;
  size_t page_size;
  BLOG_MAP_TYPE(block) *block_map;
  pthread_spinlock_t pages_spinlock;
#define PAGE_NR_TO_PTR(fs,nr) ((fs)->page_base+((fs)->page_size*(nr)))
#define PAGE_PTR_TO_NR(fs,ptr) (((void*)(ptr) - (fs)->page_base)/(fs)->page_size)
#define INVALID_PAGE_NO  (0xFFFFFFFFFFFFFFFF)
  void *page_base;
  uint64_t nr_pages;
  uint64_t nr_pages_pers;
  //The following members are for data persistent routine
  uint32_t page_fd;
  uint32_t page_shm_fd;
  uint32_t int_sec;
  uint32_t alive;
  pthread_t pers_thrd;
  char pers_path[256];
};
