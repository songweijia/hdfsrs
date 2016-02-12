#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <inttypes.h>
#include <jni.h>

#include "map.h"

#define BLOCK_MAP_SIZE 1024
#define SNAPSHOT_BLOCK_MAP_SIZE 1024
#define LOG_MAP_SIZE 64
#define SNAPSHOT_MAP_SIZE 64

typedef char* page_t;
typedef struct log log_t;
typedef struct block block_t;
typedef struct snapshot_block snapshot_block_t;
typedef struct filesystem filesystem_t;
typedef struct snapshot snapshot_t;
typedef struct disk_log disk_log_t;

#ifdef PRELOAD_MAPPED_FILE
  #define PAGE_FILE_SIZE (1024L*1024*1024*32)
  #define LOG_CAP	(1024L*1024*32)
  #define LOG_FILE_SIZE (LOG_CAP*sizeof(log_t))
#else
  #define PAGE_FILE_SIZE (1024L*1024*1024*1024)
  #define LOG_CAP	(1024L*1024*1024)
  #define LOG_FILE_SIZE (LOG_CAP*sizeof(log_t))
#endif
#define FLUSH_INTERVAL_SEC (10)

#ifdef DEBUG
#define DEBUG_PRINT(arg,fmt...) {fprintf(stdout,arg, ##fmt );fflush(stdout);}
#else
#define DEBUG_PRINT(arg,fmt...)
#endif

MAP_DECLARE(block, block_t);
MAP_DECLARE(snapshot_block, snapshot_block_t);
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
 * block_id     :   block that is modified.
 * pages_offset :   -1 for CREATE BLOCK,
 *                  -2 for DELETE BLOCK,
 *                  N for WRITE that starts at N page.
 * page_id      :   page in the block; 
 * pages_length :   number of pages written at WRITE,
 *                  0 otherwise.
 * previous     :   previous Log Entry of the same block.
 * r            :   real-time component of HLC.
 * c            :   logical component of HLC.
 * first_pn     :   page number of the first page we write.
 *              :   reused as generation time stamp for SET_GENSTAMP log
 */
struct log {
  uint64_t block_id;
  uint32_t op : 4;
  uint32_t block_length : 28;
  uint32_t page_id;
  uint32_t nr_pages;
  uint64_t previous;
  uint64_t r;
  uint64_t l;
  uint64_t first_pn;
};

/**
 * Block Entry structure.
 * length       :   length of the block in bytes.
 * cap          :   storage allocated for this block in memory.
 * last_entry   :   last Log Entry for this block.
 * pages        :   pointer array that stores pointers of different pages.
 */
struct block {
  uint64_t log_index;
  uint32_t length;
  uint32_t pages_cap;
  page_t *pages;
};

struct snapshot_block {
  uint32_t status : 4;
  uint32_t length : 28;
  uint32_t pages_cap;
  page_t *pages;
};

/**
 * Snapshot structure.
 * block_map    :   hash map that contains all the blocks instantiated for this
 *                  snapshot (key: block id).
 * ref_count    :   counter for how many accesses to this snapshot are
 *                  made.
 */
struct snapshot {
  uint64_t ref_count;
  BLOG_MAP_TYPE(snapshot_block) *block_map;
};

/**
 * Filesystem structure.
 * block_size   :   blocksize in bytes.
 * page_size    :   pagesize in bytes.
 * block_map    :   hash map that contains all the blocks in the current state
 *                  (key: block id).
 * log_map      :   hash map that maps from RTC values to Snapshot IDs.
 * snapshot_map :   hash map that maps from Snapshot IDs to Snapshot structure.
 * log_cap      :   Log Entries allocated in memory for blog.
 * log_length   :   Log Entries utilized in blog.
 * log          :   pointer to blog.
 * lock         :   lock for appends in the blog.
 * writer_thrd  :   thread responsible for pushing the blog to the disk for
 *                  persistance.
 */
struct filesystem {
  size_t block_size;
  size_t page_size;
  uint64_t *log_length;
  uint64_t *page_nr;
  log_t *log;
#define FS_PAGE(fs,pn) (fs->page_base+(fs->page_size*(pn)))
  void   *page_base;
  void   *log_base;
  pthread_rwlock_t log_lock;
  pthread_mutex_t page_lock;
  BLOG_MAP_TYPE(log) *log_map;
  BLOG_MAP_TYPE(snapshot) *snapshot_map;
  BLOG_MAP_TYPE(block) *block_map;
  uint32_t log_fd;
  uint32_t page_fd;
// persistent configuration
  uint32_t int_sec;
  uint32_t alive; // is the persistent thread is alive
  pthread_t writer_thrd;

};
