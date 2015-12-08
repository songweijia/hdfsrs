#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <inttypes.h>
#include <jni.h>

#include "map.h"
#include "InfiniBandRDMA.h"

#define BLOCK_MAP_SIZE 4096
#define LOG_MAP_SIZE 64
#define SNAPSHOT_MAP_SIZE 64

typedef struct page page_t;
typedef struct log log_t;
typedef struct block block_t;
typedef struct filesystem filesystem_t;
typedef struct snapshot snapshot_t;
typedef struct disk_log disk_log_t;
typedef struct blog_writer_ctxt blog_writer_ctxt_t;

MAP_DECLARE(block, block_t);
MAP_DECLARE(log, uint64_t);
MAP_DECLARE(snapshot, snapshot_t);

/**
 *  Pointer to data.
 */
struct page {
  char *data;
};

/**
 * Log Entry structure.
 * block_id     :   block that is modified.
 * pages_offset :   -1 for CREATE BLOCK,
 *                  -2 for DELETE BLOCK,
 *                  N for WRITE that starts at N page.
 * pages_length :   number of pages written at WRITE,
 *                  0 otherwise.
 * previous     :   previous Log Entry of the same block.
 * r            :   real-time component of HLC.
 * c            :   logical component of HLC.
 * data         :   pointer to data written in case of WRITE,
 *                  null otherwise.
 */
struct log {
  uint64_t block_id;
  uint64_t block_length;
  int32_t  pages_offset;
  uint32_t pages_length;
  uint64_t previous;
  uint64_t r;
  uint64_t c;
  page_t *data;
};

/**
 * Disk Log Entry similar to Log Entry without the data.
 */
struct disk_log {
  uint64_t block_id;
  uint64_t block_length;
  int32_t  pages_offset;
  uint32_t pages_length;
  uint64_t previous;
  uint64_t r;
  uint64_t c;
};

/**
 * Block Entry structure.
 * length       :   length of the block in bytes.
 * cap          :   storage allocated for this block in memory.
 * last_entry   :   last Log Entry for this block.
 * pages        :   pointer array that stores pointers of different pages.
 */
struct block {
  uint32_t length;
  int32_t cap;
  uint64_t last_entry;
  page_t **pages;
};

/**
 * Snapshot structure.
 * block_map    :   hash map that contains all the blocks instantiated for this
 *                  snapshot (key: block id).
 * ref_count    :   counter for how many accesses to this snapshot are
 *                  made.
 */
struct snapshot {
  BLOG_MAP_TYPE(block) *block_map;
  uint64_t ref_count;
};

/**
 * Blog Writer Ctxt.
 * fs           :   filesystem to write.
 * log_fd       :   log file descriptor.
 * page_fd      :   page_file descriptor.
 * snap_fd      :   snapshot file descriptor.
 * next_entry   :   next Log Entry to be written.
 * int_sec      :   frequency (per second).
 * alive        :   whether or not the blog writer is alive.
 */
struct blog_writer_ctxt{
  filesystem_t *fs;
  uint32_t log_fd;
  uint32_t page_fd;
  uint32_t snap_fd;
  uint32_t int_sec; 
  uint64_t next_entry;
  uint32_t alive;
  JNIEnv *env;
  jobject thisObj;
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
 * bwc          :   writer context.
 * writer_thrd  :   thread responsible for pushing the blog to the disk for
 *                  persistance.
 */
struct filesystem {
  size_t block_size;
  size_t page_size;
  BLOG_MAP_TYPE(block) *block_map;
  BLOG_MAP_TYPE(log) *log_map;
  BLOG_MAP_TYPE(snapshot) *snapshot_map;
  size_t log_cap;
  size_t log_length;
  log_t *log;
  RDMACtxt *rdmaCtxt; // RDMA
  pthread_rwlock_t lock;
  blog_writer_ctxt_t bwc;
  pthread_t writer_thrd;
};
