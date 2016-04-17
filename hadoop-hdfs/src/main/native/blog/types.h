#include <stdint.h>
#include "map.h"

#define BLOCK_MAP_SIZE 1024
#define LOG_MAP_SIZE 64
#define SNAPSHOT_MAP_SIZE 64

typedef char* page_t;
typedef struct log log_t;
typedef struct block block_t;
typedef struct snapshot_block snapshot_block_t;
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
  MAP_TYPE(log) *log_map_hlc; // by hlc
  MAP_TYPE(log) *log_map_ut; // by user timestamp
  MAP_TYPE(snapshot) *snapshot_map;
  uint32_t blog_fd;
};

struct snapshot {
  uint64_t ref_count;
  uint32_t status : 4;
  uint32_t length : 28;
  uint64_t pages;
};

/**
 * Filesystem structure.
 * block_size   :   blocksize in bytes.
 * page_size    :   pagesize in bytes.
 * block_map    :   hash map that contains all the blocks in the current state
 *                  (key: block id).
 * page_base    :   base address for the page
 * page_fd      :   page file descriptor
 * alive        :   if the persistent thread is alive or not?
 * writer_thrd  :   thread responsible for pushing the blog to the disk for
 *                  persistance.
 */
struct filesystem {
  size_t block_size;
  size_t page_size;
  MAP_TYPE(block) *block_map;
#define FS_PAGE(fs,pn) (fs->page_base+(fs->page_size*(pn)))
  void   *page_base;
  uint32_t page_fd;
  uint32_t int_sec;
  uint32_t alive;
  pthread_t writer_thrd;
};
