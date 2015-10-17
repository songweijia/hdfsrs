#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>

#include "map.h"

#define BLOCK_MAP_SIZE 4096
#define LOG_MAP_SIZE 64
#define SNAPSHOT_MAP_SIZE 64

typedef struct page page_t;
typedef struct log log_t;
typedef struct block block_t;
typedef struct filesystem filesystem_t;
typedef struct snapshot snapshot_t;

MAP_DECLARE(block, block_t);
MAP_DECLARE(log, int64_t);
MAP_DECLARE(snapshot, snapshot_t);

struct page {
  char *data;
};

struct log {
  int64_t block_id;
  int block_length;
  int page_id;
  int nr_pages;
  page_t *pages;
  int64_t previous;
  int64_t r;
  int64_t c;
};

typedef struct disk_log {
  uint64_t block_id;
  uint64_t previous;
  uint64_t r;
  uint64_t c;
  uint32_t block_length;
  uint32_t page_id;
  uint32_t nr_pages;
  uint32_t reserved;
//  uint32_t pages[1];
} DiskLog;

struct block {
  int length;
  int cap;
  page_t **pages;
  int64_t last_entry;
};

struct snapshot {
  BLOG_MAP_TYPE(block) *block_map;
  int64_t ref_count;
};

typedef struct _blog_writer_ctxt{
  filesystem_t *fs;
  int log_fd;       // log file descriptor
  int page_fd;      // page file descriptor
  int snap_fd;      // snap file descriptor
  long next_entry;  // next blog entry to be written
  int int_sec;      // frequency in seconds
  volatile int alive;        // is the blog writer is alive?
} BlogWriterCtxt;


struct filesystem {
  size_t block_size;
  size_t page_size;
  BLOG_MAP_TYPE(block) *block_map;
  BLOG_MAP_TYPE(log) *log_map;
  BLOG_MAP_TYPE(snapshot) *snapshot_map;
  size_t log_cap;
  size_t log_length;
  log_t *log;
  pthread_rwlock_t lock;
  BlogWriterCtxt bwc; // writer context
  pthread_t writerThd; // writer thread
};
