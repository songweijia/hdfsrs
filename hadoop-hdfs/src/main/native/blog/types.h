#include <stdlib.h>
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

struct block {
  int length;
  int cap;
  page_t **pages;
  int64_t last_entry;
};

struct snapshot {
  MAP_TYPE(block) *block_map;
  int64_t ref_count;
};

struct filesystem {
  size_t block_size;
  size_t page_size;
  MAP_TYPE(block) *block_map;
  MAP_TYPE(log) *log_map;
  MAP_TYPE(snapshot) *snapshot_map;
  size_t log_cap;
  size_t log_length;
  log_t *log;
  pthread_rwlock_t lock;
};
