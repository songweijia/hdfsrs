 #include <stdint.h>
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

MAP_DECLARE(block, block_t);
MAP_DECLARE(snapshot_block, snapshot_block_t);
MAP_DECLARE(log, uint64_t);
MAP_DECLARE(snapshot, snapshot_t);

enum OPERATION {
  BOL,
  CREATE_BLOCK,
  DELETE_BLOCK,
  WRITE
};

enum STATUS {
  ACTIVE,
  NON_ACTIVE
};

struct log {
  uint64_t block_id;
  uint32_t op : 4;
  uint32_t block_length : 28;
  uint32_t page_id;
  uint32_t nr_pages;
  uint64_t previous;
  uint64_t r;
  uint64_t l;
  page_t pages;
};

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

struct snapshot {
  uint64_t ref_count;
  MAP_TYPE(snapshot_block) *block_map;
};

struct filesystem {
  size_t block_size;
  size_t page_size;
  uint64_t log_length;
  uint64_t log_cap;
  log_t *log;
  pthread_rwlock_t log_lock;
  MAP_TYPE(log) *log_map;
  MAP_TYPE(snapshot) *snapshot_map;
  MAP_TYPE(block) *block_map;
};
