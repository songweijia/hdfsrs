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

MAP_DECLARE(block, block_t);
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
  uint32_t op : 4;
  uint32_t block_length : 28;
  uint32_t start_page;
  uint32_t nr_pages;
  uint64_t r;
  uint64_t l;
  uint64_t u; // user timestamp
  page_t pages;
};

struct block {
  uint64_t id;
  uint64_t log_length;
  uint64_t log_cap;
  uint32_t status : 4;
  uint32_t length : 28;
  uint32_t pages_cap;
  page_t *pages;
  log_t *log;
  MAP_TYPE(log) *log_map_hlc; // by hlc
  MAP_TYPE(log) *log_map_ut; // by user timestamp
  MAP_TYPE(snapshot) *snapshot_map;
};

struct snapshot {
  uint64_t ref_count;
  uint32_t status : 4;
  uint32_t length : 28;
  page_t *pages;
};

struct filesystem {
  size_t block_size;
  size_t page_size;
  MAP_TYPE(block) *block_map;
};
