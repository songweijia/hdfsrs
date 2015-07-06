#include <stdlib.h>

typedef struct page page_t;
typedef struct log log_t;
typedef struct block block_t;
typedef struct filesystem filesystem_t;
typedef struct snapshot snapshot_t;

struct page {
  char *data;
};

struct log {
  int64_t block_id;
  int block_length;
  int page_id;
  int nr_pages;
  page_t *pages;
  log_t *previous;
  int64_t rtc;
  size_t vc_length;
  char *vc;
};

struct block {
  int64_t id;
  int length;
  int cap;
  page_t **pages;
  log_t *last_entry;
  block_t *next;
};

struct filesystem {
  size_t block_size;
  size_t page_size;
  block_t **block_map;
  snapshot_t **snapshot_map;
  size_t log_cap;
  size_t log_length;
  log_t *log;
};

struct snapshot {
  int64_t id;
  int64_t last_entry;
  block_t **block_map;
  snapshot_t *next;
};
