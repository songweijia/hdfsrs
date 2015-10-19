#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <string.h>
#include <fcntl.h>

#include "JNIBlog.h"
#include "types.h"

// Define hash tables that are going to be used in the DataNode.
MAP_DEFINE(block, block_t, BLOCK_MAP_SIZE);
MAP_DEFINE(log, uint64_t, LOG_MAP_SIZE);
MAP_DEFINE(snapshot, snapshot_t, SNAPSHOT_MAP_SIZE);

// Define files for storing persistent data.
#define BLOGFILE "blog._dat"
#define PAGEFILE "page._dat"
#define SNAPFILE "snap._dat"
#define MAX_FNLEN (strlen(BLOGFILE)+strlen(PAGEFILE)+strlen(SNAPFILE))

static void print_page(page_t *page)
{
  printf("%s\n", page->data);
}

static void print_log(log_t *log)
{
  int32_t i;
  
  printf("Block ID: %"PRIu64"\n", log->block_id);
  printf("Block Length: %"PRIu64"\n", log->block_length);
  printf("Pages Offset: %"PRId32"\n", log->pages_offset);
  printf("Pages Length: %"PRIu32"\n", log->pages_length);
  if (log->previous != -1)
    printf("Previous: %"PRIu64"\n", log->previous);
  printf("HLC Value: (%"PRIu64",%"PRIu64")\n", log->r, log->c);
  for (i = 0; i < log->pages_length; i++) {
    printf("Page %"PRId32":\n", log->pages_offset + i);
    print_page(log->data + i);
  }
}

static void print_block(block_t *block, int page_size)
{
  uint32_t i = 0;

  printf("Length: %"PRIu32"\n", block->length);
  printf("Capacity: %"PRIu32"\n", block->cap);
  printf("Last Log Entry: %"PRIu64"\n", block->last_entry);
  if (block->length != 0) {
    for (i = 0; i <= (block->length - 1) / page_size; i++) {
      printf("Page %"PRIu32"\n", i);
      print_page(block->pages[i]);
    }
  }
}

static void print_snapshot(snapshot_t *snapshot, log_t *log, int page_size)
{
  block_t *block;
  uint64_t *ids;
  uint64_t length, i;
  
  printf("Blocks:\n");
  for (i = 0; i < BLOCK_MAP_SIZE; i++)
    MAP_LOCK(block, snapshot->block_map, i, 'r');
  length = MAP_LENGTH(block, snapshot->block_map);
  ids = MAP_GET_IDS(block, snapshot->block_map, length);
  for (i = 0; i < length; i++) {
    if (MAP_READ(block, snapshot->block_map, ids[i], &block) != 0) {
      fprintf(stderr, "Print Snapshot: Something is wrong with BLOCK_MAP_GET_IDS or BLOCK_MAP_READ\n");
      return;
    }
    print_block(block, page_size);
    printf("\n");
  }
  printf("\n");
  for (i = 0; i < BLOCK_MAP_SIZE; i++)
    MAP_UNLOCK(block, snapshot->block_map, i);
  free(ids);
}

static void print_filesystem(JNIEnv *env, filesystem_t *filesystem)
{
  block_t *block;
  snapshot_t *snapshot;
  uint64_t *ids;
  uint64_t length, i;
  uint64_t *log_id;
  
  pthread_rwlock_rdlock(&filesystem->lock);
  
  // Print Filesystem.
  printf("Filesystem\n");
  printf("----------\n");
  printf("Block Size: %zu\n", filesystem->block_size);
  printf("Page Size: %zu\n", filesystem->page_size);
  
  // Print Blocks.
  printf("Blocks:\n");
  for (i = 0; i < BLOCK_MAP_SIZE; i++)
    MAP_LOCK(block, filesystem->block_map, i, 'r');
  length = MAP_LENGTH(block, filesystem->block_map);
  ids = MAP_GET_IDS(block, filesystem->block_map, length);
  for (i = 0; i < length; i++) {
    if (MAP_READ(block, filesystem->block_map, ids[i], &block) != 0) {
      fprintf(stderr, "Print Filesystem: Something is wrong with BLOCK_MAP_GET_IDS or BLOCK_MAP_READ\n");
      return;
    }
    printf("Block ID: %ld\n", ids[i]);
    print_block(block, filesystem->page_size);
    printf("\n");
  }
  printf("\n");
  for (i = 0; i < BLOCK_MAP_SIZE; i++)
    MAP_UNLOCK(block, filesystem->block_map, i);
  free(ids);
  
  // Print Snapshots
  printf("Snapshots:\n");
  for (i = 0; i < LOG_MAP_SIZE; i++)
    MAP_LOCK(log, filesystem->log_map, i, 'r');
  length = MAP_LENGTH(log, filesystem->log_map);
  ids = MAP_GET_IDS(log, filesystem->log_map, length);
  for (i = 0; i < length; i++) {
    if (MAP_READ(log, filesystem->log_map, ids[i], &log_id) != 0) {
      fprintf(stderr, "Print Filesystem: Something is wrong with LOG_MAP_GET_IDS or LOG_MAP_READ\n");
      return;
    }
    MAP_LOCK(snapshot, filesystem->snapshot_map, *log_id, 'r');
    if (MAP_READ(snapshot, filesystem->snapshot_map, *log_id, &snapshot) != 0) {
      fprintf(stderr, "Print Filesystem: Something is wrong with LOG_MAP_GET_IDS or LOG_MAP_READ\n");
      return;
    }
    printf("RTC: %"PRIu64"\n", ids[i]);
    printf("Last Log Entry: %"PRIu64"\n", *log_id);
    print_snapshot(snapshot, filesystem->log, filesystem->page_size);
    printf("\n");
    MAP_UNLOCK(snapshot, filesystem->snapshot_map, *log_id);
  }
  printf("\n");
  for (i = 0; i < LOG_MAP_SIZE; i++)
    MAP_UNLOCK(log, filesystem->log_map, i);
  free(ids);
  
  printf("Log Capacity: %zu\n", filesystem->log_cap);
  printf("Log Length: %zu\n", filesystem->log_length);
  for (i = 0; i < filesystem->log_length; i++) {
    printf("Log %"PRIu64"\n", i);
    print_log(filesystem->log + i);
    printf("\n");
  }
  
  pthread_rwlock_unlock(&filesystem->lock);
}

block_t *find_or_allocate_snapshot_block(filesystem_t *filesystem, snapshot_t *snapshot, uint64_t last_log_entry,
                                         uint64_t block_id) {
  log_t *log = filesystem->log;
  log_t *cur_log;
  block_t *block;
  uint64_t log_id;
  uint32_t nr_unfilled_pages, i;
  
  MAP_LOCK(block, snapshot->block_map, block_id, 'w');
  if (MAP_CREATE(block, snapshot->block_map, block_id) == -1) {
    MAP_UNLOCK(block, snapshot->block_map, block_id);
    MAP_LOCK(block, snapshot->block_map, block_id, 'r');
    if (MAP_READ(block, snapshot->block_map, block_id, &block) == -1) {
      MAP_UNLOCK(block, snapshot->block_map, block_id);
      fprintf(stderr, "Find or Allocate Snapshot Block: Something is wrong with BLOCK_MAP_CREATE or BLOCK_MAP_READ.\n");
      return NULL;
    }
    MAP_UNLOCK(block, snapshot->block_map, block_id);
    return block;
  }
  
  block = (block_t *) malloc(sizeof(block_t));
  log_id = last_log_entry;
  while ((log_id >= 0) && (log[log_id].block_id != block_id))
    log_id--;
  if (log_id == -1 || log[log_id].pages_offset == -2) {
    block->length = 0;
    block->cap = -1;
    block->pages = NULL;
    block->last_entry = -1;
  } else if (log[log_id].pages_offset == -1) {
    block->length = 0;
    block->cap = 0;
    block->pages = NULL;
    block->last_entry = -1;
  } else {
    block->length = log[log_id].block_length;
    if (block->length == 0)
      block->cap = 0;
    else
      block->cap = (block->length - 1) / filesystem->page_size + 1;
    block->pages = (page_t**) malloc(block->cap*sizeof(page_t*));
    for (i = 0; i < block->cap; i++)
      block->pages[i] = NULL;
    cur_log = log + log_id;
    nr_unfilled_pages = block->cap;
    while (cur_log != NULL && nr_unfilled_pages > 0) {
      for (i = 0; i < cur_log->pages_length; i++) {
        if (block->pages[cur_log->pages_offset + i] == NULL) {
          nr_unfilled_pages--;
          block->pages[cur_log->pages_offset + i] = cur_log->data + i;
        }
      }
      if (cur_log->previous != -1)
        cur_log = log + cur_log->previous;
      else
        cur_log = NULL;
    }
    block->last_entry = log_id;
  }
  
  if (MAP_WRITE(block, snapshot->block_map, block_id, block) != 0) {
    fprintf(stderr, "Find or Allocate Snapshot Block: Something is wrong with BLOCK_MAP_CREATE or BLOCK_MAP_WRITE.\n");
    MAP_UNLOCK(block, snapshot->block_map, block_id);
    return NULL;
  }

  MAP_UNLOCK(block, snapshot->block_map, block_id);
  return block;
}

static void check_and_increase_log_length(filesystem_t *filesystem)
{
  if (filesystem->log_length == filesystem->log_cap) {
    filesystem->log = (log_t *) realloc(filesystem->log, filesystem->log_cap*2*sizeof(log_t));
    if (filesystem->log == NULL) {
      perror("Error: ");
      exit(1);
    }
    filesystem->log_cap *= 2;
  }
}

static int64_t read_local_rtc()
{
  struct timeval tv;
  int64_t rtc;
  
  gettimeofday(&tv,NULL);
  rtc = tv.tv_sec*1000 + tv.tv_usec/1000;
  return rtc;
}

static void update_log_clock(JNIEnv *env, jobject hlc, log_t *log) {
  jclass hlcClass = (*env)->GetObjectClass(env, hlc);
  jfieldID rfield = (*env)->GetFieldID(env, hlcClass, "r", "J");
  jfieldID cfield = (*env)->GetFieldID(env, hlcClass, "c", "J");

  log->r = (*env)->GetLongField(env, hlc, rfield);
  log->c = (*env)->GetLongField(env, hlc, cfield);
}

static void tick_hybrid_logical_clock(JNIEnv *env, jobject hlc, jobject mhlc)
{
  jclass hlcClass = (*env)->GetObjectClass(env, hlc);
  jmethodID mid = (*env)->GetMethodID(env, hlcClass, "tickOnRecv", "(Ledu/cornell/cs/sa/HybridLogicalClock;)V");

  if (mid == NULL) {
    perror("Error");
    exit(-1);
  }
  (*env)->CallObjectMethod(env, hlc, mid, mhlc);
}

static void mock_tick_hybrid_logical_clock(JNIEnv *env, jobject hlc, jlong rtc)
{
  jclass hlcClass = (*env)->GetObjectClass(env, hlc);
  jmethodID mid = (*env)->GetMethodID(env, hlcClass, "mockTick", "(J)V");
  
  if (mid == NULL) {
    perror("Error");
    exit(-1);
  }
  (*env)->CallObjectMethod(env, hlc, mid, rtc);
}

static filesystem_t *get_filesystem(JNIEnv *env, jobject thisObj)
{
  jclass thisCls = (*env)->GetObjectClass(env,thisObj);
  jfieldID fid = (*env)->GetFieldID(env,thisCls,"jniData","J");
  
  return (filesystem_t *) (*env)->GetLongField(env, thisObj, fid);
}

static jobject get_hybrid_logical_clock(JNIEnv *env, jobject thisObj)
{
  jclass thisCls = (*env)->GetObjectClass(env,thisObj);
  jfieldID fid = (*env)->GetFieldID(env,thisCls,"hlc","Ledu/cornell/cs/sa/HybridLogicalClock;");
  
  return (*env)->GetObjectField(env, thisObj, fid);
}

static int file_exists(const char * filename){
  struct stat buffer;

  return(stat(filename, &buffer)==0);
}


/* flush a log entry to disk
 * entry layout:
 * 0) int reserved padding
 * 2) int block_length
 * 3) int page_id
 * 4) int nr_pages
 * 1) int64 block_id
 * 5) int64 previous
 * 6) int64 r
 * 7) int64 c
 */
static int do_blog_flush_entry(blog_writer_ctxt_t *bwc)
{
  disk_log_t dle;
  const struct log *pmle = &(bwc->fs->log[bwc->next_entry]);
  int i;

  // 1 - convert log entry to disk format
  dle.block_id = pmle->block_id;
  dle.previous = pmle->previous;
  dle.r = pmle->r;
  dle.c = pmle->c;
  dle.block_length = pmle->block_length;
  dle.pages_offset = pmle->pages_offset;
  dle.pages_length = pmle->pages_length;
  // 2 - write pages and return
  for (i = 0; i < dle.pages_length; i++) {
    if(bwc->fs->page_size != write(bwc->page_fd, pmle->data[i], bwc->fs->page_size)) {
      fprintf(stderr, "Write page failed: logid=%ld.\n", bwc->next_entry);
      print_log(&bwc->fs->log[bwc->next_entry]);
      return -2;
    }
  }
  // 3 - write log to log file
  if (sizeof(dle) != write(bwc->log_fd, &dle,sizeof(dle))) {
      fprintf(stderr, "Write log entry failed: logid=%ld.\n", bwc->next_entry);
      print_log(&bwc->fs->log[bwc->next_entry]);
      return -1;
  }
  bwc->next_entry++;
  return 0;
}

/*
 * do_blog_fulsh()
 * PARAM bwc:  blog_writer_ctxt_t
 */
static int do_blog_flush(blog_writer_ctxt_t *bwc)
{
  //DISCUSS: do we need to lock the log to disable the log reallocation?
  // It seems that read should be OK, because "filesystem->log" is updated
  // immediately. let's just copy the logentry as soon as possible.
  if (bwc->next_entry < bwc->fs->log_length) {
    // We do not flush to the end of the log to avoid
    // keeping it busy when many writes are undergoing...
    long flushTo = bwc->fs->log_length;
    
    while (bwc->next_entry < flushTo)
      if (do_blog_flush_entry(bwc) != 0)
        return -1;
  }
  return 0;
}

/*
 * snapshot file format:
 * [rtc][eid][rtc][eid][rtc][eid]...
 */
static int do_snap_flush(blog_writer_ctxt_t *bwc)
{
  // STEP 1 get snapshot list
  uint64_t len = MAP_LENGTH(log, bwc->fs->log_map);
  uint64_t *ids = MAP_GET_IDS(log, bwc->fs->log_map, len);
  uint64_t *peid;
  uint64_t i;
  
  // STEP 2 write to file
  for (i = 0; i < len; i++) {
    if (MAP_READ(log,bwc->fs->log_map, ids[i], &peid) != 0) {
      fprintf(stderr, "Print Filesystem: can not read eid of snapshot @%ld,\n",ids[i]);
      free(ids);
      return -1;
    };
    write(bwc->snap_fd, &ids[i], sizeof(int64_t));
    write(bwc->snap_fd, peid, sizeof(int64_t));
  }
  free(ids);
  return 0;
}

/*
 * blog_writer_routine()
 * PARAM param: the blog writer context
 * RETURN: the 
 */
static void * blog_writer_routine(void * param)
{
  blog_writer_ctxt_t *bwc = (blog_writer_ctxt_t*) param;
  long time_next_write = 0;
  struct timeval tv;

  while(bwc->alive){
    // STEP 1 - test if a flush is required.
    gettimeofday(&tv,NULL);
    if(time_next_write < tv.tv_sec) {
      usleep(1000000l);
      continue;
    } else {
      time_next_write += bwc->int_sec;
    }

    // STEP 2 - flush
    do_blog_flush(bwc);
  }
  do_blog_flush(bwc);
  do_snap_flush(bwc);
  return param;
}

#define loadBlogReturn(x) \
{ \
  if(plog!=(void*)-1) \
    munmap(plog,log_stat.st_size); \
  if(ppage!=(void*)-1) \
    munmap(ppage,page_stat.st_size); \
  if(psnap!=(void*)-1) \
    munmap(psnap,snap_stat.st_size); \
  return (x); \
}
/*
 *  0 - success
 * -1 - create log_map entry error
 * -2 - create snapshot_map entry error
 */
int do_load_snapshot(filesystem_t *fs, int64_t rtc, int64_t eid)
{
  int64_t *peid = (int64_t *) malloc(sizeof(int64_t));
  snapshot_t * snapshot = NULL;
  
  *peid = eid;
  // log_map
  if(MAP_CREATE(log, fs->log_map, rtc)){
    fprintf(stderr, "Error load snapshot:rtc=%ld\n", rtc);
    return -1;
  }
  if(MAP_WRITE(log, fs->log_map, rtc, peid)){
    fprintf(stderr, "Error load snapshot:rtc=%ld\n", rtc);
    return -1;
  }
  // snapshot_map
  if (MAP_CREATE(snapshot, fs->snapshot_map, eid)) {
    if (MAP_READ(snapshot, fs->snapshot_map, eid, &snapshot)) {
      fprintf(stderr, "Error load snapshot:eid=%ld\n", eid);
      return -2;
    }
    snapshot->ref_count++;
    return 0;
  } else {
    snapshot = (snapshot_t*)malloc(sizeof(snapshot_t));
    snapshot->block_map = MAP_INITIALIZE(block);
    if (snapshot->block_map == NULL) {
      fprintf(stderr, "Create Snapshot for eid=%ld: cannot allocate block map\n",eid);
      return -2;
    }
    snapshot->ref_count = 1;
    if (MAP_WRITE(snapshot, fs->snapshot_map, eid, snapshot)) {
      fprintf(stderr, "Create Snapshot %ld: cannot write to map\n", eid);
      return -2;
    }
    return 0;
  }
}

/*
 * loadBlog()
 * PARAM fs: file system structure
 * PARAM log_fd: log file
 * PARAM page_fd: page file
 * RETURN VALUES: 
 *    0 for succeed.
 *   -1 for "log file is corrupted", 
 *   -2 for "page file is correupted".
 *   -3 for "block operation".
 */
static int loadBlog(filesystem_t *fs, int log_fd, int page_fd, int snap_fd)
{
  struct stat log_stat, page_stat, snap_stat;
  void *plog = (void*)-1, * ppage = (void*)-1, * psnap = (void*)-1;
  disk_log_t *pdle; // disk log entry
  void *pp, *page_mem;
  page_t *pages;
  block_t *block;
  uint64_t *pse; // snapshot entry
  uint64_t log_pos;
  int i;

  // STEP 1 mmap files
  if (fstat(log_fd, &log_stat)) {
    fprintf(stderr, "call fstat on log file descriptor failed");
    loadBlogReturn(-1);
  }
  if (fstat(page_fd, &page_stat)) {
    fprintf(stderr, "call fstat on page file descriptor failed");
    loadBlogReturn(-2);
  }
  if (fstat(snap_fd, &snap_stat)) {
    fprintf(stderr, "call fstat on snapshot file descriptor failed");
    loadBlogReturn(-2);
  }
  if ((plog = mmap(NULL,log_stat.st_size,PROT_READ,MAP_SHARED,log_fd,0)) == (void*) -1) {
    fprintf(stderr, "call mmap on log file descriptor failed");
    loadBlogReturn(-1);
  }
  if ((ppage = mmap(NULL,page_stat.st_size,PROT_READ,MAP_SHARED,page_fd,0)) == (void*) -1) {
    fprintf(stderr, "call mmap on page file descriptor failed");
    loadBlogReturn(-2);
  }
  if ((psnap = mmap(NULL,snap_stat.st_size,PROT_READ,MAP_SHARED,page_fd,0)) == (void*) -1) {
    fprintf(stderr, "call mmap on snap file descriptor failed");
    loadBlogReturn(-2);
  }
  
  // STEP 2 replay log;
  pdle = (disk_log_t*)plog;
  pp = ppage;
  pthread_rwlock_wrlock(&(fs->lock));
  while (((void*) (pdle + 1)) <= (plog + log_stat.st_size)) {//NOTE: this is gcc extension!!!
    /// STEP 2.1 - log entry
    page_mem = NULL;
    pages = NULL;
    // skip createBlock or deleteBlock
    if (pdle->pages_offset != -1 && pdle->pages_offset != -2) {
      // allocate page memory
      page_mem = malloc(fs->page_size*(pdle->pages_length+1));
      pages = (page_t*) page_mem;
      // load page data
      if (pp + fs->page_size*pdle->pages_length > ppage + page_stat.st_size) {
        fprintf(stderr, "not enough pages exists!");
        loadBlogReturn(-2);
      }
      memcpy(page_mem + fs->page_size, pp, fs->page_size*pdle->pages_length);
      for (i = 0; i < pdle->pages_offset; i++)
        pages[i].data = (char*) (page_mem + fs->page_size*(i+1));
      pp += fs->page_size * pdle->pages_length;
    }
    
    check_and_increase_log_length(fs);
    log_pos = fs->log_length;
    fs->log[log_pos].block_id = pdle->block_id;
    fs->log[log_pos].block_length = pdle->block_length;
    fs->log[log_pos].pages_offset = pdle->pages_offset;
    fs->log[log_pos].pages_length = pdle->pages_length;
    fs->log[log_pos].data = pages;
    fs->log[log_pos].previous = pdle->previous;
    fs->log_length += 1;
    //UPDATE Block MAP
    block=NULL;
    switch(pdle->pages_offset) {
    case -1: // CREATE BLOCK
      if(MAP_CREATE(block, fs->block_map, pdle->block_id)==-1){
        fprintf(stderr, "failed in create block during initialization: blockid=%ld.\n", pdle->block_id);
        loadBlogReturn(-3);
      }
      block = (block_t*)malloc(sizeof(block_t));
      block->length = 0;
      block->cap = 0;
      block->pages = NULL;
      block->last_entry = log_pos;
      if(MAP_WRITE(block, fs->block_map, pdle->block_id, block)!=0){
        fprintf(stderr, "failed in create block during initialization: blockid=%ld.\n", pdle->block_id);
        loadBlogReturn(-3);
      }
      break;
    case -2: // DELETE BLOCK
      if(MAP_DELETE(block, fs->block_map, pdle->block_id)==-1){
        fprintf(stderr, "failed in delete block during initialization: blockid=%ld.\n", pdle->block_id);
        loadBlogReturn(-3);
      }
      break;
    default: // WRITE BLOCK
      {
        block_t *block;
        if (MAP_READ(block, fs->block_map, pdle->block_id, &block) != 0) {
          fprintf(stderr, "failed in read from block during initialization: blockid=%ld.\n", pdle->block_id);
          loadBlogReturn(-3);
        }
        block->length = pdle->block_length;
        block->cap = 1;
        while (block->cap < ((pdle->block_length+fs->page_size-1) / fs->page_size))
          block->cap=block->cap<<1;
        block->pages = (page_t **)realloc(block->pages,block->cap*sizeof(page_t*));
        for (i = 0; i < pdle->pages_length; i++)
          block->pages[pdle->pages_offset+i] = ((page_t*)page_mem) + i;
        block->last_entry = log_pos;
      }
      break;
    }
    pdle++;
  }
  pthread_rwlock_unlock(&(fs->lock));

  if ((void*)pdle != plog + log_stat.st_size) {
      fprintf(stderr,"log file is corrupted.\n");
    loadBlogReturn(-1);
  }

  // STEP 3 load snapshots
  pse = (int64_t*)psnap;
  int nsnap = (snap_stat.st_size/sizeof(int64_t)/2);
  while (nsnap--) {
    int64_t rtc = *pse++;
    int64_t eid = *pse++;
    do_load_snapshot(fs,rtc,eid);
  }
  loadBlogReturn(0);
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    initialize
 * Signature: (II)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_initialize
  (JNIEnv *env, jobject thisObj, jint blockSize, jint pageSize, jstring persPath)
{
  const char * pp = (*env)->GetStringUTFChars(env,persPath,NULL); // get the presistent path
  jclass thisCls = (*env)->GetObjectClass(env, thisObj);
  jfieldID long_id = (*env)->GetFieldID(env, thisCls, "jniData", "J");
  jfieldID hlc_id = (*env)->GetFieldID(env, thisCls, "hlc", "Ledu/cornell/cs/sa/HybridLogicalClock;");
  jclass hlc_class = (*env)->FindClass(env, "edu/cornell/cs/sa/HybridLogicalClock");
  jmethodID cid = (*env)->GetMethodID(env, hlc_class, "<init>", "()V");
  jobject hlc_object = (*env)->NewObject(env, hlc_class, cid);
  filesystem_t *filesystem;
  char *fullpath;
  int log_fd, page_fd, snap_fd;
  
  filesystem = (filesystem_t *) malloc (sizeof(filesystem_t));
  if (filesystem == NULL) {
    perror("Error");
    exit(1);
  }
  filesystem->block_size = blockSize;
  filesystem->page_size = pageSize;
  filesystem->block_map = MAP_INITIALIZE(block);
  if (filesystem->block_map == NULL) {
    fprintf(stderr, "Initialize: Allocation of block_map failed.\n");
    (*env)->ReleaseStringUTFChars(env, persPath, pp);
    return -1;
  }
  filesystem->log_map = MAP_INITIALIZE(log);
  if (filesystem->log_map == NULL) {
    fprintf(stderr, "Initialize: Allocation of log_map failed.\n");
    (*env)->ReleaseStringUTFChars(env, persPath, pp);
    return -1;
  }
  filesystem->snapshot_map = MAP_INITIALIZE(snapshot);
  if (filesystem->snapshot_map == NULL) {
    fprintf(stderr, "Initialize: Allocation of snapshot_map failed.\n");
    (*env)->ReleaseStringUTFChars(env, persPath, pp);
    return -1;
  }
  filesystem->log_cap = 1024;
  filesystem->log_length = 0;
  filesystem->log = (log_t *) malloc (1024*sizeof(log_t));
  pthread_rwlock_init(&(filesystem->lock), NULL);
  
  (*env)->SetObjectField(env, thisObj, hlc_id, hlc_object);
  (*env)->SetLongField(env, thisObj, long_id, (int64_t) filesystem);
  
  fullpath = (char*) malloc(strlen(pp)+MAX_FNLEN+1);
  sprintf(fullpath,"%s/%s",pp,BLOGFILE);
  if (file_exist(fullpath)) {
    log_fd = open(fullpath, O_RDONLY);
    sprintf(fullpath, "%s/%s",pp,PAGEFILE);
    page_fd = open(fullpath, O_RDONLY);
    sprintf(fullpath, "%s/%s",pp,SNAPFILE);
    snap_fd = open(fullpath, O_RDONLY);
    if (log_fd == -1 || page_fd == -1 || snap_fd == -1) {
      fprintf(stderr,"Cannot open data node files, exit...\n");
      exit(1);
    }
    if (loadBlog(filesystem, log_fd, page_fd, snap_fd)) {
      fprintf(stderr,"Fail to read data node files, exit...\n");
      exit(1);
    }
  }
  // start write thread.
  filesystem->bwc.fs = filesystem;
  sprintf(fullpath, "%s/%s",pp,BLOGFILE);
  filesystem->bwc.log_fd = open(fullpath, O_APPEND);
  sprintf(fullpath, "%s/%s",pp,PAGEFILE);
  filesystem->bwc.page_fd = open(fullpath, O_APPEND);
  sprintf(fullpath, "%s/%s",pp,SNAPFILE);
  filesystem->bwc.snap_fd = open(fullpath, O_APPEND);
  if(filesystem->bwc.log_fd == -1 || 
     filesystem->bwc.page_fd == -1 || 
     filesystem->bwc.snap_fd == -1){
    fprintf(stderr,"Cannot open data node files, exit...\n");
    exit(1);
  }
  filesystem->bwc.next_entry = filesystem->log_length;
  filesystem->bwc.int_sec = 60; // every minutes
  filesystem->bwc.alive = 1; // alive.
  
  //start blog writer thread
  if (pthread_create(&filesystem->writer_thrd, NULL, blog_writer_routine, (void*)&filesystem->bwc)) {
    fprintf(stderr,"CANNOT create blogWriter thread, exit\n");
    exit(1);
  }

  (*env)->ReleaseStringUTFChars(env, persPath, pp);
  return 0;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    createBlock
 * Signature: (Ledu/cornell/cs/sa/HybridLogicalClock;J)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_createBlock
  (JNIEnv *env, jobject thisObj, jobject mhlc, jlong blockId)
{
  filesystem_t *filesystem;
  block_t *new_block;
  uint64_t log_pos;
  jobject hlc;
  
  // Create a new block.
  filesystem = get_filesystem(env, thisObj);
  MAP_LOCK(block, filesystem->block_map, blockId, 'w');
  if (MAP_CREATE(block, filesystem->block_map, blockId) == -1) {
    MAP_UNLOCK(block, filesystem->block_map, blockId);
    fprintf(stderr, "Create Block: Filesystem already contains block %ld.\n", blockId);
    return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, blockId);
  
  // Create the corresponding log entry.
  pthread_rwlock_wrlock(&(filesystem->lock));
  check_and_increase_log_length(filesystem);
  log_pos = filesystem->log_length;
  filesystem->log[log_pos].block_id = blockId;
  filesystem->log[log_pos].block_length = 0;
  filesystem->log[log_pos].pages_offset = -1;
  filesystem->log[log_pos].pages_length = 0;
  filesystem->log[log_pos].data = NULL;
  filesystem->log[log_pos].previous = -1;
  hlc = get_hybrid_logical_clock(env, thisObj);
  tick_hybrid_logical_clock(env, hlc, mhlc);
  update_log_clock(env, hlc, filesystem->log+log_pos);
  filesystem->log_length += 1;
  pthread_rwlock_unlock(&(filesystem->lock));

  // Create the block, fill the appropriate fields and write it to block map.
  new_block = (block_t *) malloc(sizeof(block_t));
  new_block->length = 0;
  new_block->cap = 0;
  new_block->pages = NULL;
  new_block->last_entry = log_pos;
  MAP_LOCK(block, filesystem->block_map, blockId, 'r');
  if (MAP_WRITE(block, filesystem->block_map, blockId, new_block) != 0) {
    fprintf(stderr, "Create Block: Something is wrong with BLOCK_MAP_CREATE or BLOCK_MAP_WRITE.\n");
    MAP_UNLOCK(block, filesystem->block_map, blockId);
    return -2;
  }
  MAP_UNLOCK(block, filesystem->block_map, blockId);
  return 0;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    deleteBlock
 * Signature: (Ledu/cornell/cs/sa/VectorClock;J)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_deleteBlock
  (JNIEnv *env, jobject thisObj, jobject mhlc, jlong blockId)
{
  filesystem_t *filesystem;
  uint32_t block_pos;
  block_t *cur_block;
  block_t *last_block = NULL;
  block_t *new_block;
  uint64_t log_pos;
  
  // Find the corresponding block. In case you did not find it return an error.
  filesystem = get_filesystem(env, thisObj);
  MAP_LOCK(block, filesystem->block_map, blockId, 'r');
  if (MAP_READ(block, filesystem->block_map, blockId, &cur_block) == -1) {
      fprintf(stderr, "Delete Block: Block with id %ld is not present.\n", blockId);
      MAP_UNLOCK(block, filesystem->block_map, blockId);
      return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, blockId);
  
  // Create the corresponding log entry.
  pthread_rwlock_wrlock(&(filesystem->lock));
  check_and_increase_log_length(filesystem);
  log_pos = filesystem->log_length;
  filesystem->log[log_pos].block_id = blockId;
  filesystem->log[log_pos].block_length = 0;
  filesystem->log[log_pos].pages_offset = -2;
  filesystem->log[log_pos].pages_length = 0;
  filesystem->log[log_pos].data = NULL;
  filesystem->log[log_pos].previous = cur_block->last_entry;
  tick_hybrid_logical_clock(env, get_hybrid_logical_clock(env, thisObj), mhlc);
  update_log_clock(env, mhlc, filesystem->log+log_pos);
  filesystem->log_length += 1;
  pthread_rwlock_unlock(&(filesystem->lock));
  
  // Free the underlying data and delete the block.
  if (cur_block->pages != NULL)
    free(cur_block->pages);
  MAP_LOCK(block, filesystem->block_map, blockId, 'w');
  if (MAP_DELETE(block, filesystem->block_map, blockId) == -1) {
      fprintf(stderr, "Delete Block: Something is wrong with BLOCK_MAP_READ or BLOCK_MAP_DELETE.\n");
      MAP_UNLOCK(block, filesystem->block_map, blockId);
      return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, blockId);
  
  return 0;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readBlock
 * Signature: (JJIII[B)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_readBlock
  (JNIEnv *env, jobject thisObj, jlong blockId, jlong snapshotId, jint blkOfst,
  jint bufOfst, jint length, jbyteArray buf)
{
  filesystem_t *filesystem;
  snapshot_t *snapshot;
  block_t *block;
  uint32_t page_id, page_offset, read_length, cur_length;
  char *page_data;
  uint64_t *log_id;
  int cnt;
  
  //struct timeval tv1,tv2,tv3;
  //gettimeofday(&tv1,NULL);  
  
  // Find the corresponding block.
  filesystem = get_filesystem(env, thisObj);
  if (snapshotId == -1) {
    MAP_LOCK(block, filesystem->block_map, blockId, 'r');
    if (MAP_READ(block, filesystem->block_map, blockId, &block) == -1) {
      MAP_UNLOCK(block, filesystem->block_map, blockId);
      // In case you did not find the block return an error.
      fprintf(stderr, "Read Block: Block with id %ld is not present.\n", blockId);
      return -1;
    }
    MAP_UNLOCK(block, filesystem->block_map, blockId);
  } else {
    MAP_LOCK(log, filesystem->log_map, snapshotId, 'r');
    if (MAP_READ(log, filesystem->log_map, snapshotId, &log_id) == -1) {
      MAP_UNLOCK(log, filesystem->log_map, snapshotId);
      // In case you did not find the snapshot return an error.
      fprintf(stderr, "Read Block: Snapshot with id %ld is not present.\n", snapshotId);
      return -2;
    }
    MAP_LOCK(snapshot, filesystem->snapshot_map, *log_id, 'r');
    MAP_UNLOCK(log, filesystem->log_map, snapshotId);
    if (MAP_READ(snapshot, filesystem->snapshot_map, *log_id, &snapshot) == -1) {
      MAP_UNLOCK(snapshot, filesystem->snapshot_map, *log_id);
      // In case you did not find the snapshot return an error.
      fprintf(stderr, "Read Block: Something is wrong with readBlock.\n");
      return -3;
    }
    MAP_UNLOCK(snapshot, filesystem->snapshot_map, *log_id);
    block = find_or_allocate_snapshot_block(filesystem, snapshot, *log_id, blockId);
    // If block did not exist at this point.
    if (block->cap == -1) {
      fprintf(stderr, "Read Block: Block with id %ld is not present at snapshot with rtc %ld.\n", blockId, snapshotId);
      return -1;
    }
  }
  //gettimeofday(&tv2,NULL);
  cnt=1;
  // In case the data you ask is not written return an error.
  if (blkOfst >= block->length) {
    fprintf(stderr, "Read Block: Block %ld is not written at %d byte.\n", blockId, blkOfst);
    return -3;
  }
  
  // See if the data is partially written.
  if (blkOfst + length <= block->length)
    read_length = length;
  else
    read_length = block->length - blkOfst;
  
  // Fill the buffer.
  page_id = blkOfst / filesystem->page_size;
  page_offset = blkOfst % filesystem->page_size;
  page_data = block->pages[page_id]->data;
  page_data += page_offset;
  cur_length = filesystem->page_size - page_offset;
  if (cur_length >= read_length) {
    (*env)->SetByteArrayRegion(env, buf, bufOfst, read_length, (jbyte*) page_data);
    // return read_length;
  } else {
    (*env)->SetByteArrayRegion(env, buf, bufOfst, cur_length, (jbyte*) page_data);
    page_id++;
    while (1) {
      //struct timeval t1,t2;
      //gettimeofday(&t1,NULL);
      cnt++;
      page_data = block->pages[page_id]->data;
      if (cur_length + filesystem->page_size >= read_length) {
        (*env)->SetByteArrayRegion(env, buf, bufOfst + cur_length, read_length - cur_length, (jbyte*) page_data);
        break;//return read_length;
      }
      (*env)->SetByteArrayRegion(env, buf, bufOfst + cur_length, filesystem->page_size, (jbyte*) page_data);
      //fprintf(stdout, "set: off=%d,len=%ld\n",bufOfst + cur_length, filesystem->page_size);
      cur_length += filesystem->page_size;
      page_id++;
      //gettimeofday(&t2,NULL);
      // fprintf(stdout, "%ldus\n", (t2.tv_sec-t1.tv_sec)*1000000+t2.tv_usec-t1.tv_usec);
    }
  }
  //gettimeofday(&tv3,NULL);
  //  long search=(tv2.tv_sec-tv1.tv_sec)*1000000+tv2.tv_usec-tv1.tv_usec;
  //  long copy=(tv3.tv_sec-tv2.tv_sec)*1000000+tv3.tv_usec-tv2.tv_usec;
  //  fprintf(stdout, "readBlock %ld %ld [%d,%d,%d]\n",search,copy,blkOfst,read_length,cnt);
  return read_length;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    getNumberOfBytes
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_getNumberOfBytes
  (JNIEnv *env, jobject thisObj, jlong blockId, jlong snapshotId)
{
  filesystem_t *filesystem;
  snapshot_t *snapshot;
  block_t *block;
  uint64_t *log_id;
  
  // Find the corresponding block.
  filesystem = get_filesystem(env, thisObj);
  if (snapshotId == -1) {
    MAP_LOCK(block, filesystem->block_map, blockId, 'r');
    if (MAP_READ(block, filesystem->block_map, blockId, &block) == -1) {
      MAP_UNLOCK(block, filesystem->block_map, blockId);
      // In case you did not find the block return an error.
      fprintf(stderr, "Get Number of Bytes: Block with id %ld is not present.\n", blockId);
      return -1;
    }
    MAP_UNLOCK(block, filesystem->block_map, blockId);
  } else {
    MAP_LOCK(log, filesystem->log_map, snapshotId, 'r');
    if (MAP_READ(log, filesystem->log_map, snapshotId, &log_id) == -1) {
      MAP_UNLOCK(log, filesystem->log_map, snapshotId);
      // In case you did not find the snapshot return an error.
      fprintf(stderr, "Get Number of Bytes: Snapshot with id %ld is not present.\n", snapshotId);
      return -2;
    }
    MAP_LOCK(snapshot, filesystem->snapshot_map, *log_id, 'r');
    MAP_UNLOCK(log, filesystem->log_map, snapshotId);
    if (MAP_READ(snapshot, filesystem->snapshot_map, *log_id, &snapshot) == -1) {
      MAP_UNLOCK(snapshot, filesystem->snapshot_map, *log_id);
      // In case you did not find the snapshot return an error.
      fprintf(stderr, "Get Number of Bytes: Something is wrong with getNumberOfBytes.\n");
      return -3;
    }
    MAP_UNLOCK(snapshot, filesystem->snapshot_map, *log_id);
    block = find_or_allocate_snapshot_block(filesystem, snapshot, *log_id, blockId);
    // If block did not exist at this point.
    if (block->cap == -1) {
      fprintf(stderr, "Get Number of Bytes: Block with id %ld is not present at snapshot with rtc %ld.\n", blockId,
              snapshotId);
      return -1;
    }
  }
  
  return (jint) block->length;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    writeBlock
 * Signature: (Ledu/cornell/cs/sa/VectorClock;JIII[B)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_writeBlock
  (JNIEnv *env, jobject thisObj, jobject mhlc, jlong blockId, jint blkOfst,
  jint bufOfst, jint length, jbyteArray buf)
{
#ifdef PERF_WRITE
  struct timeval tv_base,tv1,tv2;
  long t_tot=0,t_tick=0,t_copy=0;
  gettimeofday(&tv_base,NULL);
#endif//PERF_WRITE
  filesystem_t *filesystem;
  block_t *block;
  page_t *new_pages;
  int first_page, last_page;
  int page_offset, block_offset, buffer_offset;
  int new_pages_length, new_pages_capacity, write_length, last_page_length;
  int64_t log_pos;
  char *pdata;
  int i;
  char *allData=NULL;
  int adp=0; // next usagable page in allData.
#define ADP	(adp*filesystem->page_size)
#define IADP	(adp++)
#define MALLOCPAGE (void*)(allData+(filesystem->page_size*adp++))
  
  // Find the corresponding block.
  filesystem = get_filesystem(env, thisObj);
  MAP_LOCK(block, filesystem->block_map, blockId, 'r');
  if (MAP_READ(block, filesystem->block_map, blockId, &block) != 0) {
    // In case you did not find it return an error.
    fprintf(stderr, "Write Block: Block with id %ld is not present.\n", blockId);
    MAP_UNLOCK(block, filesystem->block_map, blockId);
    return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, blockId);

  // In case you cannot write in the required offset.
  if (blkOfst > block->length) {
    fprintf(stderr, "Write Block: Block %ld cannot be written at byte %d.\n", blockId, blkOfst);
    return -3;
  }

#ifdef FIRST_EXPERIMENT
  // First experiment.
  if (blkOfst + length > block->length)
      block->length = blkOfst + length;
#else
  // Create the new pages.
  block_offset = (int) blkOfst;
  buffer_offset = (int) bufOfst;
  
  first_page = block_offset / filesystem->page_size;
  last_page = (block_offset + length - 1) / filesystem->page_size;
  page_offset = block_offset % filesystem->page_size;
  new_pages_length = last_page - first_page + 1;
  allData = (char*) malloc(filesystem->page_size*sizeof(char)*(new_pages_length+1));
  //new_pages = (page_t*) malloc(new_pages_length*sizeof(page_t));
  new_pages = (page_t*)MALLOCPAGE;
  
  //new_pages[0].data = (char *) malloc(filesystem->page_size*sizeof(char));
  new_pages[0].data = (char*)MALLOCPAGE;
  for (i = 0; i < page_offset; i++)
    new_pages[0].data[i] = block->pages[first_page]->data[i];
  if (first_page == last_page) {
    write_length = (jint) length;
    pdata = new_pages[0].data + page_offset;
#ifdef SECOND_EXPERIMENT
#else
#ifdef PERF_WRITE
    gettimeofday(&tv1,NULL);
#endif//PERF_WRITE
    (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_length, (jbyte *) pdata);
#ifdef PERF_WRITE
    gettimeofday(&tv2,NULL);
    t_copy+=(tv2.tv_usec-tv1.tv_usec+(1<<20))%(1<<20);
#endif//PERF_WRITE
#endif
    last_page_length = page_offset + write_length;
  } else {
    write_length = filesystem->page_size - page_offset;
    pdata = new_pages[0].data + page_offset;
#ifdef SECOND_EXPERIMENT
#else
#ifdef PERF_WRITE
    gettimeofday(&tv1,NULL);
#endif//PERF_WRITE
    (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_length, (jbyte *) pdata);
#ifdef PERF_WRITE
    gettimeofday(&tv2,NULL);
    t_copy+=(tv2.tv_usec-tv1.tv_usec+(1<<20))%(1<<20);
#endif//PERF_WRITE
#endif
    buffer_offset += write_length;
    for (i = 1; i < new_pages_length-1; i++) {
      //new_pages[i].data = (char *) malloc(filesystem->page_size*sizeof(char));
      new_pages[i].data = (char *)MALLOCPAGE;
      write_length = filesystem->page_size;
      pdata = new_pages[i].data;
#ifdef SECOND_EXPERIMENT
#else
#ifdef PERF_WRITE
    gettimeofday(&tv1,NULL);
#endif//PERF_WRITE
      (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_length, (jbyte *) pdata);
#ifdef PERF_WRITE
    gettimeofday(&tv2,NULL);
    t_copy+=(tv2.tv_usec-tv1.tv_usec+(1<<20))%(1<<20);
#endif//PERF_WRITE
#endif
      buffer_offset += write_length;
    }
    //new_pages[new_pages_length-1].data =  (char *) malloc(filesystem->page_size*sizeof(char));
    new_pages[new_pages_length-1].data =  (char *)MALLOCPAGE;
    write_length = (int) bufOfst + (int) length - buffer_offset;
    pdata = new_pages[new_pages_length-1].data;
#ifdef SECOND_EXPERIMENT
#else
#ifdef PERF_WRITE
    gettimeofday(&tv1,NULL);
#endif//PERF_WRITE
    (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_length, (jbyte *) pdata);
#ifdef PERF_WRITE
    gettimeofday(&tv2,NULL);
    t_copy+=(tv2.tv_usec-tv1.tv_usec+(1<<20))%(1<<20);
#endif//PERF_WRITE
#endif
    last_page_length = write_length;
  }
  if (last_page*filesystem->page_size + last_page_length > block->length) {
    block->length = last_page*filesystem->page_size + last_page_length;
  } else if (last_page*filesystem->page_size > block->length) {
    for (i = last_page_length; i < block->length - (last_page-1)*filesystem->page_size; i++)
      new_pages[new_pages_length-1].data[i] = block->pages[last_page]->data[i];
  } else {
    for (i = last_page_length; i < filesystem->page_size; i++)
      new_pages[new_pages_length-1].data[i] = block->pages[last_page]->data[i];
  }
  
  // Fill block with the appropriate information.
  if (block->cap == 0)
    new_pages_capacity = 1;
  else
    new_pages_capacity = block->cap;
  while ((block->length - 1) / filesystem->page_size >= new_pages_capacity)
    new_pages_capacity *= 2;
  if (new_pages_capacity > block->cap) {
    block->cap = new_pages_capacity;
    block->pages = (page_t**) realloc(block->pages, block->cap*sizeof(page_t*));
  }
  for (i = 0; i < new_pages_length; i++)
    block->pages[first_page+i] = new_pages + i;


  // Create log entry.
  pthread_rwlock_wrlock(&(filesystem->lock));
  check_and_increase_log_length(filesystem);
  log_pos = filesystem->log_length;
  filesystem->log[log_pos].block_id = blockId;
  filesystem->log[log_pos].block_length = block->length;
  filesystem->log[log_pos].pages_offset = first_page;
  filesystem->log[log_pos].pages_length = new_pages_length;
  filesystem->log[log_pos].data = new_pages;
  filesystem->log[log_pos].previous = block->last_entry;
#ifdef SECOND_EXPERIMENT
#else
#ifdef THIRD_EXPERIMENT
#else
#ifdef PERF_WRITE
  gettimeofday(&tv1,NULL);
#endif
//  tick_vector_clock(env, thisObj, mvc, filesystem->log+log_pos);
  tick_hybrid_logical_clock(env, get_hybrid_logical_clock(env, thisObj), mhlc);
  update_log_clock(env, mhlc, filesystem->log+log_pos);
#ifdef PERF_WRITE
  gettimeofday(&tv2,NULL);
  t_tick=(tv2.tv_usec-tv1.tv_usec+(1<<20))%(1<<20);
#endif//PERF_WRITE
#endif
#endif
  filesystem->log_length += 1;
  pthread_rwlock_unlock(&(filesystem->lock));
  block->last_entry = log_pos;
#endif
#ifdef PERF_WRITE
  gettimeofday(&tv2,NULL);
  t_tot+=(tv2.tv_usec-tv_base.tv_usec+(1<<20))%(1<<20);
  printf("%ld %ld %ld %d\n",t_tot-(t_copy+t_tick),t_copy,t_tick,length);
  // fflush(stdout);
#endif//PERF_WRITE
  return 0;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    createSnapshot
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_createSnapshot
  (JNIEnv *env, jobject thisObj, jlong rtc)
{
  filesystem_t *filesystem;
  snapshot_t *snapshot;
  snapshot_t *last_snapshot = NULL;
  log_t *log;
  int64_t log_pos, cur_value, length;
  int64_t *new_id;
  
  // Find the corresponding log.
  filesystem = get_filesystem(env, thisObj);
  log = filesystem->log;
  pthread_rwlock_rdlock(&(filesystem->lock));
  length = filesystem->log_length;
  pthread_rwlock_unlock(&(filesystem->lock));
  // If the real time cut is before the first log...
  if ((length == 0) || (log[0].r > rtc)) {
    fprintf(stderr, "Create Snapshot: Log has not entries before RTC %ld\n", rtc);
    return -1;
  }
  // If the real time cut is after the last log, create a mock event with rtc timestamp. Otherwise do binary search.
  log_pos = length-1;
  if (log[log_pos].r < rtc) {
    mock_tick_hybrid_logical_clock(env, get_hybrid_logical_clock(env, thisObj), rtc);
  } else {
    log_pos = length / 2;
    cur_value = log_pos / 2;
    while ((log[log_pos].r > rtc) || ((log_pos+1 < length) && (log[log_pos+1].r <= rtc))) {
      if (log[log_pos].r > rtc)
        log_pos -= cur_value;
      else
        log_pos += cur_value;
      if (cur_value > 1)
        cur_value /= 2;
    }
  }
  
  // Find the corresponding position to write snapshot.
  MAP_LOCK(log, filesystem->log_map, rtc, 'w');
  if (MAP_CREATE(log, filesystem->log_map, rtc) != 0) {
    MAP_UNLOCK(log, filesystem->log_map, rtc);
    fprintf(stderr, "Create Snapshot: Snapshot %ld already exists\n", rtc);
    return -1;
  }
  new_id = (int64_t *) malloc(sizeof(int64_t));
  *new_id = log_pos;
  if (MAP_WRITE(log, filesystem->log_map, rtc, new_id)) {
    MAP_UNLOCK(log, filesystem->log_map, rtc);
    fprintf(stderr, "Create Snapshot: Something is wrong with LOG_MAP_CREATE or LOG_MAP_WRITE\n");
    return -2;
  }
  MAP_LOCK(snapshot, filesystem->snapshot_map, log_pos, 'w');
  MAP_UNLOCK(log, filesystem->log_map, rtc);
  
  if (MAP_CREATE(snapshot, filesystem->snapshot_map, log_pos) != 0) {
    if (MAP_READ(snapshot, filesystem->snapshot_map, log_pos, &snapshot) != 0) {
      MAP_UNLOCK(snapshot, filesystem->snapshot_map, log_pos);
      fprintf(stderr, "Create Snapshot: Something is wrong with SNAPSHOT_MAP_CREATE or SNAPSHOT_MAP_READ\n");
    }
    snapshot->ref_count++;
    MAP_UNLOCK(snapshot, filesystem->snapshot_map, log_pos);
    return 0;
  }
    
  snapshot = (snapshot_t*) malloc(sizeof(snapshot_t));
  snapshot->block_map = MAP_INITIALIZE(block);
  if (snapshot->block_map == NULL) {
    fprintf(stderr, "Create Snapshot: Allocation of snapshot block map failed.\n");
    return -1;
  }
  snapshot->ref_count = 1;
  if (MAP_WRITE(snapshot, filesystem->snapshot_map, log_pos, snapshot)) {
    MAP_UNLOCK(snapshot, filesystem->snapshot_map, log_pos);
    fprintf(stderr, "Create Snapshot: Something is wrong with SNAPSHOT_MAP_CREATE or SNAPSHOT_MAP_WRITE\n");
    return -2;
  }
  MAP_UNLOCK(snapshot, filesystem->snapshot_map, log_pos);
  return 0;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readLocalRTC
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_edu_cornell_cs_blog_JNIBlog_readLocalRTC
  (JNIEnv *env, jclass thisCls)
{
  return read_local_rtc();
}

JNIEXPORT void Java_edu_cornell_cs_blog_JNIBlog_destroy
  (JNIEnv *env, jobject thisObj)
{
  //TODO: release all memory data? currently we leave it for OS.
  // kill blog writer
  filesystem_t *fs;
  void * ret;
  
  fs = get_filesystem(env,thisObj);
  fs->bwc.alive = 0;
  if(pthread_join(fs->writer_thrd, &ret))
    fprintf(stderr,"waiting for blogWriter thread error...disk data may be corrupted\n");
}
