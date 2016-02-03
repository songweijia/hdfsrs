#include <inttypes.h>
#include <string.h>
#include <sys/time.h>
#include <limits.h>
#include "JNIBlog.h"
#include "types.h"
#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>

// Define files for storing persistent data.
#define BLOGFILE "blog._dat"
#define PAGEFILE "page._dat"
#define SNAPFILE "snap._dat"
#define MAX_FNLEN (strlen(BLOGFILE)+strlen(PAGEFILE)+strlen(SNAPFILE))

static void print_page(page_t *page)

// Type definitions for dictionaries.
MAP_DEFINE(block, block_t, BLOCK_MAP_SIZE);
MAP_DEFINE(snapshot_block, snapshot_block_t, SNAPSHOT_BLOCK_MAP_SIZE);
MAP_DEFINE(log, uint64_t, LOG_MAP_SIZE);
MAP_DEFINE(snapshot, snapshot_t, SNAPSHOT_MAP_SIZE);

// Printer Functions.
char *log_to_string(log_t *log, size_t page_size)
{
  char *res = (char *) malloc(1024 * 1024 * sizeof(char));
  char buf[1024];
  uint32_t length, i;
  
  switch (log->op) {
  case BOL:
  	strcpy(res, "Operation: Beginning Of Log\n");
    sprintf(buf, "HLC Value: (%" PRIu64 ",%" PRIu64 ")\n", log->r, log->l);
    strcat(res, buf);
    break;
  case CREATE_BLOCK:
    strcpy(res, "Operation: Create Block\n");
    sprintf(buf, "Block ID: %" PRIu64 "\n", log->block_id);
    strcat(res, buf);
    sprintf(buf, "HLC Value: (%" PRIu64 ",%" PRIu64 ")\n", log->r, log->l);
    strcat(res, buf);
    break;
  case DELETE_BLOCK:
    strcpy(res, "Operation: Delete Block\n");
    sprintf(buf, "Block ID: %" PRIu64 "\n", log->block_id);
    strcat(res, buf);
    sprintf(buf, "HLC Value: (%" PRIu64 ",%" PRIu64 ")\n", log->r, log->l);
    strcat(res, buf);
    sprintf(buf, "Previous: %" PRIu64 "\n", log->previous);
    strcat(res, buf);
    break;
  case WRITE:
    strcpy(res, "Operation: Write\n");
    sprintf(buf, "Block ID: %" PRIu64 "\n", log->block_id);
    strcat(res, buf);
    sprintf(buf, "Block Length: %" PRIu32 "\n", (uint32_t) log->block_length);
    strcat(res, buf);
    sprintf(buf, "Starting Page: %" PRIu32 "\n", log->page_id);
    strcat(res, buf);
    sprintf(buf, "Number of Pages: %" PRIu32 "\n", log->nr_pages);
    strcat(res, buf);
    for (i = 0; i < log->nr_pages-1; i++) {
      sprintf(buf, "Page %" PRIu32 ":\n", log->page_id + i);
      strcat(res, buf);
      snprintf(buf, page_size+1, "%s", log->pages + i*page_size);
      strcat(res, buf);
      strcat(res, "\n");
    }
    if (log->block_length > log->nr_pages*page_size)
      length = page_size;
    else
      length = log->block_length - (log->nr_pages-1)*page_size;
    sprintf(buf, "Page %" PRIu32 ":\n", log->page_id + log->nr_pages-1);
    strcat(res, buf);
    snprintf(buf, length+1, "%s", log->pages + (log->nr_pages-1)*page_size);
    strcat(res, buf);
    strcat(res, "\n");
    sprintf(buf, "HLC Value: (%" PRIu64 ",%" PRIu64 ")\n", log->r, log->l);
    strcat(res, buf);
    sprintf(buf, "Previous: %" PRIu64 "\n", log->previous);
    strcat(res, buf);
    break;
  default:
    strcat(res, "Operation: Unknown Operation\n");
  }
  return res;
}

char *block_to_string(block_t *block, size_t page_size)
{
  char *res = (char *) malloc((4096 + block->length) * sizeof(char));
  char buf[1024];
  uint32_t i;

  sprintf(buf, "Log Index: %" PRIu64 "\n", block->log_index);
	strcpy(res, buf);
  sprintf(buf, "Length: %" PRIu32 "\n", (uint32_t) block->length);
  strcat(res, buf);
  sprintf(buf, "Pages Capacity: %" PRIu32 "\n", block->pages_cap);
  strcat(res, buf);
  if (block->length != 0) {
    for (i = 0; i <= (block->length - 1) / page_size; i++) {
      sprintf(buf, "Page %" PRIu32 ":\n", i);
      strcat(res, buf);
      snprintf(buf, page_size+2, "%s\n", block->pages[i]);
      strcat(res, buf);
    }
  }
  return res;
}

char *snapshot_block_to_string(snapshot_block_t *block, size_t page_size)
{
  char *res = (char *) malloc((4096 + block->length) * sizeof(char));
  char buf[1024];
  uint32_t i;

	if (block->status == 0)
		strcpy(res, "Status: Active\n");
	else
		strcpy(res, "Status: Non-Active\n");
  sprintf(buf, "Length: %" PRIu32 "\n", (uint32_t) block->length);
  strcat(res, buf);
  sprintf(buf, "Pages Capacity: %" PRIu32 "\n", block->pages_cap);
  strcat(res, buf);
  if (block->length != 0) {
    for (i = 0; i <= (block->length - 1) / page_size; i++) {
      sprintf(buf, "Page %" PRIu32 ":\n", i);
      strcat(res, buf);
      snprintf(buf, page_size+2, "%s\n", block->pages[i]);
      strcat(res, buf);
    }
  }
  return res;
}

char *snapshot_to_string(snapshot_t *snapshot, size_t page_size)
{
  char *res = (char *) malloc((1024*page_size) * sizeof(char));
  char buf[1024];
  snapshot_block_t *block;
  uint64_t *block_ids;
  uint64_t length, i;
  
  strcpy(res, "Blocks:\n");
  for (i = 0; i < BLOCK_MAP_SIZE; i++)
    MAP_LOCK(snapshot_block, snapshot->block_map, i, 'r');
  length = MAP_LENGTH(snapshot_block, snapshot->block_map);
  block_ids = MAP_GET_IDS(snapshot_block, snapshot->block_map, length);
  for (i = 0; i < length; i++) {
    if (MAP_READ(snapshot_block, snapshot->block_map, block_ids[i], &block) != 0) {
      fprintf(stderr, "ERROR: Cannot read block whose block ID is contained in block map.\n");
      exit(0);
    }
    sprintf(buf, "Block ID: %" PRIu64 "\n", block_ids[i]);
    strcat(res, buf);
    strcat(res, snapshot_block_to_string(block, page_size));
  }
  sprintf(buf, "Reference Count: %" PRIu64 "\n", snapshot->ref_count);
  strcat(res, buf);
  for (i = 0; i < BLOCK_MAP_SIZE; i++)
    MAP_UNLOCK(snapshot_block, snapshot->block_map, i);
  free(block_ids);
  return res;
}

char *filesystem_to_string(filesystem_t *filesystem)
{
  char *res = (char *) malloc((1024*filesystem->page_size) * sizeof(char));
  char buf[1024];
  snapshot_t *snapshot;
  uint64_t length, i, log_index;
  uint64_t *snapshot_ids;
  uint64_t *log_ptr;
  
  // Print Filesystem.
  strcpy(res, "Filesystem\n");
  strcat(res, "----------\n");
  sprintf(buf, "Block Size: %" PRIu64 "\n", (uint64_t) filesystem->block_size);
  strcat(res, buf);
  sprintf(buf, "Page Size: %" PRIu64 "\n", (uint64_t) filesystem->page_size);
  strcat(res, buf);
  
  // Print Log.
  pthread_rwlock_rdlock(&filesystem->log_lock);
  sprintf(buf, "Log Capacity: %" PRIu64 "\n", filesystem->log_cap);
  strcat(res, buf);
  sprintf(buf, "Log Length: %" PRIu64 "\n", filesystem->log_length);
  strcat(res, buf);
  for (i = 0; i < filesystem->log_length; i++) {
    sprintf(buf, "Log %" PRIu64 ":\n", i);
  	strcat(res, buf);
    strcat(res, log_to_string(filesystem->log + i, filesystem->page_size));
    strcat(res, "\n");
  }
  pthread_rwlock_unlock(&filesystem->log_lock);
  
  // Print Snapshots.
  for (i = 0; i < LOG_MAP_SIZE; i++)
    MAP_LOCK(log, filesystem->log_map, i, 'r');
  length = MAP_LENGTH(log, filesystem->log_map);
  sprintf(buf, "Length: %" PRIu64 "\n", length);
  strcat(res, buf);
  if (length > 0) {
    snapshot_ids = MAP_GET_IDS(log, filesystem->log_map, length);
    strcat(res, "Snapshots:\n");
  }
  for (i = 0; i < length; i++) {
    sprintf(buf, "RTC: %" PRIu64 "\n", snapshot_ids[i]);
    strcat(res, buf);
    if (MAP_READ(log, filesystem->log_map, snapshot_ids[i], &log_ptr) != 0) {
      fprintf(stderr, "ERROR: Cannot read log index whose time is contained in log map.\n");
      exit(0);
    }
    log_index = *log_ptr;
  	sprintf(buf, "Last Log Index: %" PRIu64 "\n", log_index);
  	strcat(res, buf);
    MAP_LOCK(snapshot, filesystem->snapshot_map, log_index, 'r');
    if (MAP_READ(snapshot, filesystem->snapshot_map, log_index, &snapshot) != 0) {
      fprintf(stderr, "ERROR: Cannot read snapshot whose last log index is contained in snapshot map.\n");
      exit(0);
    }
    strcat(res, snapshot_to_string(snapshot, filesystem->page_size));
    strcat(res, "\n");
    MAP_UNLOCK(snapshot, filesystem->snapshot_map, *log_ptr);
  }
  for (i = 0; i < LOG_MAP_SIZE; i++)
    MAP_UNLOCK(log, filesystem->log_map, i);
  if (length > 0)
    free(snapshot_ids);
  return res;
}

// Helper functions for clock updates - See JAVA counterpart for more details.
void update_log_clock(JNIEnv *env, jobject hlc, log_t *log)
{
  jclass hlcClass = (*env)->GetObjectClass(env, hlc);
  jfieldID rfield = (*env)->GetFieldID(env, hlcClass, "r", "J");
  jfieldID cfield = (*env)->GetFieldID(env, hlcClass, "c", "J");

  log->r = (*env)->GetLongField(env, hlc, rfield);
  log->l = (*env)->GetLongField(env, hlc, cfield);
}

static void tick_hybrid_logical_clock(JNIEnv *env, jobject hlc, jobject mhlc)
{
  jclass hlcClass = (*env)->GetObjectClass(env, hlc);
  jmethodID mid = (*env)->GetMethodID(env, hlcClass, "tickOnRecv", "(Ledu/cornell/cs/sa/HybridLogicalClock;)V");

  (*env)->CallObjectMethod(env, hlc, mid, mhlc);
}

jobject get_hybrid_logical_clock(JNIEnv *env, jobject thisObj)
{
  jclass thisCls = (*env)->GetObjectClass(env,thisObj);
  jfieldID fid = (*env)->GetFieldID(env,thisCls,"hlc","Ledu/cornell/cs/sa/HybridLogicalClock;");
  
  return (*env)->GetObjectField(env, thisObj, fid);
}

// Helper function for obtaining the local filesystem object - See JAVA counterpart for more details.
filesystem_t *get_filesystem(JNIEnv *env, jobject thisObj)
{
  jclass thisCls = (*env)->GetObjectClass(env,thisObj);
  jfieldID fid = (*env)->GetFieldID(env,thisCls,"jniData","J");
  
  return (filesystem_t *) (*env)->GetLongField(env, thisObj, fid);
}


/**
 * Read local RTC value.
 * return clock value.
*/
uint64_t read_local_rtc()
{
  struct timeval tv;
  uint64_t rtc;
  
  gettimeofday(&tv,NULL);
  rtc = tv.tv_sec*1000 + tv.tv_usec/1000;
  return rtc;
}

int compare(uint64_t r1, uint64_t c1, uint64_t r2, uint64_t c2)
{
  if (r1 > r2)
    return 1;
  if (r2 > r1)
    return -1;
  if (c1 > c2)
    return 1;
  if (c2 > c1)
    return -1;
  return 0;
}

// Find last log entry that has timestamp less or equal than (r,l).
int find_last_entry(filesystem_t *filesystem, uint64_t r, uint64_t l, uint64_t *last_entry)
{
  log_t *log = filesystem->log;
  uint64_t length, log_index, cur_diff;
  
  pthread_rwlock_rdlock(&filesystem->log_lock);
  length = filesystem->log_length;
  pthread_rwlock_unlock(&filesystem->log_lock);
  
  if (compare(r,l,log[length-1].r,log[length-1].l) > 0) {
    if (read_local_rtc() < r)
      return -1;
    log_index = length -1;
  } else if (compare(r,l,log[length-1].r,log[length-1].l) == 0) {
    log_index = length - 1;
 	} else {
    log_index = length/2 > 0 ? length / 2 - 1 : 0;
    cur_diff = length/4 > 1? length/4 : 1;
    while ((compare(r,l,log[log_index].r,log[log_index].l) == -1) ||
           (compare(r,l,log[log_index+1].r,log[log_index+1].l) >= 0)) {
      if (compare(r,l,log[log_index].r,log[log_index].l) == -1)
        log_index -= cur_diff;
      else
        log_index += cur_diff;
      cur_diff = cur_diff > 1 ? cur_diff/2 : 1;
    }
  }
  *last_entry = log_index;
  return 0;
}

/**
 * Check if the log capacity needs to be increased.
 * filesystem - Local filesystem object.
 * return  0, if successful,
 *        -1, otherwise.
 */
int check_and_increase_log_length(filesystem_t *filesystem)
{
  if (filesystem->log_length == filesystem->log_cap) {
    filesystem->log = (log_t *) realloc(filesystem->log, 2*filesystem->log_cap*sizeof(log_t));
    if (filesystem->log == NULL)
    	return -1;
    filesystem->log_cap *= 2;
  }
  return 0;
}

/**
 * Find a snapshot object with specific time. If not found, instatiate one.
 * filesystem   - Local filesystem object.
 * time         - Time to take the snapshot.
 *                Must be lower or equal than a timestamp that might appear in the future.
 * snapshot_ptr - Snapshot pointer used for returning the correct snapshot instance.
 * return  1, if created snapshot,
 *         0, if found snapshot,
 *         error code, otherwise. 
 */
int find_or_create_snapshot(filesystem_t *filesystem, uint64_t t, snapshot_t **snapshot_ptr)
{
	snapshot_t *snapshot;
  uint64_t *log_ptr;
  uint64_t log_index;
  
  // If snapshot already exists return the existing snapshot.
  MAP_LOCK(log, filesystem->log_map, t, 'w');
  if (MAP_READ(log, filesystem->log_map, t, &log_ptr) == 0) {
    log_index = *log_ptr;
    MAP_LOCK(snapshot, filesystem->snapshot_map, log_index, 'r');
    if (MAP_READ(snapshot, filesystem->snapshot_map, log_index, snapshot_ptr) == -1) {
      fprintf(stderr, "ERROR: Could not find snapshot in snapshot map although ID exists in log map.\n");
      exit(0);
    }
    MAP_UNLOCK(snapshot, filesystem->snapshot_map, log_index);
    MAP_UNLOCK(log, filesystem->log_map, t);
    return 0;
  }
 	
 	// If snapshot can include future references do not create it.
  log_ptr = (uint64_t *) malloc(sizeof(uint64_t));
  if (find_last_entry(filesystem, t-1, ULLONG_MAX, log_ptr) == -1) {
    free(log_ptr);
    fprintf(stderr, "WARNING: Snapshot was not created because it might have future entries.\n");
  	return -1;
  }
  log_index = *log_ptr;
  
  // Create snapshot.
  if (MAP_CREATE_AND_WRITE(log, filesystem->log_map, t, log_ptr) == -1) {
      fprintf(stderr, "ERROR: Could not create snapshot although it does not exist in the log map.\n");
      exit(0);
  }
  MAP_LOCK(snapshot, filesystem->snapshot_map, log_index, 'w');
  if (MAP_READ(snapshot, filesystem->snapshot_map, log_index, snapshot_ptr) == 0) {
  	snapshot = *snapshot_ptr;
    snapshot->ref_count++;
  } else {
    snapshot = (snapshot_t*) malloc(sizeof(snapshot_t));
    snapshot->block_map = MAP_INITIALIZE(snapshot_block);
    if (snapshot->block_map == NULL) {
      fprintf(stderr, "ERROR: Allocation of block map failed.\n");
      exit(0);
    }
    snapshot->ref_count = 1;
    if (MAP_CREATE_AND_WRITE(snapshot, filesystem->snapshot_map, log_index, snapshot) == -1) {
      fprintf(stderr, "ERROR: Could not create snapshot although it does not exist in the snapshot map.\n");
      exit(0);
    }
    (*snapshot_ptr) = snapshot;
  }
  MAP_UNLOCK(snapshot, filesystem->snapshot_map, log_index);
  MAP_UNLOCK(log, filesystem->log_map, t);
  return 1;
}

/**
 * Find a snapshot block object. If not found, instatiate one.
 * filesystem     - Local filesystem object.
 * snapshot       - Snapshot object.
 * last_log_index - Last log entry for this snapshot.
 * block_id				- ID of the requested block.
 * block_ptr 		  - Block pointer used for returning the correct block instance.
 * return  1, if created block,
 *         0, if found block.
 */
int find_or_create_snapshot_block(filesystem_t *filesystem, snapshot_t *snapshot, uint64_t last_log_index,
                                  uint64_t block_id, snapshot_block_t **block_ptr)
{
  log_t *log = filesystem->log;
  log_t *log_ptr;
  snapshot_block_t *block;
  uint64_t log_index;
  uint32_t i, nr_unfilled_pages;
  
  // If you find the snapshot block return it.
  MAP_LOCK(snapshot_block, snapshot->block_map, block_id, 'w');
  if (MAP_READ(snapshot_block, snapshot->block_map, block_id, block_ptr) == 0) {
    MAP_UNLOCK(snapshot_block, snapshot->block_map, block_id);
    return 0;
  }
  
  // Otherwise, traverse the log until you find the last log entry associated with the requested block for this 
  // snapshot.
  log_index = last_log_index;
  while ((log_index > 0) && (log[log_index].block_id != block_id))
    log_index--;
  
  // Instatiate the snapshot block
  block = (snapshot_block_t *) malloc(sizeof(snapshot_block_t));
  if ((log[log_index].block_id != block_id) || (log[log_index].op == DELETE_BLOCK)) {
  	block->status = NON_ACTIVE;
    block->length = 0;
    block->pages_cap = 0;
    block->pages = NULL;
  } else if (log[log_index].op == CREATE_BLOCK) {
  	block->status = ACTIVE;
    block->length = 0;
    block->pages_cap = 0;
    block->pages = NULL;
  } else {
  	block->status = ACTIVE;
    block->length = log[log_index].block_length;
    if (block->length == 0)
      block->pages_cap = 0;
    else
      block->pages_cap = (block->length - 1) / filesystem->page_size + 1;
    block->pages = (page_t*) malloc(block->pages_cap * sizeof(page_t));
    for (i = 0; i < block->pages_cap; i++)
      block->pages[i] = NULL;
    log_ptr = log + log_index;
    nr_unfilled_pages = block->pages_cap;
    while ((log_ptr != NULL) && (nr_unfilled_pages > 0)) {
      for (i = 0; i < log_ptr->nr_pages; i++) {
        if (block->pages[log_ptr->page_id + i] == NULL) {
          nr_unfilled_pages--;
          block->pages[log_ptr->page_id + i] = log_ptr->pages + i * filesystem->page_size;
        }
      }
      if (log_ptr->op != CREATE_BLOCK)
        log_ptr = log + log_ptr->previous;
      else
        log_ptr = NULL;
    }
  }
  
  // Write the block back to snapshot map for future references.
  if (MAP_CREATE_AND_WRITE(snapshot_block, snapshot->block_map, block_id, block) != 0) {
    fprintf(stderr, "ERROR: Could not create snapshot block although it does not exist in the block map.\n");
    exit(0);
  }
	MAP_UNLOCK(snapshot_block, snapshot->block_map, block_id);
  (*block_ptr) = block;
  return 1;
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
    if(bwc->fs->page_size != write(bwc->page_fd, pmle->data[i].data, bwc->fs->page_size)) {
      fprintf(stderr, "Write page failed: logid=%ld.\n", bwc->next_entry);
      print_log(&bwc->fs->log[bwc->next_entry]);
      return -2;
    }
  }
  // 3 - write log to log file
  if(sizeof(dle) != write(bwc->log_fd, &dle,sizeof(dle))){
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
  // flush to persistent layer
  fsync(bwc->log_fd);
  fsync(bwc->page_fd);
  fsync(bwc->snap_fd);
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

static int do_bmap_flush(blog_writer_ctxt_t *bwc)
{
  // STEP 1 get "this"
  jclass thisCls = (*bwc->env)->GetObjectClass(bwc->env, bwc->thisObj);
  jmethodID mid = (*bwc->env)->GetMethodID(bwc->env,thisCls,"flushBlockMap", "()V");
  if (mid == NULL) {
    perror("Error");
    exit(-1);
  }
  // STEP 2 call flush()
  (*bwc->env)->CallVoidMethod(bwc->env, bwc->thisObj, mid);
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

  JavaVM *jvm;
  int gotVM = (*bwc->env)->GetJavaVM(bwc->env,&jvm);
  if((*jvm)->AttachCurrentThread(jvm, (void*)&(bwc->env), NULL) > 0){
    fprintf(stderr,"blog_writer_routine:cannot attach current thread to JVM\n");
    fflush(stderr);
    return NULL;
  }

  while(bwc->alive){
    // STEP 1 - test if a flush is required.
    gettimeofday(&tv,NULL);
    if(time_next_write > tv.tv_sec) {
      usleep(1000000l);
      continue;
    } else {
      time_next_write = tv.tv_sec + bwc->int_sec;
    }
    // STEP 2 - flush
    if(do_blog_flush(bwc)){
      fprintf(stderr,"Cannot flush blog to persistent storage.\n");
      return param;
    }
    if(do_bmap_flush(bwc)){
      fprintf(stderr,"Cannot flush block map to persistent storage.\n");
    }
  }
  if(do_blog_flush(bwc)){
    fprintf(stderr,"Cannot flush blog to persistent storage.\n");
    return param;
  }
  if(do_snap_flush(bwc)){
    fprintf(stderr,"Cannot flush snapshot to persistent storage.\n");
  }
  if(do_bmap_flush(bwc)){
    fprintf(stderr,"Cannot flush block map to persistent storage.\n");
  }
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
  // skip empty log file
  if(log_stat.st_size == 0)return 0;
  // else continue ...
  if (fstat(page_fd, &page_stat)) {
    fprintf(stderr, "call fstat on page file descriptor failed");
    loadBlogReturn(-2);
  }
  if (fstat(snap_fd, &snap_stat)) {
    fprintf(stderr, "call fstat on snapshot file descriptor failed");
    loadBlogReturn(-3);
  }
  if ((plog = mmap(NULL,log_stat.st_size,PROT_READ,MAP_SHARED,log_fd,0)) == (void*) -1) {
    fprintf(stderr, "call mmap on log file descriptor failed");
    loadBlogReturn(-1);
  }
  if ((ppage = mmap(NULL,page_stat.st_size,PROT_READ,MAP_SHARED,page_fd,0)) == (void*) -1) {
    fprintf(stderr, "call mmap on page file descriptor failed");
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
      for (i = 0; i < pdle->pages_length; i++){
        pages[i].data = (char*) (page_mem + fs->page_size*(i+1));
      }
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
        for (i = 0; i < pdle->pages_length; i++){
          block->pages[pdle->pages_offset+i] = ((page_t*)page_mem) + i;
        }
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
  // skip empty snapshot
  if(snap_stat.st_size == 0) return 0;
  // else continue ...
  if ((psnap = mmap(NULL,snap_stat.st_size,PROT_READ,MAP_SHARED,snap_fd,0)) == (void*) -1) {
    fprintf(stderr, "call mmap on snap file descriptor failed");
    loadBlogReturn(-3);
  }
  pse = (int64_t*)psnap;
  int nsnap = (snap_stat.st_size/sizeof(int64_t)/2);
  while (nsnap--) {
    int64_t rtc = *pse++;
    int64_t eid = *pse++;
    if(do_load_snapshot(fs,rtc,eid)){
      fprintf(stderr,"load snapshot failed for rtc=%ld,eid=%ld!\n",rtc,eid);
      loadBlogReturn(-1);
    };
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
  filesystem->log_cap = 1024;
  filesystem->log_length = 1;
  filesystem->log = (log_t *) malloc (1024*sizeof(log_t));
  filesystem->log[0].op = BOL;
  filesystem->log[0].r = 0;
  filesystem->log[0].l = 0;
  
  filesystem->log_map = MAP_INITIALIZE(log);
  if (filesystem->log_map == NULL) {
    fprintf(stderr, "ERROR: Allocation of log_map failed.\n");
    exit(0);
  }
  filesystem->snapshot_map = MAP_INITIALIZE(snapshot);
  if (filesystem->snapshot_map == NULL) {
    fprintf(stderr, "ERROR: Allocation of snapshot_map failed.\n");
    exit(0);
  }
  filesystem->block_map = MAP_INITIALIZE(block);
  if (filesystem->block_map == NULL) {
    fprintf(stderr, "ERROR: Allocation of block_map failed.\n");
    exit(0);
  }
  pthread_rwlock_init(&(filesystem->log_lock), NULL);
  
  (*env)->SetObjectField(env, thisObj, hlc_id, hlc_object);
  (*env)->SetLongField(env, thisObj, long_id, (uint64_t) filesystem);
  
  fullpath = (char*) malloc(strlen(pp)+MAX_FNLEN+1);
  sprintf(fullpath,"%s/%s",pp,BLOGFILE);
  if (file_exists(fullpath)) {
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
    close(log_fd);
    close(page_fd);
    close(snap_fd);
  }
  // start write thread.
  filesystem->bwc.fs = filesystem;
  sprintf(fullpath, "%s/%s",pp,BLOGFILE);
  filesystem->bwc.log_fd = open(fullpath, O_RDWR|O_APPEND|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH);
  sprintf(fullpath, "%s/%s",pp,PAGEFILE);
  filesystem->bwc.page_fd = open(fullpath, O_RDWR|O_APPEND|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH);
  sprintf(fullpath, "%s/%s",pp,SNAPFILE);
  filesystem->bwc.snap_fd = open(fullpath, O_RDWR|O_APPEND|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH);

  if(filesystem->bwc.log_fd == -1 || 
     filesystem->bwc.page_fd == -1 || 
     filesystem->bwc.snap_fd == -1){
    fprintf(stderr,"Cannot open data node files, exit...\n");
    exit(1);
  }
  filesystem->bwc.next_entry = filesystem->log_length;
  filesystem->bwc.int_sec = 60; // every minutes
  filesystem->bwc.alive = 1; // alive.
  filesystem->bwc.env = env; // java environment
  filesystem->bwc.thisObj = (*env)->NewGlobalRef(env, thisObj); // java this object
  
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
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  uint64_t block_id = (uint64_t) blockId;
  block_t *block;
  log_t *log_entry;
  uint64_t log_index;
  
  // If the block already exists return an error.
  MAP_LOCK(block, filesystem->block_map, block_id, 'w');
  if (MAP_CREATE(block, filesystem->block_map, block_id) != 0) {
    fprintf(stderr, "WARNING: Block with ID %" PRIu64 " already exists.\n", block_id);
    return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);
  
  // Create the corresponding log entry.
  pthread_rwlock_wrlock(&(filesystem->log_lock));
  log_index = filesystem->log_length;
  check_and_increase_log_length(filesystem);
  tick_hybrid_logical_clock(env, get_hybrid_logical_clock(env, thisObj), mhlc);
  log_entry = filesystem->log + log_index;
  log_entry->block_id = block_id;
  log_entry->op = CREATE_BLOCK;
  log_entry->block_length = 0;
  update_log_clock(env, mhlc, log_entry);
  log_entry->pages = NULL;
  filesystem->log_length += 1;
  pthread_rwlock_unlock(&(filesystem->log_lock));
  
  // Create the block structure.
  block = (block_t *) malloc(sizeof(block_t));
  block->log_index = log_index;
  block->length = 0;
  block->pages_cap = 0;
  block->pages = NULL;

  MAP_LOCK(block, filesystem->block_map, block_id, 'w');
  if (MAP_WRITE(block, filesystem->block_map, block_id, block) != 0) {
    fprintf(stderr, "ERROR: Block with ID %" PRIu64 " was not found in the block map while it should be there.\n",
            block_id);
    exit(0);
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);
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
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  uint64_t block_id = (uint64_t) blockId;
  block_t *block;
  log_t *log_entry;
  uint64_t log_index;
  
  // If the block does not exist return an error.
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) == -1) {
    fprintf(stderr, "WARNING: It is not possible to delete block with ID %" PRIu64 " because it does not exist.\n",
            block_id);
    MAP_UNLOCK(block, filesystem->block_map, block_id);
    return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);
  
  // Create the corresponding log entry.
  pthread_rwlock_wrlock(&(filesystem->log_lock));
  log_index = filesystem->log_length;
  check_and_increase_log_length(filesystem);
  tick_hybrid_logical_clock(env, get_hybrid_logical_clock(env, thisObj), mhlc);
  log_entry = filesystem->log + log_index;
  log_entry->block_id = block_id;
  log_entry->op = DELETE_BLOCK;
  log_entry->block_length = 0;
  log_entry->previous = block->log_index;
  update_log_clock(env, mhlc, log_entry);
  log_entry->pages = NULL;
  filesystem->log_length += 1;
  pthread_rwlock_unlock(&(filesystem->log_lock));
  
  // Delete the block in block map for future references.
  if (block->pages != NULL)
    free(block->pages);
  free(block);
  MAP_LOCK(block, filesystem->block_map, block_id, 'w');
  if (MAP_DELETE(block, filesystem->block_map, block_id) != 0) {
    fprintf(stderr, "ERROR: Block with ID %" PRIu64 " was not found in the block map while it should have been there.\n",
            block_id);
    exit(0);
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);
  return 0;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readBlock
 * Signature: (JJJIII[B)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_readBlock__JIII_3B
  (JNIEnv *env, jobject thisObj, jlong blockId, jint blkOfst, jint bufOfst, jint length,
   jbyteArray buf)
{
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  snapshot_t *snapshot;
  snapshot_block_t *block;
  uint64_t block_id = (uint64_t) blockId;
  uint32_t block_offset = (uint32_t) blkOfst;
  uint32_t buffer_offset = (uint32_t) bufOfst;
  uint32_t read_length = (uint32_t) length;
  uint64_t log_index;
  uint32_t cur_length, page_id, page_offset;
  char *page_data;
  
  // Find the last entry.
  pthread_rwlock_rdlock(&filesystem->log_lock);
  log_index = filesystem->log_length-1;
  pthread_rwlock_unlock(&filesystem->log_lock);
  
  // Find or create snapshot.
  MAP_LOCK(snapshot, filesystem->snapshot_map, log_index, 'w');
  if (MAP_READ(snapshot, filesystem->snapshot_map, log_index, &snapshot) != 0) {
    snapshot = (snapshot_t *) malloc(sizeof(snapshot_t));
    snapshot->ref_count = 0;
    snapshot->block_map = MAP_INITIALIZE(snapshot_block);
    if (snapshot->block_map == NULL) {
      fprintf(stderr, "ERROR: Allocation of block map failed.\n");
      exit(0);
    }
    snapshot->ref_count = 0;
    if (MAP_CREATE_AND_WRITE(snapshot, filesystem->snapshot_map, log_index, snapshot) == -1) {
      fprintf(stderr, "ERROR: Snapshot with log index %" PRIu64 " cannot be created or read in the snapshot map.\n",
              log_index);
      exit(0);
    }
  }

  if (find_or_create_snapshot_block(filesystem, snapshot, log_index, block_id, &block) < 0) {
    fprintf(stderr, "ERROR: Block with id %" PRIu64 " was not created or found.\n", block_id);
    exit(0);
  }
  
  // In case the block does not exist return an error.
  if (block->status == NON_ACTIVE) {
      fprintf(stderr, "WARNING: Block with id %" PRId64 " is not active at snapshot with log index %" PRIu64 ".\n",
              block_id, log_index);
      return -1;
  }
  
  // In case the data you ask is not written return an error.
  if (block_offset >= block->length) {
    fprintf(stderr, "WARNING: Block %" PRId64 " is not written at %" PRId32 " byte.\n", block_id, block_offset);
    return -2;
  }
  
  // See if the data is partially written.
  if (block_offset + read_length > block->length)
    read_length = block->length - block_offset;
  
  // Fill the buffer.
  page_id = block_offset / filesystem->page_size;
  page_offset = block_offset % filesystem->page_size;
  page_data = block->pages[page_id];
  page_data += page_offset;
  cur_length = filesystem->page_size - page_offset;
  if (cur_length >= read_length) {
    (*env)->SetByteArrayRegion(env, buf, buffer_offset, read_length, (jbyte*) page_data);
  } else {
  	(*env)->SetByteArrayRegion(env, buf, buffer_offset, cur_length, (jbyte*) page_data);
  	page_id++;
  	while (1) {
  		page_data = block->pages[page_id];
  		if (cur_length + filesystem->page_size >= read_length) {
        (*env)->SetByteArrayRegion(env, buf, buffer_offset + cur_length, read_length - cur_length, (jbyte*) page_data);
        break;
      }
      (*env)->SetByteArrayRegion(env, buf, buffer_offset + cur_length, filesystem->page_size, (jbyte*) page_data);
      cur_length += filesystem->page_size;
      page_id++;
    }
  }
  return read_length;
}
  

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readBlock
 * Signature: (JJIII[B)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_readBlock__JJIII_3B
  (JNIEnv *env, jobject thisObj, jlong blockId, jlong t, jint blkOfst, jint bufOfst, jint length, jbyteArray buf)
{
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  snapshot_t *snapshot;
  snapshot_block_t *block;
  uint64_t snapshot_time = (uint64_t) t;
  uint64_t block_id = (uint64_t) blockId;
  uint32_t block_offset = (uint32_t) blkOfst;
  uint32_t buffer_offset = (uint32_t) bufOfst;
  uint32_t read_length = (uint32_t) length;
  uint64_t *log_ptr;
  uint64_t log_index;
  uint32_t cur_length, page_id, page_offset;
  char *page_data;
  
  // Find the corresponding block.
  if (find_or_create_snapshot(filesystem, snapshot_time, &snapshot) < 0) {
      fprintf(stderr, "WARNING: Snapshot for time %" PRIu64 " cannot be created.\n", snapshot_time);
      return -1;
  }
  MAP_LOCK(log, filesystem->log_map, snapshot_time, 'r');
  if (MAP_READ(log, filesystem->log_map, snapshot_time, &log_ptr) != 0) {
    fprintf(stderr, "ERROR: Snapshot time %" PRIu64 " was not found in the log map while it should have been there.\n",
            snapshot_time);
    exit(0);
  }
  MAP_UNLOCK(log, filesystem->log_map, snapshot_time);
  log_index = *log_ptr;
  if (find_or_create_snapshot_block(filesystem, snapshot, log_index, block_id, &block) < 0) {
    fprintf(stderr, "ERROR: Block with id %" PRIu64 " was not created or found.\n", block_id);
    exit(0);
  }
  
  // In case the block does not exist return an error.
  if (block->status == NON_ACTIVE) {
      fprintf(stderr, "WARNING: Block with id %" PRIu64 " is not active at snapshot with rtc %" PRIu64 ".\n", block_id,
              snapshot_time);
      return -2;
  }
  
  // In case the data you ask is not written return an error.
  if (block_offset >= block->length) {
    fprintf(stderr, "WARNING: Block %" PRIu64 " is not written at %" PRIu32 " byte.\n", block_id, block_offset);
    return -3;
  }
  
  // See if the data is partially written.
  if (block_offset + read_length > block->length)
    read_length = block->length - block_offset;
  
  // Fill the buffer.
  page_id = block_offset / filesystem->page_size;
  page_offset = block_offset % filesystem->page_size;
  page_data = block->pages[page_id];
  page_data += page_offset;
  cur_length = filesystem->page_size - page_offset;
  if (cur_length >= read_length) {
    (*env)->SetByteArrayRegion(env, buf, buffer_offset, read_length, (jbyte*) page_data);
  } else {
  	(*env)->SetByteArrayRegion(env, buf, buffer_offset, cur_length, (jbyte*) page_data);
  	page_id++;
  	while (1) {
  		page_data = block->pages[page_id];
  		if (cur_length + filesystem->page_size >= read_length) {
        (*env)->SetByteArrayRegion(env, buf, buffer_offset + cur_length, read_length - cur_length, (jbyte*) page_data);
        break;
      }
      (*env)->SetByteArrayRegion(env, buf, buffer_offset + cur_length, filesystem->page_size, (jbyte*) page_data);
      cur_length += filesystem->page_size;
      page_id++;
    }
  }
  return read_length;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    getNumberOfBytes
 * Signature: (JJJ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_getNumberOfBytes__J
  (JNIEnv *env, jobject thisObj, jlong blockId)
{
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  log_t *log = filesystem->log;
  uint64_t log_index;
  uint64_t block_id = (uint64_t) blockId;
  
  // Find the last entry.
  pthread_rwlock_rdlock(&filesystem->log_lock);
  log_index = filesystem->log_length-1;
  pthread_rwlock_unlock(&filesystem->log_lock);
  
  // Find the last entry with 
  while ((log_index > 0) && (log[log_index].block_id != block_id))
    log_index--;
    
  if (log[log_index].block_id != block_id) {
    fprintf(stderr, "WARNING: Block with id %" PRIu64 " is not active.\n", block_id);
    return -1;
  }

  return (jint) log[log_index].block_length;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    getNumberOfBytes
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_getNumberOfBytes__JJ
  (JNIEnv *env, jobject thisObj, jlong blockId, jlong t)
{
  filesystem_t *filesystem = get_filesystem(env, thisObj);;
  snapshot_t *snapshot;
  snapshot_block_t *block;
  uint64_t *log_ptr;
  uint64_t log_index;
  uint64_t block_id = (uint64_t) blockId;
  uint64_t snapshot_time = (uint64_t) t;
  
  // Find the corresponding block.
  if (find_or_create_snapshot(filesystem, snapshot_time, &snapshot) < 0) {
      fprintf(stderr, "WARNING: Snapshot for time %" PRIu64 " cannot be created.\n", snapshot_time);
      return -2;
  }
  MAP_LOCK(log, filesystem->log_map, snapshot_time, 'r');
  if (MAP_READ(log, filesystem->log_map, snapshot_time, &log_ptr) != 0) {
    fprintf(stderr, "ERROR: Snapshot time %" PRIu64 " was not found in the log map while it should have been there.\n",
            snapshot_time);
    exit(0);
  }
  MAP_UNLOCK(log, filesystem->log_map, snapshot_time);
  log_index = *log_ptr; 
  if (find_or_create_snapshot_block(filesystem, snapshot, log_index, block_id, &block) < 0) {
    fprintf(stderr, "ERROR: Block with id %" PRIu64 " was not created or found.\n", block_id);
    exit(0);
  }
  
  // In case the block does not exist return an error.
  if (block->status == NON_ACTIVE) {
      fprintf(stderr, "WARNING: Block with id %" PRIu64 " is not active at snapshot with rtc %" PRIu64 ".\n", block_id,
              snapshot_time);
      return -1;
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
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  uint64_t block_id = (uint64_t) blockId;
  uint32_t block_offset = (uint32_t) blkOfst;
  uint32_t buffer_offset = (uint32_t) bufOfst;
  uint32_t write_length = (uint32_t) length;
  size_t page_size = filesystem->page_size;
  log_t *log_entry;
  block_t *block;
  uint64_t log_index;
  uint32_t first_page, last_page, block_length, write_page_length, first_page_offset, last_page_length, pages_cap;
  char *data, *temp_data;
  uint32_t i;

  // printf("Block: %" PRIu64 "\n", block_id);
  // fflush(stdout);

  // If the block does not exist return an error.
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) == -1) {
    fprintf(stderr, "WARNING: It is not possible to write to block with ID %" PRIu64 " because it does not exist.\n",
            block_id);
    MAP_UNLOCK(block, filesystem->block_map, block_id);
    return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);

	// In case you write after the end of the block return an error.
  if (block_offset > block->length) {
    fprintf(stderr, "WARNING: Block %" PRIu64 " cannot be written at %" PRIu32 " byte.\n", block_id, block_offset);
    return -2;
  }

  // Create the new pages.
  first_page = block_offset / page_size;
  // printf("First Page: %" PRIu32 "\n", first_page);
  // fflush(stdout);
  last_page = (block_offset + write_length - 1) / page_size;
  // printf("Last Page: %" PRIu32 "\n", last_page);
  // fflush(stdout);
  first_page_offset = block_offset % page_size;
  // printf("First Page Offset: %" PRIu32 "\n", first_page_offset);
  // fflush(stdout);
  data = (char*) malloc((last_page-first_page+1) * page_size * sizeof(char));
  temp_data = data;
  
  // Write the first page until block_offset.
  if (first_page_offset > 0) {
    strncpy(temp_data, block->pages[first_page], first_page_offset);
    temp_data += first_page_offset;
    // printf("Filled the initial page up to %" PRIu32 "\n", first_page_offset);
    // fflush(stdout);
  }
  
  // Write all the data from the packet.
  if (first_page == last_page) {
    write_page_length = write_length;
    (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_page_length, (jbyte *) temp_data);
    temp_data += write_page_length;
    last_page_length = first_page_offset + write_page_length;
    // printf("Filled the last page up to %" PRIu32 "\n", write_page_length+first_page_offset);
    // fflush(stdout);
  } else {
    write_page_length = filesystem->page_size - first_page_offset;
    (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_page_length, (jbyte *) temp_data);
    buffer_offset += write_page_length;
    temp_data += write_page_length;
    // printf("Filled the initial page completely\n");
    // fflush(stdout);
    for (i = 1; i < last_page-first_page; i++) {
      write_page_length = page_size;
      (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_page_length, (jbyte *) temp_data);
      buffer_offset += write_page_length;
      temp_data += write_page_length;
    }
    // printf("Filled all the intermediate pages.\n");
    // fflush(stdout);
    write_page_length = (block_offset + write_length) % page_size == 0 ? page_size
                                                                       : (block_offset + write_length) % page_size;
    (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_page_length, (jbyte *) temp_data);
    temp_data += write_page_length;
    last_page_length = write_page_length;
    // printf("Filled the last page up to %" PRIu32 "\n", write_page_length);
    // fflush(stdout);
  }
  block_length = last_page * page_size + last_page_length;
  if (block_length < block->length) {
    write_page_length = last_page*(page_size+1) <= block->length ? page_size - last_page_length
                                                                 : block_length%page_size - last_page_length;
    strncpy(temp_data, block->pages[last_page] + last_page_length, write_page_length);
    // printf("Filled the last page up to %" PRIu32 "\n", write_page_length+last_page_length);
    // fflush(stdout);
    block_length = block->length;
  }
  
  // Create the corresponding log entry.
  pthread_rwlock_wrlock(&(filesystem->log_lock));
  log_index = filesystem->log_length;
  check_and_increase_log_length(filesystem);
  tick_hybrid_logical_clock(env, get_hybrid_logical_clock(env, thisObj), mhlc);
  log_entry = filesystem->log + log_index;
  log_entry->block_id = block_id;
  log_entry->op = WRITE;
  log_entry->block_length = block_length;
  log_entry->page_id = first_page;
  log_entry->nr_pages = last_page-first_page+1;
  log_entry->previous = block->log_index;
  update_log_clock(env, mhlc, log_entry);
  log_entry->pages = data;
  filesystem->log_length += 1;
  pthread_rwlock_unlock(&(filesystem->log_lock));
  
  // Fill block with the appropriate information.
  block->log_index = log_index;
  block->length = block_length;
  if (block->pages_cap == 0)
    pages_cap = 1;
  else
    pages_cap = block->pages_cap;
  while ((block->length - 1) / filesystem->page_size >= pages_cap)
    pages_cap *= 2;
  if (pages_cap > block->pages_cap) {
    block->pages_cap = pages_cap;
    block->pages = (page_t*) realloc(block->pages, block->pages_cap * sizeof(page_t));
  }
  for (i = 0; i < log_entry->nr_pages; i++)
    block->pages[log_entry->page_id+i] = data + i * filesystem->page_size;

  return 0;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    createSnapshot
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_createSnapshot
  (JNIEnv *env, jobject thisObj, jlong t)
{
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  uint64_t snapshot_time = (uint64_t) t;
  snapshot_t *snapshot;
  
  switch (find_or_create_snapshot(filesystem, snapshot_time, &snapshot)) {
    case 0:
      fprintf(stderr, "WARNING: Snapshot for time %" PRIu64 " already exists.\n", snapshot_time);
      return -1;
    case -1:
      fprintf(stderr, "WARNING: Snapshot for time %" PRIu64 " cannot be created.\n", snapshot_time);
      return -2;
    case 1:
      return 0;
    default:
      fprintf(stderr, "ERROR: Unknown error code for find_or_create_snapshot function.\n");
      exit(0);
  }
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

  // close files
  if(fs->bwc.log_fd!=-1){close(fs->bwc.log_fd),fs->bwc.log_fd=-1;}
  if(fs->bwc.page_fd!=-1){close(fs->bwc.log_fd),fs->bwc.page_fd=-1;}
  if(fs->bwc.snap_fd!=-1){close(fs->bwc.log_fd),fs->bwc.snap_fd=-1;}
}
