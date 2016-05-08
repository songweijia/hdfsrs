#include <inttypes.h>
#include <string.h>
#include <sys/time.h>
#include <limits.h>
#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/sendfile.h>
#include <fcntl.h>
#include <sys/statvfs.h>
#include <dirent.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "JNIBlog.h"
#include "types.h"
#include "InfiniBandRDMA.h"

#define MAKE_VERSION(x,y) ((((x)&0xffffl)<<16)|((y)&0xffff))
#define FFFS_VERSION MAKE_VERSION(0,1)

// Define files for storing persistent data.
#define BLOGFILE_SUFFIX "blog"
#define PAGEFILE_SUFFIX "page"
#define SHM_FN "fffs.pg"

// Definitions to be substituted by configuration:

// misc
#define MAX(x,y) (((x)>(y))?(x):(y))

// Type definitions for dictionaries.
MAP_DEFINE(block, block_t, BLOCK_MAP_SIZE);
MAP_DEFINE(log, uint64_t, LOG_MAP_SIZE);
MAP_DEFINE(snapshot, snapshot_t, SNAPSHOT_MAP_SIZE);

//local typedefs
typedef struct transport_parameters {
#define TP_LOCAL_BUFFER (0)
#define TP_RDMA (1)
  int mode;
  union {
    struct tp_local_buffer_param {
      jbyteArray buf;
      jint bufOfst;
      jlong user_timestamp; // only for write.
    } lbuf;
    struct tp_rdma_param{
      jbyteArray client_ip;
      jint remote_pid;
      jlong vaddr;
      jobject record_parser; // only for write.
    } rdma;
  } param;
} transport_parameters_t;

// some internal tools
#define LOG2(x) calc_log2(x)
inline int calc_log2(uint64_t val){
  int i=0;
  while(((val>>i)&0x1)==0 && (i<64) )i++;
  return i;
}

// Printer Functions.
char *log_to_string(filesystem_t *fs,log_t *log, size_t page_size)
{
  char *res = (char *) malloc(1024 * 1024 * sizeof(char));
  char buf[1025];
  uint32_t length, i;
  
  switch (log->op) {
  case BOL:
    strcpy(res, "Operation: Beginning of Log\n");
    break;
  case CREATE_BLOCK:
    strcpy(res, "Operation: Create Block\n");
    sprintf(buf, "HLC Value: (%" PRIu64 ",%" PRIu64 ")\n", log->r, log->l);
    strcat(res, buf);
    break;
  case DELETE_BLOCK:
    strcpy(res, "Operation: Delete Block\n");
    sprintf(buf, "HLC Value: (%" PRIu64 ",%" PRIu64 ")\n", log->r, log->l);
    strcat(res, buf);
    break;
  case WRITE:
    strcpy(res, "Operation: Write\n");
    sprintf(buf, "Block Length: %" PRIu32 "\n", (uint32_t) log->block_length);
    strcat(res, buf);
    sprintf(buf, "Starting Page: %" PRIu32 "\n", log->start_page);
    strcat(res, buf);
    sprintf(buf, "Number of Pages: %" PRIu32 "\n", log->nr_pages);
    strcat(res, buf);
    for (i = 0; i < log->nr_pages-1; i++) {
      sprintf(buf, "Page %" PRIu32 ":\n", log->start_page + i);
      strcat(res, buf);
      snprintf(buf, page_size+1, "%s", (char *)((char*)fs->page_base + (log->first_pn + i)*page_size));
      strcat(res, buf);
      strcat(res, "\n");
    }
    if (log->block_length > log->nr_pages*page_size)
      length = page_size;
    else
      length = log->block_length - (log->nr_pages-1)*page_size;
    sprintf(buf, "Page %" PRIu32 ":\n", log->start_page + log->nr_pages-1);
    strcat(res, buf);
    snprintf(buf, length+1, "%s", (char *)((char*)fs->page_base + (log->first_pn + log->nr_pages-1)*page_size));
    strcat(res, buf);
    strcat(res, "\n");
    sprintf(buf, "HLC Value: (%" PRIu64 ",%" PRIu64 ")\n", log->r, log->l);
    strcat(res, buf);
    break;
  default:
    strcat(res, "Operation: Unknown Operation\n");
  }
  return res;
}

char *snapshot_to_string(filesystem_t *fs, snapshot_t *snapshot, size_t page_size)
{
  char *res = (char *) malloc((1024*page_size) * sizeof(char));
  char buf[1025];
  uint32_t i;
  
  sprintf(buf, "Reference Count: %" PRIu64 "\n", snapshot->ref_count);
  strcpy(res, buf);
  switch (snapshot->status) {
    case ACTIVE:
      strcat(res, "Status: Active");
      break;
    case NON_ACTIVE:
      strcat(res, "Status: Non Active");
      break;
    default:
      fprintf(stderr, "ERROR: Status should be either ACTIVE or NON_ACTIVE\n");
      exit(0);
  }
  sprintf(buf, "Length: %" PRIu32 "\n", (uint32_t) snapshot->length);
  strcat(res, buf);
  if (snapshot->length != 0) {
    for (i = 0; i <= (snapshot->length - 1) / page_size; i++) {
      sprintf(buf, "Page %" PRIu32 ":\n", i);
      strcat(res, buf);
      snprintf(buf, page_size+2, "%s\n", (const char*)PAGE_NR_TO_PTR(fs, snapshot->pages[i]));
      strcat(res, buf);
    }
  }
  return res;
}

char *block_to_string(filesystem_t *fs, block_t *block, size_t page_size)
{
  char *res = (char *) malloc(1024 * 1024 * sizeof(char));
  char buf[1025];
  snapshot_t *snapshot;
  uint64_t *snapshot_ids, *log_ptr;
  uint32_t i;
  uint64_t j, log_index, map_length;
  log_t *log = (log_t*)block->log;
  
  // Pring id.
  sprintf(buf, "ID: %" PRIu64 "\n", block->id);
  strcpy(res,buf);
  
  // Print blog.
  sprintf(buf, "Log Length: %" PRIu64 "\n", block->log_length);
	strcat(res, buf);
  sprintf(buf, "Log Capacity: %" PRIu64 "\n", block->log_cap);
  strcat(res, buf);
  for (j = 0; j < block->log_length; j++) {
    sprintf(buf, "Log %" PRIu64 ":\n", j);
  	strcat(res, buf);
    strcat(res, log_to_string(fs, log + j, page_size));
    strcat(res, "\n");
  }
  
  // Print current state.
  sprintf(buf, "Status: %s\n", block->status == ACTIVE? "Active" : "Non-Active");
  strcat(res, buf);
  sprintf(buf, "Length: %" PRIu32 "\n", (uint32_t) block->length);
  strcat(res, buf);
  sprintf(buf, "Pages Capacity: %" PRIu32 "\n", block->pages_cap);
  strcat(res, buf);
  if (block->length != 0) {
    for (i = 0; i <= (block->length - 1) / page_size; i++) {
      sprintf(buf, "Page %" PRIu32 ":\n", i);
      strcat(res, buf);
      snprintf(buf, page_size+1, "%s\n", (char *)PAGE_NR_TO_PTR(fs,block->pages[i]));
      strcat(res, buf);
      strcat(res, "\n");
    }
  }
  strcat(res, "\n");
  
  // Print Snapshots.
  strcat(res, "Snapshots:\n");
  for (i = 0; i < LOG_MAP_SIZE; i++)
    MAP_LOCK(log, block->log_map_hlc, i, 'r');
  map_length = MAP_LENGTH(log, block->log_map_hlc);
  sprintf(buf, "Number of Snapshots: %" PRIu64 "\n", map_length);
  strcat(res, buf);
  if (map_length > 0)
    snapshot_ids = MAP_GET_IDS(log, block->log_map_hlc, map_length);
  for (i = 0; i < map_length; i++) {
    sprintf(buf, "RTC: %" PRIu64 "\n", snapshot_ids[i]);
    strcat(res, buf);
    if (MAP_READ(log, block->log_map_hlc, snapshot_ids[i], &log_ptr) != 0) {
      fprintf(stderr, "ERROR: Cannot read log index whose time is contained in log map.\n");
      exit(0);
    }
    log_index = *log_ptr;
  	sprintf(buf, "Last Log Index: %" PRIu64 "\n", log_index);
  	strcat(res, buf);
    MAP_LOCK(snapshot, block->snapshot_map, log_index, 'r');
    if (MAP_READ(snapshot, block->snapshot_map, log_index, &snapshot) != 0) {
      fprintf(stderr, "ERROR: Cannot read snapshot whose last log index is contained in snapshot map.\n");
      exit(0);
    }
    strcat(res, snapshot_to_string(fs, snapshot, page_size));
    strcat(res, "\n");
    MAP_UNLOCK(snapshot, block->snapshot_map, *log_ptr);
  }
  for (i = 0; i < LOG_MAP_SIZE; i++)
    MAP_UNLOCK(log, block->log_map_hlc, i);
  if (map_length > 0)
    free(snapshot_ids);
  strcat(res, "--------------------------------------------------\n");
  return res;
}

char *filesystem_to_string(filesystem_t *filesystem)
{
  char *res = (char *) malloc((1024*filesystem->page_size) * sizeof(char));
  char buf[1025];
  block_t *block;
  uint64_t *block_ids;
  uint64_t length, i;
  
  // Print Filesystem.
  strcpy(res, "Filesystem\n");
  strcat(res, "----------\n");
  sprintf(buf, "Block Size: %" PRIu64 "\n", (uint64_t) filesystem->block_size);
  strcat(res, buf);
  sprintf(buf, "Page Size: %" PRIu64 "\n", (uint64_t) filesystem->page_size);
  strcat(res, buf);
  
  // Print Blocks.
  for (i = 0; i < BLOCK_MAP_SIZE; i++)
    MAP_LOCK(block, filesystem->block_map, i, 'r');
  length = MAP_LENGTH(block, filesystem->block_map);
  sprintf(buf, "Length: %" PRIu64 "\n", length);
  strcat(res, buf);
  if (length > 0)
    block_ids = MAP_GET_IDS(block, filesystem->block_map, length);
  for (i = 0; i < length; i++) {
    if (MAP_READ(block, filesystem->block_map, block_ids[i], &block) != 0) {
      fprintf(stderr, "ERROR: Cannot read block whose id is contained in block map.\n");
      exit(0);
    }
    strcat(res, block_to_string(filesystem, block, filesystem->page_size));
  }
  for (i = 0; i < BLOCK_MAP_SIZE; i++)
    MAP_UNLOCK(block, filesystem->block_map, i);
  if (length > 0)
    free(block_ids);
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

void tick_hybrid_logical_clock(JNIEnv *env, jobject hlc, jobject mhlc)
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

// NOTE: acquire read lock on blog before call this function.
// Find last log entry that has timestamp less or equal than (r,l).
int find_last_entry(block_t *block, uint64_t r, uint64_t l, uint64_t *last_entry)
{
  log_t *log = (log_t*)block->log;
  uint64_t length, log_index, cur_diff;
  
  length = block->log_length;

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

// NOTE: acquire read lock on blog before call this function
// Find last log entry that has user timestamp less or equal than ut.
void find_last_entry_by_ut(block_t *block, uint64_t ut, uint64_t *last_entry){
  log_t *log = (log_t*)block->log;
  uint64_t length, log_index, cur_diff;

  length = block->log_length;
  if(log[length-1].u <= ut){
    log_index = length - 1;
  }else{
    log_index = length/2>0 ? length/2-1 : 0;
    cur_diff = length/4>1 ? length/4 : 1;
    while(log[log_index].u > ut || log[log_index+1].u <= ut){
      if(log[log_index].u > ut)
        log_index -= cur_diff;
      else
        log_index += cur_diff;
      cur_diff = cur_diff > 1 ? cur_diff/2 : 1;
    }
  }
  *last_entry = log_index;
}

/**
 * Check if the log capacity needs to be increased.
 * Acquire writer's lock on blog before calling me.
 * block  - Block object.
 * return  0, if successful,
 *        -1, otherwise.
 */
int check_and_increase_log_cap(block_t *block)
{
  if (block->log_length == block->log_cap) {
    block->log = (log_t *) realloc((void *)block->log, 2*block->log_cap*sizeof(log_t));
    if (block->log == NULL)
    	return -1;
    block->log_cap *= 2;
  }
  return 0;
}

/**
 * Fills the snapshot from the log.
 * NOTE: acquire read lock on the blog befaore call this
 * @param log_entry - Last log entry for snapshot.
 * @param page_size - Page size for the filesystem.
 * @return Snapshot object.
 */
snapshot_t *fill_snapshot(log_t *log_entry, uint32_t page_size) {
  snapshot_t *snapshot = (snapshot_t *) malloc(sizeof(snapshot_t));
  uint32_t nr_pages, i, cur_page;

  snapshot->ref_count = 0;

  //skip the first SET_GENSTAMP
  while(log_entry->op==SET_GENSTAMP)
    log_entry--;

  switch(log_entry->op) {
  case CREATE_BLOCK:
    snapshot->status = ACTIVE;
    snapshot->length = 0;
    snapshot->pages = NULL;
    return snapshot;
  case DELETE_BLOCK:
  case BOL:
    snapshot->status = NON_ACTIVE;
    snapshot->length = 0;
    snapshot->pages = NULL;
    return snapshot;
  case WRITE:
    snapshot->status = ACTIVE;
    snapshot->length = log_entry->block_length;
    nr_pages = (snapshot->length-1+page_size)/page_size;
    snapshot->pages = (uint64_t *) malloc(nr_pages*sizeof(uint64_t));
    for (i = 0; i < nr_pages; i++)
      snapshot->pages[i] = INVALID_PAGE_NO;
    while (nr_pages > 0) {
      for (i = 0; i < log_entry->nr_pages; i++) {
        cur_page = i + log_entry->start_page;
        if (snapshot->pages[cur_page] == INVALID_PAGE_NO) {
          snapshot->pages[cur_page] = log_entry->first_pn + i;
          nr_pages--;
        }
      }
      log_entry--;
    }
    return snapshot;
  default:
    fprintf(stderr, "ERROR: Unknown operation %" PRIu32 " detected\n", log_entry->op);
    exit(0);
  }
}

/**
 * Find a snapshot object with specific time. If not found, instatiate one.
 * NOTE: acquire read lock on blog before call this function.
 * block        - Block object.
 * snapshot_time- Time to take the snapshot.
 *                Must be lower or equal than a timestamp that might appear in the future.
 * snapshot_ptr - Snapshot pointer used for returning the correct snapshot instance.
 * by_ut        - if it is for user timestamp or not.
 * return  1, if created snapshot,
 *         0, if found snapshot,
 *         error code, otherwise. 
 */
int find_or_create_snapshot(block_t *block, uint64_t snapshot_time, size_t page_size, snapshot_t **snapshot_ptr, int by_ut)
{
  log_t *log = (log_t*)block->log;
  snapshot_t *snapshot;
  uint64_t *log_ptr;
  uint64_t log_index;
  
  // If snapshot already exists return the existing snapshot.
  MAP_LOCK(log, by_ut?block->log_map_ut:block->log_map_hlc, snapshot_time, 'w');
  if (MAP_READ(log, by_ut?block->log_map_ut:block->log_map_hlc, snapshot_time, &log_ptr) == 0) {
    log_index = *log_ptr;
    MAP_LOCK(snapshot, block->snapshot_map, log_index, 'r');
    if (MAP_READ(snapshot, block->snapshot_map, log_index, snapshot_ptr) == -1) {
      fprintf(stderr, "ERROR: Could not find snapshot in snapshot map although ID exists in log map.\n");
      exit(0);
    }
    MAP_UNLOCK(snapshot, block->snapshot_map, log_index);
    MAP_UNLOCK(log, by_ut?block->log_map_ut:block->log_map_hlc, snapshot_time);
    return 0;
  }
 	
  // for hlc: If snapshot can include future references do not create it. //TODO
  log_ptr = (uint64_t *) malloc(sizeof(uint64_t));
  if(by_ut){
    find_last_entry_by_ut(block, snapshot_time, log_ptr);
  }else{
    if (find_last_entry(block, snapshot_time-1, ULLONG_MAX, log_ptr) == -1) {
      free(log_ptr);
      fprintf(stderr, "WARNING: Snapshot was not created because it might have future entries.\n");
        return -1;
    }
  }
  log_index = *log_ptr;
  // Create snapshot.
  if(by_ut && log[log_index].u < snapshot_time){
    // we do not create a log_map entry at this point because
    // future data may come later.
  }else if (MAP_CREATE_AND_WRITE(log, by_ut?block->log_map_ut:block->log_map_hlc, snapshot_time, log_ptr) == -1) {
      fprintf(stderr, "ERROR: Could not create snapshot although it does not exist in the log map.\n");
      exit(0);
  }
  MAP_LOCK(snapshot, block->snapshot_map, log_index, 'w');
  if (MAP_READ(snapshot, block->snapshot_map, log_index, snapshot_ptr) == 0) {
  	snapshot = *snapshot_ptr;
    snapshot->ref_count++;
  } else {
    snapshot = fill_snapshot(log+log_index, page_size);
    snapshot->ref_count++;
    if (MAP_CREATE_AND_WRITE(snapshot, block->snapshot_map, log_index, snapshot) == -1) {
      fprintf(stderr, "ERROR: Could not create snapshot although it does not exist in the snapshot map.\n");
      exit(0);
    }
    (*snapshot_ptr) = snapshot;
  }
  MAP_UNLOCK(snapshot, block->snapshot_map, log_index);
  MAP_UNLOCK(log, by_ut?block->log_map_ut:block->log_map_hlc, snapshot_time);
  return 1;
}

/*
 * flushBlog: flush the log entries specified by evt:
 * evt->block:          the block;
 * evt->log_length:     the log length;
 */
static int flushBlog(filesystem_t *fs, pers_event_t *evt ){
  block_t *block = evt->block;

  // no new log to be flushed
  if(block->log_length_pers == evt->log_length)
    return 0;

  // flush to file
  for(;block->log_length_pers < evt->log_length; block->log_length_pers++){
    
    log_t *log_entry;
    void *pages;
    uint64_t copy_size;

    // get pages
    BLOG_RDLOCK(evt->block);
    log_entry = (log_t*)block->log + block->log_length_pers;
    if(log_entry->op == WRITE){
      pages = (char*)fs->page_base + log_entry->first_pn*fs->page_size;
      copy_size = log_entry->nr_pages * fs->page_size;
    } else
      copy_size = 0L;
    BLOG_UNLOCK(evt->block);

    // flush pages NOTE: page_fd was opened with O_DIRECT.
    if(copy_size>0L){
      if( write(block->page_fd, pages, copy_size) != copy_size ){
        fprintf(stderr, "Flush, cannot write to page file %ld.page, Error:%s\n",block->id,strerror(errno));
        return -1;
      }
    }

    // write log
    BLOG_RDLOCK(evt->block);
    log_entry = (log_t*)block->log + block->log_length_pers;
    if( write(block->blog_fd,log_entry,sizeof(log_t)) != sizeof(log_t) ){
      fprintf(stderr,"Flush, cannot write to blog file %ld."BLOGFILE_SUFFIX", Error:%s\n",block->id,strerror(errno));
      return -2;
    }
    BLOG_UNLOCK(evt->block);
  }

  // flush to disk
  if(fsync(block->blog_fd) != 0){
    fprintf(stderr,"Flush, cannot fsync blog file, block id:%ld, Error:%s\n",block->id,strerror(errno));
    return -3;
  }

  return 0;
}

/*
 * blog_pers_routine()
 * PARAM param: the blog writer context
 * RETURN: pointer to the param
 */
static void * blog_pers_routine(void * param)
{

  filesystem_t *fs = (filesystem_t*) param;
  pers_event_t * evt;

  while(1){

    // get event
    PERS_DEQ(fs,&evt);

    // End of Queue
    if(IS_EOQ(evt)){
      free(evt);
      do{
        PERS_DEQ(fs,&evt);
        if(evt!=NULL)free(evt);
      }while(evt!=NULL);
      break;
    }

    // flush blog
    if(flushBlog(fs, evt) != 0){
      fprintf(stderr, "Faile to flush blog: id=%ld,log_length=%ld\n",
        evt->block->id,evt->log_length);
      exit(1);
    }

    // release event
    free(evt);
  }

  return param;
}

/*
 * createShmFile:
 * 1) create a file of "size" in tmpfs
 * 2) return the file descriptor
 * 3) return a negative integer on error.
 */
static int createShmFile(const char *fn, uint64_t size){
  const char * tmpfs_dirs[] = {"/run/shm","/dev/shm",NULL};
  const char ** p_tmpfs_dir = tmpfs_dirs;

  while(access(*p_tmpfs_dir,R_OK|W_OK) < 0 && *p_tmpfs_dir != NULL) {
    p_tmpfs_dir++;
  }

  if(p_tmpfs_dir == NULL){
    fprintf(stderr, "Cannot find a tmpfs system.\n");
    return -1;
  }

  char fullname[64];
  sprintf(fullname,"%s/%s",*p_tmpfs_dir,fn);
  // check if we have enough space
  struct statvfs stat;

  if(statvfs(*p_tmpfs_dir, &stat)==-1){
    fprintf(stderr,"Cannot open tmpfs VFS:%s,error=%s\n",*p_tmpfs_dir,strerror(errno));
    return -2;
  }
  if(size > stat.f_bsize*stat.f_bavail){
    fprintf(stderr,"Cannot create file %s because we have only 0x%lx bytes free, but asked 0x%lx bytes.\n", fullname, stat.f_bsize*stat.f_bavail, size);
    return -3;
  }
  // create file
  int fd = open(fullname, O_RDWR|O_CREAT,S_IWUSR|S_IRUSR|S_IRGRP|S_IWGRP|S_IROTH);
  if(fd<0){
    fprintf(stderr,"Cannot open/create file %s. Error: %s\n",fullname,strerror(errno));
    return -4;
  }

  // truncate it to given size
  if(ftruncate(fd,size)<0){
    fprintf(stderr,"Cannot truncate file %s to size 0x%lx, Error: %s\n",fullname,size,strerror(errno));
    close(fd);
    return -5;
  }
  return fd;
}

/*
 * mapPages:
 * filesystem.page_base
 * filesystem.page_shm_fd
 * filesystem.nr_pages = 0;
 * 1) check if tmpfs ramdisk space is enough
 * 2) create a huge file in tmpfs
 * 3) map it to memory
 * 4) update filesystem
 * return a negative integer on error
 */
static int mapPages(filesystem_t *fs){

  // 1 - create a file in tmpfs
  if((fs->page_shm_fd = createShmFile(SHM_FN,fs->pool_size)) < 0){
    return -1;
  }

  // 2 - map it in memory and setup file system parameters:
  if((fs->page_base = mmap(NULL,fs->pool_size,PROT_READ|PROT_WRITE,MAP_SHARED,
    fs->page_shm_fd,0)) < 0){
    fprintf(stderr,"Fail to mmap page file %s. Error: %s\n",SHM_FN,strerror(errno));
    return -2;
  }

  // 3 - nr_pages = 0
  fs->nr_pages = 0;

  return 0;
}

/*
 * loadPages:
 * filesystem.page_fd
 * filesystem.nr_pages
 * filesystem.nr_pages_pers
 * 1) find and open page file
 * 2) load page file contents into memory
 * return a negative integer on error
 */
/* TODO: do not load pages anymore
static int loadPages(filesystem_t *fs, const char *pp){
  char fullname[256];
  sprintf(fullname,"%s/%s",pp,PAGEFILE);
  // STEP 1 Is file exists?
  fs->page_fd = open(fullname,O_RDWR|O_CREAT,S_IWUSR|S_IRUSR|S_IRGRP|S_IWGRP|S_IROTH);
  if(fs->page_fd < 0){
    fprintf(stderr, "Fail to open persistent page file %s. Error: %s\n",fullname, strerror(errno));
    return -1;
  }
  off_t fsize = lseek(fs->page_fd, 0, SEEK_END);
  // STEP 2 load
  if(fsize < fs->page_size || fsize < sizeof(PageHeader)){
    //create a new PageHeader
    PageHeader ph;
    ph.version = FFFS_VERSION;
    ph.block_size = fs->block_size;
    ph.page_size = fs->page_size;
    ph.nr_pages = 0;
    if(write(fs->page_fd,(const void *)&ph,sizeof ph) != sizeof ph){
      fprintf(stderr,"Fail to write to on-disk page file %s. Error: %s\n",fullname, strerror(errno));
      close(fs->page_fd);
      fs->page_fd = -1;
      return -2;
    }
    if(sizeof ph < fs->page_size){
      const int pdsz = fs->page_size - sizeof ph;
      void *padding = malloc(pdsz);
      memset(padding,0,pdsz);
      if(write(fs->page_fd,(const void *)padding, pdsz)!=pdsz){
        fprintf(stderr,"Fail to write to on-disk page file %s. Error: %s\n",fullname, strerror(errno));
        close(fs->page_fd);
        fs->page_fd = -1;
        return -3;
      }
      free(padding);
    }
    fs->nr_pages = 0;
    fs->nr_pages_pers = 0;
  }else{
    //load valid pages.
    lseek(fs->page_fd, 0, SEEK_SET);
    PageHeader ph;
    if(read(fs->page_fd,(void *)&ph,sizeof ph)!=sizeof ph){// if we can not read page header
      fprintf(stderr,"Fail to read from on-disk page file %s. Error: %s\n",fullname, strerror(errno));
      close(fs->page_fd);
      fs->page_fd = -1;
      return -4;
    }
    if(fsize < (MAX(fs->page_size,sizeof ph) + ph.nr_pages*fs->page_size)){// corrupted page file
      fprintf(stderr,"Fail to load pages, header shows %ld pages. But file size is %ld\n", ph.nr_pages, fsize);
      close(fs->page_fd);
      fs->page_fd = -1;
      return -5;
    }else{// valid page file
      if(fsize > (MAX(fs->page_size,sizeof ph) + ph.nr_pages*fs->page_size)){
        // truncate file to correct size, this may be from aborted writes.
        if(ftruncate(fs->page_fd, (MAX(fs->page_size,sizeof ph) + ph.nr_pages*fs->page_size))!=0){
          fprintf(stderr,"Fail to truncate page file %s, Error: %s\n", fullname, strerror(errno));
          close(fs->page_fd);
          fs->page_fd = -1;
          return -6;
        }
      }
      lseek(fs->page_fd, MAX(fs->page_size,sizeof ph), SEEK_SET); // jump to the first page.
      uint64_t i;
      for(i=0;i<ph.nr_pages;i++){// read them into memory.
        if(read(fs->page_fd, (void *)(fs->page_base + i*fs->page_size), fs->page_size)!=fs->page_size){
          fprintf(stderr,"Fail to read from on-disk page file %s. Error: %s\n",fullname, strerror(errno));
          close(fs->page_fd);
          fs->page_fd = -1;
          return -7;
        }
      }
      fs->nr_pages = ph.nr_pages;
      fs->nr_pages_pers = ph.nr_pages;
    }
  }
  return 0;
}
*/

/*
 * replayBlog() will replay all log entries to reconstruct block info
 * It assume the following entries to be intialized:
 * 1) block->id
 * 2) block->log and log_length are set.
 * 3) block->log_cap is set.
 * 4) block->blog_fd is set.
 * 5) block->page_fd is set.
 * 6) fs->page_base is set.
 * 7) fs->page_shm_fd is set.
 * The following will be set:
 * 1) block->status
 * 2) block->length
 * 3) block->pages_cap
 * 4) block->pages
 * 5) fs->nr_pages
 * 6) page is loaded to *page_base;
 * 7) Java metadata is initialzied by JNIBlog.replayLogOnMetadata().
 */
static void replayBlog(JNIEnv *env, jobject thisObj, filesystem_t *fs, block_t *block){
  uint64_t i;
  uint32_t j;
  jclass thisCls = (*env)->GetObjectClass(env, thisObj);
  jmethodID rl_mid = (*env)->GetMethodID(env, thisCls, "replayLogOnMetadata", "(JIJ)V");
  off_t fsize = 0L; //page file size

  block->length = 0;
  block->status = NON_ACTIVE;
  for(i=0;i<block->log_length;i++){
    log_t *log = (log_t*)block->log+i;

    switch(log->op){
      case BOL:
      case SET_GENSTAMP:
        break;
      case CREATE_BLOCK:
        block->status = ACTIVE;
        block->length = 0;
        MAP_LOCK(block, fs->block_map, block->id, 'w');
        if (MAP_CREATE_AND_WRITE(block, fs->block_map, block->id, block) != 0) {
          fprintf(stderr, "replayBlog:Faile to create block:%ld.\n",block->id);
          exit(1);
        }
        break;

      case WRITE:
        block->length = log->block_length;
        while(block->pages_cap < (block->length+fs->page_size-1)/fs->page_size)
          block->pages_cap=MAX(block->pages_cap*2,block->pages_cap+2);
        block->pages = (uint64_t*)realloc(block->pages,sizeof(uint64_t)*block->pages_cap);

        //LOAD pages from disk file
        lseek(fs->page_shm_fd,(off_t)log->first_pn*fs->page_size,SEEK_SET);
        for(j=0;j<log->nr_pages;j++){
          block->pages[log->start_page+j] = log->first_pn + j;
          if(sendfile(fs->page_shm_fd,block->page_fd,&fsize,fs->page_size)!=fs->page_size){
            fprintf(stderr,"Cannot load page for block %ld, Error: %s\n",
              block->id,strerror(errno));
            exit(1);
          }
        }

        //UPDATE fs->nr_pages
        fs->nr_pages = MAX(fs->nr_pages,log->first_pn+log->nr_pages);

        //TRUNCATE PAGE file
        if(lseek(block->page_fd,0,SEEK_END)>fsize){
          ftruncate(block->page_fd,fsize);
          lseek(block->page_fd,0,SEEK_END);
        }
        break;

      case DELETE_BLOCK:
        block->status = NON_ACTIVE;
        block->log_length = 0;
        if(block->log_cap > 0)free(block->pages);
        block->log_cap = 0;
        block->pages = NULL;
        break;

      default:
        fprintf(stderr,"replayBlog: unknown log type:%d, %ld-th log entry of block-%ld\n",
          log->op,i,block->id);
        exit(1);
    }
    // replay log on java metadata.
    (*env)->CallObjectMethod(env,thisObj,rl_mid,block->id,log->op,log->first_pn);
  }
}

/*
 * loadBlogs()
 *   we assume that the following members have been initialized already:
 *   - block_size
 *   - page_size
 *   - page_base
 *   - int_sec
 *   - alive
 *   on success, the following members should have been initialized:
 *   - block_map
 * PARAM 
 *   fs: file system structure
 *   pp: path to the persistent data.
 * RETURN VALUES: 
 *    0 for succeed. and writer_thrd will be initialized.
 *   -1 for "log file is corrupted", 
 *   -2 for "page file is correupted".
 *   -3 for "block operation".
 *   -4 for "unknown errors"
 */
static int loadBlogs(JNIEnv *env, jobject thisObj, filesystem_t *fs, const char *pp){
  // STEP 1 find all <id>.blog files
  DIR *d;
  struct dirent *dir;
  uint64_t block_id;
  d = opendir(pp);
  if(d==NULL){
    fprintf(stderr,"Cannot open directory:%s, Error:%s\n", pp, strerror(errno));
    return -1;
  };
  // STEP 2 for each .blog file
  while((dir=readdir(d))!=NULL){
    DEBUG_PRINT("Loading %s...",dir->d_name);
    /// 2.1 if it is not a .blog file, go to the next
    char blog_filename[256],page_filename[256];
    if(strlen(dir->d_name) < strlen(BLOGFILE_SUFFIX) + 1 ||
       dir->d_type != DT_REG ||
       strcmp(dir->d_name + strlen(dir->d_name) - strlen(BLOGFILE_SUFFIX) -1, "."BLOGFILE_SUFFIX)){
      DEBUG_PRINT("skipped.\n");
      continue;
    }
    sscanf(dir->d_name,"%ld."BLOGFILE_SUFFIX,&block_id);
    DEBUG_PRINT("Block id = %ld\n",block_id);

    /// 2.2 open file
    sprintf(blog_filename,"%s/%ld."BLOGFILE_SUFFIX,pp,block_id);
    sprintf(page_filename,"%s/%ld."PAGEFILE_SUFFIX,pp,block_id);
    int blog_fd = open(blog_filename,O_RDWR);
    int page_fd = open(page_filename,O_RDWR|O_DIRECT);
    if(blog_fd < 0 || page_fd < 0){
      fprintf(stderr,"Cannot open file: %s or %s, error:%s\n",blog_filename,page_filename,strerror(errno));
      return -2;
    }

    /// 2.3 check file length, truncate it when required.
    off_t fsize = lseek(blog_fd,0,SEEK_END);
    off_t corrupted_bytes = fsize % sizeof(log_t);
    off_t exp_fsize = fsize - corrupted_bytes;
    if(corrupted_bytes > 0){//this is due to aborted log flush.
      if(ftruncate(blog_fd,exp_fsize)<0){
        fprintf(stderr,"WARNING: Cannot load blog: %ld."BLOGFILE_SUFFIX", because it cannot be truncated to expected size:%ld, Error%s\n", block_id, exp_fsize, strerror(errno));
        close(blog_fd);
        continue;
      }
    }
    lseek(blog_fd,0,SEEK_SET);

    /// 2.4 create a block structure;
    block_t *block = (block_t*)malloc(sizeof(block_t));
    block->id = block_id;
    block->log_length = exp_fsize / sizeof(log_t);
    block->log_length_pers = block->log_length;
    block->log_cap = 16;
    while(block->log_cap<block->log_length)
      block->log_cap = block->log_cap << 1;
    block->log = (log_t*)malloc(sizeof(log_t)*block->log_cap);
    block->pages = NULL;
    block->pages_cap = 0;
    block->log_map_hlc = MAP_INITIALIZE(log);
    block->log_map_ut = MAP_INITIALIZE(log);
    block->snapshot_map = MAP_INITIALIZE(snapshot);
    block->blog_fd = blog_fd;
    block->page_fd = page_fd;
    
    /// 2.5 load log entries
    int i;
    for(i = 0;i<block->log_length;i++){
      if(read(blog_fd,(void*)(block->log+i),sizeof(log_t))!=sizeof(log_t)){
        fprintf(stderr, "WARNING: Cannot read log entry from %s, error:%s\n", blog_filename, strerror(errno));
        close(blog_fd);
        free((void*)block->log);
        free(block);
        continue;
      }
    }

    /// 2.6 replay log entries: reconstruct the block status
    replayBlog(env,thisObj,fs,block);
    DEBUG_PRINT("done.\n");
  }
  closedir(d);
  return 0;
}


#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
int initializeInternal(JNIEnv *env, jobject thisObj, uint64_t poolSize, 
  uint32_t blockSize, uint32_t pageSize, const char *persPath, 
  int useRDMA, const char *devname, const uint16_t rdmaPort){

  DEBUG_PRINT("initializeInternal is called\n");

  jclass thisCls = (*env)->GetObjectClass(env, thisObj);
  jfieldID long_id = (*env)->GetFieldID(env, thisCls, "jniData", "J");
  jfieldID hlc_id = (*env)->GetFieldID(env, thisCls, "hlc", "Ledu/cornell/cs/sa/HybridLogicalClock;");
  jclass hlc_class = (*env)->FindClass(env, "edu/cornell/cs/sa/HybridLogicalClock");
  jmethodID cid = (*env)->GetMethodID(env, hlc_class, "<init>", "()V");
  jobject hlc_object = (*env)->NewObject(env, hlc_class, cid);
  filesystem_t *fs = (filesystem_t *) malloc (sizeof(filesystem_t));
  if (fs == NULL) {
    perror("Error");
    exit(1);
  }


  // STEP 1: Initialize file system members
  fs->block_size = blockSize;
  fs->pool_size = poolSize;
  fs->page_size = pageSize;
  fs->block_map = MAP_INITIALIZE(block);
  if (fs->block_map == NULL) { 
    fprintf(stderr, "ERROR: Allocation of block_map failed.\n");
    exit(-1);
  }
  strcpy(fs->pers_path,persPath);

  // STEP 2: create ramdisk file and map it into memory.
  if(mapPages(fs)!=0){
    perror("mapPages Error");
    exit(1);
  }

  // STEP 2.5: for RDMA
  if(useRDMA){
    fs->rdmaCtxt = (RDMACtxt*)malloc(sizeof(RDMACtxt));
    if(initializeContext(fs->rdmaCtxt,fs->page_base,LOG2(poolSize),LOG2(pageSize),devname,rdmaPort,0/*this is for the server side*/)){
      fprintf(stderr, "Initialize: fail to initialize RDMA context.\n");
      exit(1);
    }
  } else {
    fs->rdmaCtxt = NULL;
  }

  // STEP 3: load blogs.
  if(loadBlogs(env,thisObj,fs,persPath)!=0){
    fprintf(stderr,"Fail to load blogs.\n");
  }

  // STEP 4: initialize page spin lock
  if(pthread_spin_init(&fs->pages_spinlock,PTHREAD_PROCESS_PRIVATE)!=0){
    fprintf(stderr,"Fail to initialize page spin lock, error: %s\n",strerror(errno));
    exit(1);
  }
  
  (*env)->SetObjectField(env, thisObj, hlc_id, hlc_object);
  (*env)->SetLongField(env, thisObj, long_id, (uint64_t) fs);

  // STEP 5: initialize queues, semaphore and flags.
  if(sem_init(&fs->pers_queue_sem,0,0)<0){
    fprintf(stderr,"Cannot initialize semaphore, Error:%s\n",strerror(errno));
    exit(-1);
  }
  TAILQ_INIT(&fs->pers_queue); // queue
  if(pthread_spin_init(&fs->queue_spinlock,PTHREAD_PROCESS_PRIVATE)!=0){
    fprintf(stderr,"Fail to initialize queue spin lock, error: %s\n",strerror(errno));
    exit(1);
  }

  // STEP 6: start the persistent thread.
  if (pthread_create(&fs->pers_thrd, NULL, blog_pers_routine, (void*)fs)) {
    fprintf(stderr,"CANNOT create blogPersistent thread, exit\n");
    exit(-1);
  }

  DEBUG_PRINT("initializeInternal is done\n");
  return 0;
}
#pragma GCC diagnostic pop

/*
 * Class:     edu_cornell_cs_blog_JNIBlog_initialize
 * Method:    initialize
 * Signature: (JIILjava/lang/String;)I
 */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_initialize
  (JNIEnv *env, jobject thisObj, jlong poolSize, jint blockSize, jint pageSize, jstring persPath)
{
  const char * pp = (*env)->GetStringUTFChars(env,persPath,NULL); // get the presistent path
  jint ret;

  //call internal initializer:
  ret = initializeInternal(env, thisObj, poolSize, blockSize, pageSize, pp, 0, NULL, 0);

  (*env)->ReleaseStringUTFChars(env, persPath, pp);

  return ret;
}
#pragma GCC diagnostic pop

/*
 * Class:     edu_cornell_cs_blog_JNIBlog_initializeRDMA
 * Method:    initialize
 * Signature: (II)I
 */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_initializeRDMA
  (JNIEnv *env, jobject thisObj, jlong poolSize, jint blockSize, jint pageSize, jstring persPath, jstring dev, jint port)
{
  const char * pp = (*env)->GetStringUTFChars(env,persPath,NULL); // get the presistent path
  const char * devname = (*env)->GetStringUTFChars(env,dev,NULL); // get the device name
  jint ret;

  //call internal initializer:
  ret = initializeInternal(env, thisObj, poolSize, blockSize, pageSize, pp, 1, devname, (const uint16_t )port);

  (*env)->ReleaseStringUTFChars(env, persPath, pp);
  (*env)->ReleaseStringUTFChars(env, dev, devname);

  return ret;
}
#pragma GCC diagnostic pop

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    createBlock
 * Signature: (Ledu/cornell/cs/sa/HybridLogicalClock;JJ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_createBlock
  (JNIEnv *env, jobject thisObj, jobject mhlc, jlong blockId, jlong genStamp)
{
DEBUG_PRINT("begin createBlock\n");
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  uint64_t block_id = (uint64_t) blockId;
  block_t *block;
  log_t *log_entry;
  
  // If the block already exists return an error.
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) == 0) {
    fprintf(stderr, "WARNING: Block with ID %" PRIu64 " already exists.\n", block_id);
    return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);
  
  // Create the block structure.
  block = (block_t *) malloc(sizeof(block_t));
  block->id = block_id;

  // Create the log.
  block->log_length = 2;
  block->log_cap = 2;
  block->log = (log_t*) malloc(2*sizeof(log_t));
  
  // add the first two entries.
  log_entry = (log_t*)block->log;
  log_entry->op = BOL;
  log_entry->block_length = 0;
  log_entry->start_page = 0;
  log_entry->nr_pages = 0;
  log_entry->r = 0;
  log_entry->l = 0;
  log_entry->u = 0;
  log_entry->first_pn = 0;
  log_entry++;
  log_entry->op = CREATE_BLOCK;
  log_entry->block_length = 0;
  log_entry->start_page = 0;
  log_entry->nr_pages = 0;
  tick_hybrid_logical_clock(env, get_hybrid_logical_clock(env, thisObj), mhlc);
  update_log_clock(env,mhlc,log_entry);
  log_entry->u = 0;
  
  // Update current state.
  block->status = ACTIVE;
  block->length = 0;
  block->pages_cap = 0;
  block->pages = NULL;
  
  // Create snapshot structures.
  block->log_map_hlc = MAP_INITIALIZE(log);
  block->log_map_ut = MAP_INITIALIZE(log);
  block->snapshot_map = MAP_INITIALIZE(snapshot);
  
  // initialize rwlock.
  if(pthread_rwlock_init(&block->blog_rwlock,NULL)!=0){
    fprintf(stderr, "ERROR: cannot initialize rw lock for block:%ld\n", block->id);
    exit(-1);
  }

  // open files
  char fullname[256];
  sprintf(fullname,"%s/%ld."BLOGFILE_SUFFIX,filesystem->pers_path,block_id);
  if((block->blog_fd = open(fullname,O_RDWR|O_CREAT,S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH))<0){
    fprintf(stderr, "ERROR: cannot open blog file for write, block id=%ld, Error: %s\n",block_id,strerror(errno));
    exit(-2);
  }
  sprintf(fullname,"%s/%ld."PAGEFILE_SUFFIX,filesystem->pers_path,block_id);
  if((block->page_fd = open(fullname,O_DIRECT|O_RDWR|O_CREAT,S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH))<0){
    fprintf(stderr, "ERROR: cannot open page file for write, block id=%ld, Error: %s\n",block_id,strerror(errno));
    exit(-3);
  }
  block->log_length_pers = 0;

  // notify the persistent thread
  pers_event_t *evt = (pers_event_t*)malloc(sizeof(pers_event_t));
  evt->block = block;
  evt->log_length = block->log_length;
  PERS_ENQ(filesystem,evt);

  // Put block to block map.
  MAP_LOCK(block, filesystem->block_map, block_id, 'w');
  if (MAP_CREATE_AND_WRITE(block, filesystem->block_map, block_id, block) != 0) {
    fprintf(stderr, "ERROR: Block with ID %" PRIu64 " was not found in the block map while it should be there.\n",
            block_id);
    exit(0);
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);
DEBUG_PRINT("end createBlock\n");
  return 0;
}

/*
 * NOTE: acquire write lock on blog before calling me.
 * estimate the user's timestamp where it is not avaialbe.
 * For deleteBlock(), we need this.
 * we assume that u = K*r; where K is a linear coefficient.
 * so u2 = u1/r1*r2
 */
long estimate_user_timestamp(block_t *block, long r){
  log_t *le = (log_t*)block->log+block->log_length-1;

  if(block->log_length == 0 || le->r == 0)
    return r;
  else
    return (long)((double)le->u/(double)le->r*r);
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    deleteBlock
 * Signature: (Ledu/cornell/cs/sa/VectorClock;J)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_deleteBlock
  (JNIEnv *env, jobject thisObj, jobject mhlc, jlong blockId)
{
DEBUG_PRINT("begin deleteBlock.\n");
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  uint64_t block_id = (uint64_t) blockId;
  block_t *block;
  log_t *log_entry;
  
  // If the block does not exist return an error.
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) == -1 || block->status != ACTIVE) {
    fprintf(stderr, "WARNING: It is not possible to delete block with ID %" PRIu64 " because it does not exist.\n",
            block_id);
    MAP_UNLOCK(block, filesystem->block_map, block_id);
    return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);
  
  // Create the corresponding log entry.
  if(BLOG_WRLOCK(block)!=0){
    fprintf(stderr, "ERROR: Cannot acquire write lock on block:%ld, Error:%s\n",
      block_id, strerror(errno));
    return -2;
  }
  check_and_increase_log_cap(block);
  log_entry = (log_t*)block->log + block->log_length;
  log_entry->op = DELETE_BLOCK;
  log_entry->block_length = 0;
  log_entry->start_page = 0;
  log_entry->nr_pages = 0;
  tick_hybrid_logical_clock(env, get_hybrid_logical_clock(env, thisObj), mhlc);
  update_log_clock(env, mhlc, log_entry);
  log_entry->u = estimate_user_timestamp(block,log_entry->r);
  log_entry->first_pn = 0;
  block->log_length += 1;
  BLOG_UNLOCK(block);

  // notify the persistent thread
  pers_event_t *evt = (pers_event_t*)malloc(sizeof(pers_event_t));
  evt->block = block;
  evt->log_length = block->log_length;
  PERS_ENQ(filesystem,evt);
  
  // Release the current state of the block.
  block->status = NON_ACTIVE;
  block->length = 0;
  if (block->pages_cap > 0)
    free(block->pages);
  block->pages_cap = 0;
  block->pages = NULL;
  return 0;
}

// readBlockInternal: read the latest state of a block
int readBlockInternal(JNIEnv *env, jobject thisObj, jlong blockId, jint blkOfst, jint length, const transport_parameters_t *tp)
{
  DEBUG_PRINT("begin readBlockInternal.\n");
  struct timeval tv1,tv2;
  DEBUG_TIMESTAMP(tv1);
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  snapshot_t *snapshot;
  block_t *block;
  log_t *log_entry;
  uint64_t block_id = (uint64_t) blockId;
  uint32_t block_offset = (uint32_t) blkOfst;
  uint32_t read_length = (uint32_t) length;
  uint64_t log_index;
  char *page_data;
  
  // Find the block.
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) != 0 || block->status != ACTIVE) {
    fprintf(stderr, "WARNING: Block with id %" PRIu64 " has never been active\n", block_id);
    return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);
  
  // Find if snapshot exists for this log index.
  if(BLOG_RDLOCK(block)!=0){
    fprintf(stderr, "ERROR: Cannot acquire read lock on block:%ld, Error:%s\n",
      block_id, strerror(errno));
    return -1;
  }
  log_index = block->log_length-1;
  log_entry = (log_t*)block->log + log_index;
  MAP_LOCK(snapshot, block->snapshot_map, log_index, 'w');
  if (MAP_READ(snapshot, block->snapshot_map, log_index, &snapshot) != 0) {
    snapshot = fill_snapshot(log_entry, filesystem->page_size);
    if (MAP_CREATE_AND_WRITE(snapshot, block->snapshot_map, log_index, snapshot) != 0) {
      fprintf(stderr, "ERROR: Cannot create or read snapshot for Block %" PRIu64 " and index %" PRIu64 "\n", block_id,
              log_index);
      exit(0);
    }
  }
  MAP_UNLOCK(snapshot, block->snapshot_map, log_index);
  BLOG_UNLOCK(block);
  
  // In case the block does not exist return an error.
  if (snapshot->status == NON_ACTIVE) {
      fprintf(stderr, "WARNING: Block with id %" PRIu64 " is not active at snapshot with log index %" PRIu64 ".\n",
              block_id, log_index);
      return -1;
  }
  
  // In case the data you ask is not written return an error.
  if (block_offset >= snapshot->length) {
    fprintf(stderr, "WARNING: Block %" PRIu64 " is not written at %" PRIu32 " byte.\n", block_id, block_offset);
    return -2;
  }

  // See if the data is partially written.
  if (block_offset + read_length > snapshot->length)
    read_length = snapshot->length - block_offset;
  
  if(tp->mode == TP_LOCAL_BUFFER){ // read to local buffer;
    uint32_t cur_length, page_id, page_offset;
    page_id = block_offset / filesystem->page_size;
    jbyteArray buf = tp->param.lbuf.buf;
    uint32_t buffer_offset = (uint32_t) tp->param.lbuf.bufOfst;
    // Fill the buffer.
    page_offset = block_offset % filesystem->page_size;
    page_data = PAGE_NR_TO_PTR(filesystem,snapshot->pages[page_id]);
    page_data += page_offset;
    cur_length = filesystem->page_size - page_offset;
    if (cur_length >= read_length) {
      (*env)->SetByteArrayRegion(env, buf, buffer_offset, read_length, (jbyte*) page_data);
    } else {
      (*env)->SetByteArrayRegion(env, buf, buffer_offset, cur_length, (jbyte*) page_data);
      page_id++;
      while (1) {
        page_data = PAGE_NR_TO_PTR(filesystem,snapshot->pages[page_id]);
        if (cur_length + filesystem->page_size >= read_length) {
          (*env)->SetByteArrayRegion(env, buf, buffer_offset + cur_length, read_length - cur_length, (jbyte*) page_data);
          break;
        }
        (*env)->SetByteArrayRegion(env, buf, buffer_offset + cur_length, filesystem->page_size, (jbyte*) page_data);
        cur_length += filesystem->page_size;
        page_id++;
      }
    }
  } else { // read to remote memory(RDMA)
    uint32_t start_page_id = blkOfst / filesystem->page_size;
    uint32_t end_page_id = (blkOfst+length-1) / filesystem->page_size;
    uint32_t npage = 0;
    void **paddrlist = (void**)malloc(sizeof(void*)*(end_page_id - start_page_id + 1));
    while(start_page_id<=end_page_id){
      paddrlist[npage] = PAGE_NR_TO_PTR(filesystem,snapshot->pages[start_page_id]);
      start_page_id ++;
      npage ++;
    }

    // get ip str
    int ipSize = (int)(*env)->GetArrayLength(env,tp->param.rdma.client_ip);
    jbyte ipStr[16];
    (*env)->GetByteArrayRegion(env, tp->param.rdma.client_ip, 0, ipSize, ipStr);
    ipStr[ipSize] = 0;
    uint32_t ipkey = inet_addr((const char*)ipStr);

    // get remote address
    uint64_t vaddr = tp->param.rdma.vaddr; // vaddr is start of a block.
    const uint64_t address = vaddr + block_offset - (block_offset % filesystem->page_size);

    // rdma write...
    int rc = rdmaWrite(filesystem->rdmaCtxt, (const uint32_t)ipkey, tp->param.rdma.remote_pid, (const uint64_t)address, (const void **)paddrlist,npage,0);
    free(paddrlist);
    if(rc !=0 ){
      fprintf(stderr, "readBlockRDMA: rdmaWrite failed with error code=%d.\n", rc);
      return -2;
    }
  }
  DEBUG_TIMESTAMP(tv2);
  
  DEBUG_PRINT("end readBlockInternal. %dbytes %ldus %.3fMB/s\n",
    length,TIMESPAN(tv1,tv2),(double)length/TIMESPAN(tv1,tv2));
  return read_length;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readBlock
 * Signature: (JJJIII[B)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_readBlock__JIII_3B
  (JNIEnv *env, jobject thisObj, jlong blockId, jint blkOfst, jint bufOfst, jint length, jbyteArray buf){
  // setup transport parameter.
  transport_parameters_t tp;
  tp.mode = TP_LOCAL_BUFFER;
  tp.param.lbuf.buf = buf;
  tp.param.lbuf.bufOfst = bufOfst;
  return (jint)readBlockInternal(env,thisObj,blockId,blkOfst,length,&tp);
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readBlockRDMA
 * Signature: (JII[BIJ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_readBlockRDMA__JII_3BIJ
  (JNIEnv *env, jobject thisObj, jlong blockId, jint blkOfst, 
    jint length, jbyteArray clientIp, jint rpid, jlong vaddr){

  // setup transport parameter.
  transport_parameters_t tp;
  tp.mode = TP_RDMA;
  tp.param.rdma.client_ip = clientIp;
  tp.param.rdma.remote_pid = rpid;
  tp.param.rdma.vaddr = vaddr;

  return (jint)readBlockInternal(env,thisObj,blockId,blkOfst,length,&tp);
}

// readBlockInternalByTime(): read block state at given timestamp...
int readBlockInternalByTime(JNIEnv *env, jobject thisObj, jlong blockId, jlong t, jint blkOfst, jint length, transport_parameters_t *tp, jboolean byUserTimestamp)
{
DEBUG_PRINT("begin readBlockInternalByTime.\n");
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  snapshot_t *snapshot;
  block_t *block;
  uint64_t snapshot_time = (uint64_t) t;
  uint64_t block_id = (uint64_t) blockId;
  uint32_t block_offset = (uint32_t) blkOfst;
  uint32_t read_length = (uint32_t) length;
  uint32_t cur_length, page_id, page_offset;
  char *page_data;
  
  // Find the block.
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) != 0) {
    fprintf(stderr, "WARNING: Block with id %" PRIu64 " has never been active\n", block_id);
    return -2;
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);
  
  // Create snapshot.
  if(BLOG_RDLOCK(block)!=0){
    fprintf(stderr, "ERROR: Cannot acquire read lock on block:%ld, Error:%s\n",
      block_id, strerror(errno));
    return -1;
  }
  if (find_or_create_snapshot(block, snapshot_time, filesystem->page_size, &snapshot, byUserTimestamp==JNI_TRUE) < 0) {
    fprintf(stderr, "WARNING: Snapshot for time %" PRIu64 " cannot be created.\n", snapshot_time);
    BLOG_UNLOCK(block);
    return -1;
  }
  BLOG_UNLOCK(block);
  
  // In case the block does not exist return an error.
  if (snapshot->status == NON_ACTIVE) {
      fprintf(stderr, "WARNING: Block with id %" PRIu64 " is not active at snapshot with rtc %" PRIu64 ".\n", block_id,
              snapshot_time);
      return -2;
  }
  
  // In case the data you ask is not written return an error.
  if (block_offset >= snapshot->length) {
    fprintf(stderr, "WARNING: Block %" PRIu64 " is not written at %" PRIu32 " byte.\n", block_id, block_offset);
    return -3;
  }
  
  // See if the data is partially written.
  if (block_offset + read_length > snapshot->length)
    read_length = snapshot->length - block_offset;
  
  // Fill the buffer.
  if(tp->mode == TP_LOCAL_BUFFER){//read to local buffer
    jbyteArray buf = tp->param.lbuf.buf;
    uint32_t buffer_offset = (uint32_t)tp->param.lbuf.bufOfst;
    page_id = block_offset / filesystem->page_size;
    page_offset = block_offset % filesystem->page_size;
    page_data = PAGE_NR_TO_PTR(filesystem,snapshot->pages[page_id]);
    page_data += page_offset;
    cur_length = filesystem->page_size - page_offset;
    if (cur_length >= read_length) {
      (*env)->SetByteArrayRegion(env, buf, buffer_offset, read_length, (jbyte*) page_data);
    } else {
      (*env)->SetByteArrayRegion(env, buf, buffer_offset, cur_length, (jbyte*) page_data);
      page_id++;
      while (1) {
        page_data = PAGE_NR_TO_PTR(filesystem,snapshot->pages[page_id]);
        if (cur_length + filesystem->page_size >= read_length) {
          (*env)->SetByteArrayRegion(env, buf, buffer_offset + cur_length, read_length - cur_length, (jbyte*) page_data);
          break;
        }
        (*env)->SetByteArrayRegion(env, buf, buffer_offset + cur_length, filesystem->page_size, (jbyte*) page_data);
        cur_length += filesystem->page_size;
        page_id++;
      }
    }
  }else{ // read to remote buffer(RDMA)
    uint32_t start_page_id = blkOfst / filesystem->page_size;
    uint32_t end_page_id = (blkOfst+length-1) / filesystem->page_size;
    uint32_t npage = 0;
    void **paddrlist = (void**)malloc(sizeof(void*)*(end_page_id - start_page_id + 1));
    while(start_page_id<=end_page_id){
      paddrlist[npage] = PAGE_NR_TO_PTR(filesystem,snapshot->pages[start_page_id]);
      start_page_id ++;
      npage ++;
    }

    // get ip str
    int ipSize = (int)(*env)->GetArrayLength(env,tp->param.rdma.client_ip);
    jbyte ipStr[16];
    (*env)->GetByteArrayRegion(env, tp->param.rdma.client_ip, 0, ipSize, ipStr);
    ipStr[ipSize] = 0;
    uint32_t ipkey = inet_addr((const char*)ipStr);

    // get remote address
    uint64_t vaddr = tp->param.rdma.vaddr; // vaddr is the start of the block.
    const uint64_t address = vaddr + block_offset - (block_offset % filesystem->page_size);

    // rdma write...
    int rc = rdmaWrite(filesystem->rdmaCtxt, (const uint32_t)ipkey, tp->param.rdma.remote_pid, (const uint64_t)address, (const void **)paddrlist,npage,0);
    free(paddrlist);
    if(rc !=0 ){
      fprintf(stderr, "readBlockRDMA: rdmaWrite failed with error code=%d.\n", rc);
      return -2;
    }
  }
DEBUG_PRINT("end readBlockInternalByTime.\n");
  return read_length;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readBlock
 * Signature: (JJIII[BZ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_readBlock__JJIII_3BZ
  (JNIEnv *env, jobject thisObj, jlong blockId, jlong t, jint blkOfst, jint bufOfst, jint length, jbyteArray buf, jboolean byUserTimestamp)
{
  // setup the transport parameter
  transport_parameters_t tp;
  tp.mode = TP_LOCAL_BUFFER;
  tp.param.lbuf.buf = buf;
  tp.param.lbuf.bufOfst = bufOfst;

  return (jint)readBlockInternalByTime(env,thisObj,blockId,t,blkOfst,length, &tp, byUserTimestamp);
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readBlockRDMA
 * Signature: (JJII[BIJZ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_readBlockRDMA__JJII_3BIJZ
  (JNIEnv *env, jobject thisObj, jlong blockId, jlong t, jint blkOfst, jint length, jbyteArray clientIp, jint rpid, jlong vaddr, jboolean byUserTimestamp)
{
  // setup the transport parameter
  transport_parameters_t tp;
  tp.mode = TP_RDMA;
  tp.param.rdma.client_ip = clientIp;
  tp.param.rdma.remote_pid = rpid;
  tp.param.rdma.vaddr = vaddr;

  return (jint)readBlockInternalByTime(env,thisObj,blockId,t,blkOfst,length, &tp, byUserTimestamp);
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    getNumberOfBytes
 * Signature: (JJJ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_getNumberOfBytes__J
  (JNIEnv *env, jobject thisObj, jlong blockId)
{
DEBUG_PRINT("begin getNumberOfBytes__J.\n");
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  block_t *block;
  uint64_t log_index;
  uint64_t block_id = (uint64_t) blockId;
  jint ret;
  
  // Find the block.
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) != 0  || block->status != ACTIVE) {
    fprintf(stderr, "WARNING: Block with id %" PRIu64 " has never been active\n", block_id);
    return -1;
  }

  MAP_UNLOCK(block, filesystem->block_map, block_id);
  
  // Find the last entry.
  if(BLOG_RDLOCK(block)!=0){
    fprintf(stderr, "ERROR: Cannot acquire read lock on block:%ld, Error:%s\n",
      block_id, strerror(errno));
    return -1;
  }
  log_index = block->log_length-1;
  ret = (jint) block->log[log_index].block_length;
  BLOG_UNLOCK(block);

DEBUG_PRINT("end of getNumberOfBytes__J.\n");
  return ret;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    getNumberOfBytes
 * Signature: (JJZ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_getNumberOfBytes__JJZ
  (JNIEnv *env, jobject thisObj, jlong blockId, jlong t, jboolean by_ut)
{
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  snapshot_t *snapshot;
  block_t *block;
  // uint64_t *log_ptr;
  // uint64_t log_index;
  uint64_t block_id = (uint64_t) blockId;
  uint64_t snapshot_time = (uint64_t) t;
  
    // Find the block.
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) != 0) {
    fprintf(stderr, "WARNING: Block with id %" PRIu64 " has never been active\n", block_id);
    return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);
  
  // Find the corresponding block.
  if(BLOG_RDLOCK(block)!=0){
    fprintf(stderr, "ERROR: Cannot acquire read lock on block:%ld, Error:%s\n",
      block_id, strerror(errno));
    return -1;
  }
  if (find_or_create_snapshot(block, snapshot_time, filesystem->page_size, &snapshot, by_ut==JNI_TRUE) < 0) {
    fprintf(stderr, "WARNING: Snapshot for time %" PRIu64 " cannot be created.\n", snapshot_time);
    BLOG_UNLOCK(block);
    return -2;
  }
  BLOG_UNLOCK(block);
  
  // In case the block does not exist return an error.
  if (snapshot->status == NON_ACTIVE) {
    fprintf(stderr, "WARNING: Block with id %" PRIu64 " is not active at snapshot with rtc %" PRIu64 ".\n",
      block_id, snapshot_time);
    return -1;
  }
  
  return (jint) snapshot->length;
}

inline void * blog_allocate_pages(filesystem_t *fs,int npage){
  void *pages = NULL;
  
  pthread_spin_lock(&fs->pages_spinlock);
  if(fs->nr_pages + npage <= (fs->pool_size/fs->page_size)){
    pages = fs->page_base + fs->page_size*fs->nr_pages;
    fs->nr_pages += npage;
  }else
    fprintf(stderr,"Cannot allocate pages: ask for %d, but we have only %ld\n",npage,(fs->pool_size/fs->page_size - fs->nr_pages));
  pthread_spin_unlock(&fs->pages_spinlock);
  return pages;
}

// writeBlockInternal: write block
int writeBlockInternal (JNIEnv *env, jobject thisObj, jobject mhlc, jlong blockId, jint blkOfst, jint length, const transport_parameters_t * tp)
{
DEBUG_PRINT("beging writeBlock.\n");
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  uint64_t block_id = (uint64_t) blockId;
  uint32_t block_offset = (uint32_t) blkOfst;
  uint32_t write_length = (uint32_t) length;
  size_t page_size = filesystem->page_size;
  log_t *log_entry;
  block_t *block;
  uint64_t log_index,userTimestamp;
  uint32_t first_page, last_page, end_of_write, write_page_length, first_page_offset, last_page_length, pages_cap;
  char *data, *temp_data;
  uint32_t i;

  // If the block does not exist return an error.
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) == -1 || block->status != ACTIVE) {
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
  last_page = (block_offset + write_length - 1) / page_size;
  first_page_offset = block_offset % page_size;
  data = (char*)blog_allocate_pages(filesystem,last_page - first_page + 1);
 
  if(tp->mode == TP_LOCAL_BUFFER){// write to local buffer
    jbyteArray buf = tp->param.lbuf.buf;
    uint32_t buffer_offset = (uint32_t)tp->param.lbuf.bufOfst;
    temp_data = data + first_page_offset;

    // Write all the data from the packet.
    if (first_page == last_page) {
      (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_length, (jbyte *) temp_data);
      temp_data += write_length;
      last_page_length = first_page_offset + write_length;
    } else {
      write_page_length = page_size - first_page_offset;
      (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_page_length, (jbyte *) temp_data);
      buffer_offset += write_page_length;
      temp_data += write_page_length;
      for (i = 1; i < last_page-first_page; i++) {
        write_page_length = page_size;
        (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_page_length, (jbyte *) temp_data);
        buffer_offset += write_page_length;
        temp_data += write_page_length;
      }
      write_page_length = (block_offset + write_length - 1) % page_size + 1;
      (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_page_length, (jbyte *) temp_data);
      temp_data += write_page_length;
      last_page_length = write_page_length;
    }

    // get user timestamp
    userTimestamp = tp->param.lbuf.user_timestamp;
  } else { // read from remote memory
    // set up position markers
    last_page_length = (block_offset + length) % page_size; // could be zero!!!
    if(last_page_length == 0)last_page_length = page_size; // fix it.
    uint32_t new_pages_length = last_page - first_page + 1; // number of new pages.

    // setup page array
    void **paddrlist = (void**)malloc(new_pages_length*sizeof(void*));//page list for rdma transfer
    for(i=0;i<new_pages_length;i++)
      paddrlist[i] = (void*)(data + page_size*i);

    // do write by rdma read...
    // - get ipkey
    int ipSize = (int)(*env)->GetArrayLength(env,tp->param.rdma.client_ip);
    jbyte ipStr[16];
    (*env)->GetByteArrayRegion(env,tp->param.rdma.client_ip, 0, ipSize, ipStr);
    ipStr[ipSize] = 0;
    uint32_t ipkey = inet_addr((const char*)ipStr);


    // - get remote address, remote address is start of the block
    const uint64_t address = tp->param.rdma.vaddr + first_page*page_size;
    // - rdma read by big chunk 
    // int rc = rdmaRead(filesystem->rdmaCtxt, (const uint32_t)ipkey, tp->param.rdma.remote_pid, (const uint64_t)address, (const void**)&paddr, 1, new_pages_length*page_size);
    // or by pages.
    int rc = rdmaRead(filesystem->rdmaCtxt, (const uint32_t)ipkey, tp->param.rdma.remote_pid, (const uint64_t)address, (const void**)paddrlist, new_pages_length, page_size);
    free(paddrlist);
    if(rc != 0){
      fprintf(stderr, "writeBlockInternal: rdmaRead failed with error code = %d.\n", rc);
      return -3;
    }
    // update temp_data to the last character in position.
    temp_data = data + first_page_offset + length;

    // get user timestamp
    {
      //// create direct byte buffer
      jobject bbObj = (*env)->NewDirectByteBuffer(env, (void*)data, new_pages_length*page_size );
      if(bbObj == NULL){
        fprintf(stderr, "writeBlockInternal: cannot create java DirectByteBuffer using NewDirectByteBuffer(env=%p,data=%p,capacity=%ld)\n",env,data,(last_page-first_page)*page_size);
        return -4;
      }
      //// set position and limit
      jclass bbClz = (*env)->GetObjectClass(env,bbObj);
      if(bbClz == NULL){
        fprintf(stderr, "writeBlockInternal: cannot get DirectByteBuffer class!\n");
        return -5;
      }
      jmethodID position_id = (*env)->GetMethodID(env, bbClz, "position", "(I)Ljava/nio/Buffer;");
      jmethodID limit_id = (*env)->GetMethodID(env, bbClz, "limit", "(I)Ljava/nio/Buffer;");
      (*env)->CallObjectMethod(env,bbObj,position_id,first_page_offset);
      (*env)->CallObjectMethod(env,bbObj,limit_id,first_page_offset + length);

      //// call record paerser
      jobject rpObj = tp->param.rdma.record_parser;
      if(rpObj == NULL){
        fprintf(stderr, "writeBlockInternal: record_parser is NULL!\n");
        return -6;
      }
      jclass rpClz = (*env)->GetObjectClass(env,rpObj);
      if(rpClz == NULL){
        fprintf(stderr, "writeBlockInternal: cannot get record_parser class!\n");
        return -7;
      }
      jmethodID parse_record_id = (*env)->GetMethodID(env, rpClz, "ParseRecord", "(Ljava/nio/ByteBuffer;)I");
      jmethodID get_user_timestamp_id = (*env)->GetMethodID(env, rpClz, "getUserTimestamp", "()J");
      (*env)->CallIntMethod(env,rpObj,parse_record_id,bbObj);
      if((*env)->ExceptionCheck(env) == JNI_TRUE){
        fprintf(stderr, "writeBlockInternal: record_parser cannot parse data!\n");
        return -8;
      }
      userTimestamp = (uint64_t)(*env)->CallLongMethod(env,rpObj,get_user_timestamp_id);
    } // end get user timestamp
  }

  //fill the first written page, if required.
  if (first_page_offset > 0) {
    memcpy(data, PAGE_NR_TO_PTR(filesystem,block->pages[first_page]), first_page_offset);
  }

  //fill the last written page, if required.
  end_of_write = last_page * page_size + last_page_length;
  if (end_of_write < block->length) {
    write_page_length = last_page*(page_size+1) <= block->length ? page_size - last_page_length
                                                                 : block->length%page_size - last_page_length;
    memcpy(temp_data, PAGE_NR_TO_PTR(filesystem,block->pages[last_page]) + last_page_length, write_page_length);
  } else { // update the block length
    block->length = end_of_write;
  }


  // Create the corresponding log entry. 
  if(BLOG_WRLOCK(block)!=0){
    fprintf(stderr, "ERROR: Cannot acquire write lock on block:%ld, Error:%s\n",
      block_id, strerror(errno));
    return -9;
  }


  log_index = block->log_length;
  check_and_increase_log_cap(block);
  log_entry = (log_t*)block->log + log_index;
  log_entry->op = WRITE;
  log_entry->block_length = block->length;
  log_entry->start_page = first_page;
  log_entry->nr_pages = last_page-first_page+1;
  tick_hybrid_logical_clock(env, get_hybrid_logical_clock(env, thisObj), mhlc);
  update_log_clock(env, mhlc, log_entry);
  log_entry->u = userTimestamp;
  log_entry->first_pn = PAGE_PTR_TO_NR(filesystem,data);
  block->log_length += 1;
  BLOG_UNLOCK(block);


  // notify the persistent thread
  pers_event_t *evt = (pers_event_t*)malloc(sizeof(pers_event_t));
  evt->block = block;
  evt->log_length = block->log_length;
  PERS_ENQ(filesystem,evt);

  // Fill block with the appropriate information.
  if (block->pages_cap == 0)
    pages_cap = 1;
  else
    pages_cap = block->pages_cap;
  while ((block->length - 1) / filesystem->page_size >= pages_cap)
    pages_cap *= 2;
  if (pages_cap > block->pages_cap) {
    block->pages_cap = pages_cap;
    block->pages = (uint64_t*) realloc(block->pages, block->pages_cap * sizeof(uint64_t));
  }
  for (i = 0; i < log_entry->nr_pages; i++)
    block->pages[log_entry->start_page+i] = PAGE_PTR_TO_NR(filesystem,data) + i;
DEBUG_PRINT("end writeBlock.\n");
  return 0;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    writeBlock
 * Signature: (Ledu/cornell/cs/sa/VectorClock;JIII[B)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_writeBlock
  (JNIEnv *env, jobject thisObj, jobject mhlc, jlong userTimestamp, jlong blockId, jint blkOfst,
  jint bufOfst, jint length, jbyteArray buf){

  //setup transport parameters
  transport_parameters_t tp;
  tp.mode = TP_LOCAL_BUFFER;
  tp.param.lbuf.buf = buf;
  tp.param.lbuf.bufOfst = bufOfst;
  tp.param.lbuf.user_timestamp = userTimestamp;
  
  return (jint)writeBlockInternal(env, thisObj, mhlc, blockId, blkOfst, length, &tp);
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    getNumberOfBytes
 * Signature: (JJZ)I
 * Method:    writeBlock
 * Signature: (Ledu/cornell/cs/sa/HybridLogicalClock;Ledu/cornell/cs/blog/IRecordParser;JII[BIJ)I 
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_writeBlockRDMA
  (JNIEnv *env, jobject thisObj, jobject mhlc, jobject rp, jlong blockId, jint blkOfst,
  jint length, jbyteArray clientIp, jint rpid, jlong bufaddr){

  //setup transport parameters
  transport_parameters_t tp;
  tp.mode = TP_RDMA;
  tp.param.rdma.client_ip = clientIp;
  tp.param.rdma.remote_pid = rpid;
  tp.param.rdma.vaddr = bufaddr;
  tp.param.rdma.record_parser = rp;// only for write.
  
  return (jint)writeBlockInternal(env, thisObj, mhlc, blockId, blkOfst, length, &tp);
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

JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_getPid
  (JNIEnv *env, jclass thisCls)
{
  return getpid();
}

JNIEXPORT void Java_edu_cornell_cs_blog_JNIBlog_destroy
  (JNIEnv *env, jobject thisObj)
{
DEBUG_PRINT("beging destroy.\n");
  filesystem_t *fs;
  fs = get_filesystem(env,thisObj);
  void * ret;

  // kill blog writer
  pers_event_t *evt = (pers_event_t*)malloc(sizeof(pers_event_t));
  evt->block = NULL;
  PERS_ENQ(fs,evt);
  if(pthread_join(fs->pers_thrd, &ret))
    fprintf(stderr,"waiting for blogWriter thread error...we may lose some data.\n");

  // fs destroy the spin locks and sempahores
  pthread_spin_destroy(&fs->pages_spinlock);
  sem_destroy(&fs->pers_queue_sem);
  pthread_spin_destroy(&fs->queue_spinlock);

  // fs destroy the blog locks
  uint64_t nr_blog = MAP_LENGTH(block,fs->block_map);
  uint64_t *ids = MAP_GET_IDS(block,fs->block_map,nr_blog);
  while(nr_blog --){
    uint64_t block_id = ids[nr_blog];
    block_t *block;
    MAP_LOCK(block,fs->block_map,block_id,'w');
    if(MAP_READ(block,fs->block_map, block_id, &block) == -1) {
      fprintf(stderr, "Warning: cannot read block from map, id=%ld.\n",block_id);
      continue;
    }
    pthread_rwlock_destroy(&block->blog_rwlock);
    close(block->blog_fd);
    close(block->page_fd);
    MAP_UNLOCK(block,fs->block_map,block_id);
  }

  // destroy RDMAContext
  if(fs->rdmaCtxt != NULL){
    destroyContext(fs->rdmaCtxt);
  }

  // close files
  if(fs->page_shm_fd!=-1){
    munmap(fs->page_base,fs->pool_size);
    close(fs->page_shm_fd);
    fs->page_shm_fd=-1;
  }
  
  // remove shm file.
  char fullname[256];
  sprintf(fullname,"%s/"SHM_FN,fs->pers_path);
  if(remove(fullname)!=0){
    fprintf(stderr,"WARNING: Fail to remove page cache file %s, error:%s.\n",fullname,strerror(errno));
  }
DEBUG_PRINT("end destroy.\n");
}

/**
 * set generation stamp a block. // this is for Datanode.
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_setGenStamp
  (JNIEnv *env, jobject thisObj, jobject mhlc, jlong blockId, jlong genStamp){
DEBUG_PRINT("begin setGenStamp.\n");
  filesystem_t *filesystem;
  uint64_t block_id = (uint64_t)blockId;
  block_t *block;
  log_t *log_entry;

  filesystem = get_filesystem(env,thisObj);
  //STEP 1: check if blockId is included
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) == -1) {
    fprintf(stderr, "WARNING: It is not possible to set generation stamp for block with ID %" PRIu64 " because it does not exist.\n",
            block_id);
    MAP_UNLOCK(block, filesystem->block_map, block_id);
    return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);

  //STEP 2: append log
  if(BLOG_WRLOCK(block)!=0){
    fprintf(stderr, "ERROR: Cannot acquire write lock on block:%ld, Error:%s\n",
      block_id, strerror(errno));
    return -2;
  }
  check_and_increase_log_cap(block);
  log_entry = (log_t*)block->log + block->log_length;
  log_entry->op = SET_GENSTAMP;
  log_entry->block_length = block->length;
  log_entry->start_page = 0;
  log_entry->nr_pages = 0;
  tick_hybrid_logical_clock(env, get_hybrid_logical_clock(env, thisObj), mhlc);
  update_log_clock(env, mhlc, log_entry);
  log_entry->u = (block->log + block->log_length - 1)->u; // we reuse the last userTimestamp
  log_entry->first_pn = genStamp;
  block->log_length += 1;
  BLOG_UNLOCK(block);

  // notify the persistent thread
  pers_event_t *evt = (pers_event_t*)malloc(sizeof(pers_event_t));
  evt->block = block;
  evt->log_length = block->log_length;
  PERS_ENQ(filesystem,evt);

DEBUG_PRINT("end setGenStamp.\n");
  return 0;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpInitialize
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpInitialize
  (JNIEnv *env, jclass thisCls, jint psz, jint align, jstring dev, jint port){
  RDMACtxt *ctxt = (RDMACtxt*)malloc(sizeof(RDMACtxt));
  const char * devname = (*env)->GetStringUTFChars(env,dev,NULL); // get the RDMA device name.
  if(initializeContext(ctxt,NULL,(const uint32_t)psz,(const uint32_t)align,devname,(const uint16_t)port,1)){ // this is for client
    free(ctxt);
    fprintf(stderr, "Cannot initialize rdma context.\n");
    if(devname)(*env)->ReleaseStringUTFChars(env, dev, devname);
    return (jlong)0;
  }
  if(devname)(*env)->ReleaseStringUTFChars(env, dev, devname);
  return (jlong)ctxt;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpDestroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpDestroy
  (JNIEnv *env, jclass thisCls, jlong hRDMABufferPool){
  destroyContext((RDMACtxt*)hRDMABufferPool);
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpAllocateBlockBuffer
 * Signature: (J)Ledu/cornell/cs/blog/JNIBlog$RBPBuffer;
 */
JNIEXPORT jobject JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpAllocateBlockBuffer
  (JNIEnv *env, jclass thisCls, jlong hRDMABufferPool){
  // STEP 1: create an object
  jclass bufCls = (*env)->FindClass(env, "edu/cornell/cs/blog/JNIBlog$RBPBuffer");
  if(bufCls==NULL){
    fprintf(stderr,"Cannot find the buffers.");
    return NULL;
  }
  jmethodID bufConstructorId = (*env)->GetMethodID(env, bufCls, "<init>", "()V");
  if(bufConstructorId==NULL){
    fprintf(stderr,"Cannot find buffer constructor method.\n");
    return NULL;
  }
  jobject bufObj = (*env)->NewObject(env,bufCls,bufConstructorId);
  if(bufObj == NULL){
    fprintf(stderr,"Cannot create buffer object.");
    return NULL;
  }
  jfieldID addressId = (*env)->GetFieldID(env, bufCls, "address", "J");
  jfieldID bufferId = (*env)->GetFieldID(env, bufCls, "buffer", "Ljava/nio/ByteBuffer;");
  if(addressId == NULL || bufferId == NULL){
    fprintf(stderr,"Cannot get some field of buffer class");
    return NULL;
  }
  // STEP 2: allocate buffer
  RDMACtxt *ctxt = (RDMACtxt*)hRDMABufferPool;
  void *buf;
  if(allocateBuffer(ctxt, &buf)){
    fprintf(stderr, "Cannot allocate buffer.\n");
    return NULL;
  }
  jobject bbObj = (*env)->NewDirectByteBuffer(env,buf,(jlong)1l<<ctxt->align);
  //STEP 3: fill buffer object
  (*env)->SetLongField(env, bufObj, addressId, (jlong)buf);
  (*env)->SetObjectField(env, bufObj, bufferId, bbObj);

  return bufObj;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpReleaseBuffer
 * Signature: (JLedu/cornell/cs/blog/JNIBlog$RBPBuffer;)V
 */
JNIEXPORT void JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpReleaseBuffer
  (JNIEnv *env, jclass thisCls, jlong hRDMABufferPool, jobject rbpBuffer){
  RDMACtxt *ctxt = (RDMACtxt*)hRDMABufferPool;
  void* bufAddr;
  // STEP 1: get rbpbuffer class
  jclass bufCls = (*env)->FindClass(env, "edu/cornell/cs/blog/JNIBlog$RBPBuffer");
  if(bufCls==NULL){
    fprintf(stderr,"Cannot find the buffers.");
    return;
  }
  jfieldID addressId = (*env)->GetFieldID(env, bufCls, "address", "J");
  // STEP 2: get fields
  bufAddr = (void*)(*env)->GetLongField(env, rbpBuffer, addressId);
  // STEP 3: release buffer
  if(releaseBuffer(ctxt,bufAddr))
    fprintf(stderr,"Cannot release buffer@%p\n",bufAddr);
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpConnect
 * Signature: (JII)V
 */
JNIEXPORT void JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpConnect
  (JNIEnv *env, jclass thisCls, jlong hRDMABufferPool, jbyteArray hostIp){
  RDMACtxt *ctxt = (RDMACtxt*)hRDMABufferPool;
  int ipSize = (int)(*env)->GetArrayLength(env,hostIp);
  jbyte ipStr[16];
  (*env)->GetByteArrayRegion(env, hostIp, 0, ipSize, ipStr);
  ipStr[ipSize] = 0;
  uint32_t ipkey = inet_addr((const char*)ipStr);
  int rc = rdmaConnect(ctxt, (const uint32_t)ipkey);
  if(rc == 0 || rc == -1){
    // do nothing, -1 means duplicated connection.
  }else
    fprintf(stderr,"Setting up RDMA connection to %s failed with error %d.\n", (char*)ipStr, rc);
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpRDMAWrite
 * Signature: (IJJ[J)V
 */
JNIEXPORT void JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpRDMAWrite
  (JNIEnv *env, jobject thisObj, jbyteArray clientIp, jlong address, jlongArray pageList){
  // get filesystem
  filesystem_t *fs = get_filesystem(env,thisObj);
  // get ipkey
  int ipSize = (int)(*env)->GetArrayLength(env,clientIp);
  jbyte ipStr[16];
  (*env)->GetByteArrayRegion(env, clientIp, 0, ipSize, ipStr);
  ipStr[ipSize] = 0;
  uint32_t ipkey = inet_addr((const char*)ipStr);
  // get pagelist
  int npage = (*env)->GetArrayLength(env,pageList);
  long *plist = (long*)malloc(sizeof(long)*npage);
  void **paddrlist = (void**)malloc(sizeof(void*)*npage);
  (*env)->GetLongArrayRegion(env, pageList, 0, npage, plist);
  int i;
  for(i=0; i<npage; i++)
    paddrlist[i] = (void*)plist[i];
  // rdma write...
  int rc = rdmaWrite(fs->rdmaCtxt, (const uint32_t)ipkey, fs->rdmaCtxt->port, (const uint64_t)address, (const void **)paddrlist,npage,0);
  if(rc !=0 )
    fprintf(stderr, "rdmaWrite failed with error code=%d.\n", rc);
}

