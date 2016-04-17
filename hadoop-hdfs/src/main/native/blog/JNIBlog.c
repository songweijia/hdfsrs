#include <inttypes.h>
#include <string.h>
#include <sys/time.h>
#include <limits.h>
#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include "JNIBlog.h"
#include "types.h"

// Define files for storing persistent data.
#define BLOGFILE_PREFIX "blog"
#define BLOGFILE_SUFFIX "_dat"
#define PAGEFILE "page._dat"
#define MAX_FNLEN (strlen(BLOGFILE)+strlen(PAGEFILE))

// Type definitions for dictionaries.
MAP_DEFINE(block, block_t, BLOCK_MAP_SIZE);
MAP_DEFINE(log, uint64_t, LOG_MAP_SIZE);
MAP_DEFINE(snapshot, snapshot_t, SNAPSHOT_MAP_SIZE);

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
      snprintf(buf, page_size+1, "%s", (char *)(fs->page_base + (log->first_pn + i)*page_size));
      strcat(res, buf);
      strcat(res, "\n");
    }
    if (log->block_length > log->nr_pages*page_size)
      length = page_size;
    else
      length = log->block_length - (log->nr_pages-1)*page_size;
    sprintf(buf, "Page %" PRIu32 ":\n", log->start_page + log->nr_pages-1);
    strcat(res, buf);
    snprintf(buf, length+1, "%s", (char *)(fs->page_base + (log->first_pn + log->nr_pages-1)*page_size));
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

char *snapshot_to_string(snapshot_t *snapshot, size_t page_size)
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
      snprintf(buf, page_size+2, "%s\n", snapshot->pages[i]);
      strcat(res, buf);
    }
  }
  return res;
}

char *block_to_string(block_t *block, size_t page_size)
{
  char *res = (char *) malloc(1024 * 1024 * sizeof(char));
  char buf[1025];
  snapshot_t *snapshot;
  uint64_t *snapshot_ids, *log_ptr;
  uint32_t i;
  uint64_t j, log_index, map_length;
  
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
    strcat(res, log_to_string(block->log + j, page_size));
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
      snprintf(buf, page_size+1, "%s\n", block->pages[i]);
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
    strcat(res, snapshot_to_string(snapshot, page_size));
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
    strcat(res, block_to_string(block, filesystem->page_size));
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

// Find last log entry that has timestamp less or equal than (r,l).
int find_last_entry(block_t *block, uint64_t r, uint64_t l, uint64_t *last_entry)
{
  log_t *log = block->log;
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
// Find last log entry that has user timestamp less or equal than ut.
void find_last_entry_by_ut(block_t *block, uint64_t ut, uint64_t *last_entry){
  log_t *log = block->log;
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
 * block  - Block object.
 * return  0, if successful,
 *        -1, otherwise.
 */
int check_and_increase_log_cap(block_t *block)
{
  if (block->log_length == block->log_cap) {
    block->log = (log_t *) realloc(block->log, 2*block->log_cap*sizeof(log_t));
    if (block->log == NULL)
    	return -1;
    block->log_cap *= 2;
  }
  return 0;
}

/**
 * Fills the snapshot from the log.
 * @param log_entry - Last log entry for snapshot.
 * @param page_size - Page size for the filesystem.
 * @return Snapshot object.
 */
snapshot_t *fill_snapshot(log_t *log_entry, uint32_t page_size) {
  snapshot_t *snapshot = (snapshot_t *) malloc(sizeof(snapshot_t));
  uint32_t nr_pages, i, cur_page;

  snapshot->ref_count = 0;
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
    nr_pages = (snapshot->length-1)/page_size + 1;
    snapshot->pages = (page_t *) malloc(nr_pages*sizeof(page_t));
    for (i = 0; i < nr_pages; i++)
      snapshot->pages[i] = NULL;
    while (nr_pages > 0) {
      for (i = 0; i < log_entry->nr_pages; i++) {
        cur_page = i + log_entry->start_page;
        if (snapshot->pages[cur_page] == NULL) {
          snapshot->pages[cur_page] = log_entry->pages + i*page_size;
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
  log_t *log = block->log;
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
 	
  // for hlc: If snapshot can include future references do not create it. //TOD //TODO
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
 * blog_writer_routine()
 * PARAM param: the blog writer context
 * RETURN: the 
 */
//TODO: change it!!!
static void * blog_writer_routine(void * param)
{
  filesystem_t *fs = (filesystem_t*) param;
  long time_next_write = 0L;
  struct timeval tv;
  while(fs->alive){
    // STEP 1 - test if a flush is required.
    gettimeofday(&tv,NULL);
    if(time_next_write > tv.tv_sec) {
      usleep(500000l); // wake up every 500ms
      continue;
    } else {
      time_next_write = tv.tv_sec + fs->int_sec;
    }
    // STEP 2 - flush
    // we need to force read lock to make sure it is consistent.
    // read lock prevents log update. Notice that allocating new page
    // or writing new page may undergoing. However that will not harm
    // things because the page under written is irrelevant to visible
    // log entries.
    pthread_rwlock_rdlock(&fs->log_lock);
    if(msync((void*)fs->page_base,*fs->page_nr*fs->page_size,MS_SYNC)!=0){
      fprintf(stderr,"cannot msync() page file,error=%d\n",errno);
      exit(-1);
    }
    if(msync((void*)fs->log_base,((uint64_t)(&fs->log[*fs->log_length])-(uint64_t)fs->log_base),MS_SYNC)!=0){
      fprintf(stderr,"cannot msync() log file,error=%d\n",errno);
      exit(-1);
    }
    pthread_rwlock_unlock(&fs->log_lock);
  }
  pthread_rwlock_rdlock(&fs->log_lock);
  if(msync((void*)fs->page_base,*fs->page_nr*fs->page_size,MS_SYNC)!=0){
    fprintf(stderr,"cannot msync() page file,error=%d\n",errno);
    exit(-1);
  }
  if(msync((void*)fs->log_length,((uint64_t)(&fs->log[*fs->log_length])-(uint64_t)fs->log_length),MS_SYNC)!=0){
    fprintf(stderr,"cannot msync() log file,error=%d\n",errno);
    exit(-1);
  }
  pthread_rwlock_unlock(&fs->log_lock);
  return param;
}

/*
 * open file with size test
 */
// TODO: change it!!!
static void openFileWithSize(const char *filename, 
  uint64_t expected_filesize, uint64_t *poriginal_filesize,
  uint64_t *pcurrent_filesize, uint32_t *pfile_descriptor){
  int fd = 0;
  fd = open(filename, O_RDWR|O_APPEND|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH);
  if(fd==-1){
    fprintf(stderr,"Cannot open or create file:%s, exit...with errno=%d\n",filename,errno);
    exit(-1);
  }
  struct stat fs;
  if(fstat(fd,&fs)!=0){
    fprintf(stderr,"Cannot fstat() on file:%s, exit...with errno=%d\n",filename,errno);
    exit(-1);
  };
  *poriginal_filesize = fs.st_size;
  if(*poriginal_filesize < expected_filesize){
    if(ftruncate(fd,expected_filesize)!=0){
      fprintf(stderr,"Cannot ftruncate() on file:%s, exit...with errno=%d\n",filename,errno);
      exit(-1);
    }
    *pcurrent_filesize = expected_filesize;
  }else
    *pcurrent_filesize = *poriginal_filesize;
  *pfile_descriptor = (uint32_t)fd;
}

/*
 * loadBlog()
 *   we assume that the following members have been initialized already:
 *   - block_size
 *   - page_size
 *   - log_lock
 *   - int_sec
 *   - alive
 *   - env
 *   - blogObj
 *   on success, the following members have been initialized:
 *   - log_length,page_nr
 *   - log_fd,page_fd
 *   - log,page_base
 * PARAM 
 *   fs: file system structure
 *   pp: path to the persistent data.
 * RETURN VALUES: 
 *    0 for succeed. and writer_thrd will be initialized.
 *   -1 for "log file is corrupted", 
 *   -2 for "page file is correupted".
 *   -3 for "block operation".
 */
//TODO: change it!!!
static void loadBlog(filesystem_t *fs, const char *pp)
{
  uint64_t original_filesize,current_filesize;
  // STEP 1 load file: log file
  char * fullpath = (char*) malloc(strlen(pp)+MAX_FNLEN+1);
  sprintf(fullpath,"%s/%s",pp,BLOGFILE);
  openFileWithSize(fullpath,LOG_FILE_SIZE,&original_filesize,&current_filesize,&fs->log_fd);
  fs->log_base = mmap(NULL,current_filesize,PROT_READ|PROT_WRITE,MAP_SHARED,fs->log_fd, 0);
  if(fs->log_base == MAP_FAILED){
    fprintf(stderr,"Cannot mmap() file: %s, exit...with errno=%d\n",fullpath,errno);
    exit(-1);
  }
  fs->log_length = (uint64_t *)fs->log_base;
  fs->page_nr = ((uint64_t *)fs->log_base) + 1;
  fs->log = (log_t*)(((uint64_t*)fs->log_base)+2);
  if(original_filesize == 0){//initialize a new log
    DEBUG_PRINT("loadBlog:start a new blog:%s\n",fullpath);
    *fs->log_length = 0;
    *fs->page_nr = 0;
  }//otherwise, the log is initialized already.
  // STEP 2 load file: page file
  sprintf(fullpath,"%s/%s",pp,PAGEFILE);
  openFileWithSize(fullpath,PAGE_FILE_SIZE,&original_filesize,&current_filesize,&fs->page_fd);
  fs->page_base = mmap(NULL,current_filesize,PROT_READ|PROT_WRITE,MAP_SHARED,fs->page_fd, 0);
  if(fs->page_base == MAP_FAILED){
    fprintf(stderr,"Cannot mmap() file: %s, exit...with errno=%d\n",fullpath,errno);
    exit(-1);
  }
#ifdef PRELOAD_MAPPED_FILE
  uint64_t i;
  for(i=0l;i<(PAGE_FILE_SIZE>>3);i++){
    *((uint64_t*)fs->page_base+i) = 0l;
  }
  for(i=0l;i<(LOG_FILE_SIZE>>3);i++){
    *((uint64_t*)fs->log_base+i) = 0l;
  }
#endif
}

/* Replay the block log of a file system. This is called during 
 * initialization stage. We assume the following fs members are 
 * correctly initialized:
 * - log_length, page_nr
 * - log_base, page_base
 * - log_fd, page_fd
 * - log_map is initialized as empty
 * - block_map is initialized as empty
 * - snapshot_map is initialized as empty
 * On success, 
 * 1) the filesystem->block_map is initialized.
 * 2) the blockMaps member of JNIBlog object is also initialized.
 */
//TODO: change it!!!
static void replayLog(JNIEnv *env, jobject thisObj, filesystem_t *fs){
  long l;
  // STEP 1:
  jclass thisCls = (*env)->GetObjectClass(env, thisObj);
  jmethodID rl_mid = (*env)->GetMethodID(env, thisCls, "replayLogOnMetadata", "(JIJ)V");
  block_t *block;
  int i,pages_cap;
  // replay the log
  for(l=0;l<*fs->log_length;l++){
    DEBUG_PRINT("replay_1:op=%d\n",fs->log[l].op);
    //STEP 1: replay the log for fs->block_map
    switch(fs->log[l].op){
    case CREATE_BLOCK:
      if(MAP_CREATE(block,fs->block_map,fs->log[l].block_id) != 0){
        fprintf(stderr, "FATAL: cannot replay log entry[%ld]: create1 block %ld\n",l,fs->log[l].block_id);
        exit(-1);
      }
      block = (block_t *)malloc(sizeof(block_t));
      block->log_index = l;
      block->length = 0;
      block->pages_cap = 0;
      block->pages = NULL;
      if(MAP_WRITE(block, fs->block_map, fs->log[l].block_id, block) != 0){
        fprintf(stderr, "FATAL: cannot replay log entry[%ld]: create2 block %ld\n",l,fs->log[l].block_id);
        exit(-1);
      }
      break;
    case DELETE_BLOCK:
      if(MAP_READ(block, fs->block_map, fs->log[l].block_id, &block)==-1){
        fprintf(stderr, "WARNING: cannot replay log entry[%ld]: delete1 block %ld\n",l,fs->log[l].block_id);
      }else{
        if(MAP_DELETE(block, fs->block_map, fs->log[l].block_id)!=0){
          fprintf(stderr, "FATAL: cannot replay log entry[%ld]: delete2 block %ld\n",l,fs->log[l].block_id);
          exit(-1);
        }
      }
      break;
    case WRITE:
      if (MAP_READ(block, fs->block_map, fs->log[l].block_id, &block) == -1){
          fprintf(stderr, "FATAL: cannot replay log entry[%ld]: write1 block %ld\n",l,fs->log[l].block_id);
          exit(-1);
      }
      // update block info
      block->length = fs->log[l].block_length;
      pages_cap = block->pages_cap;
      if(block->pages_cap == 0)pages_cap = 1;
      while((block->length -1)/fs->page_size >= pages_cap)pages_cap*=2;
      if(pages_cap > block->pages_cap){
        block->pages_cap = pages_cap;
        block->pages = (page_t*) realloc(block->pages, block->pages_cap * sizeof(page_t));
      }
      for(i = 0;i < fs->log[l].nr_pages; i++)
        block->pages[fs->log[l].page_id+i] = (page_t)(fs->page_base + (fs->log[l].first_pn + i) * fs->page_size);
      break;
    case BOL:
    case SET_GENSTAMP:
    default:
      break;// do nothing
    }
    //STEP 2: replay the log for JNIBlog.blockMaps
    DEBUG_PRINT("replay_2:op=%d,block_id=%ld,first_pn/genStamp=%ld\n",fs->log[l].op,fs->log[l].block_id,fs->log[l].first_pn);
    (*env)->CallObjectMethod(env,thisObj,rl_mid,fs->log[l].block_id,fs->log[l].op,fs->log[l].first_pn);
    DEBUG_PRINT("replay_2:op=%d...done\n",fs->log[l].op);
  }
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    initialize
 * Signature: (II)I
 */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_initialize
  (JNIEnv *env, jobject thisObj, jint blockSize, jint pageSize, jstring persPath)
{
DEBUG_PRINT("initialize is called\n");
  const char * pp = (*env)->GetStringUTFChars(env,persPath,NULL); // get the presistent path
  jclass thisCls = (*env)->GetObjectClass(env, thisObj);
  jfieldID long_id = (*env)->GetFieldID(env, thisCls, "jniData", "J");
  jfieldID hlc_id = (*env)->GetFieldID(env, thisCls, "hlc", "Ledu/cornell/cs/sa/HybridLogicalClock;");
  jclass hlc_class = (*env)->FindClass(env, "edu/cornell/cs/sa/HybridLogicalClock");
  jmethodID cid = (*env)->GetMethodID(env, hlc_class, "<init>", "()V");
  jobject hlc_object = (*env)->NewObject(env, hlc_class, cid);
  filesystem_t *filesystem;
  
  filesystem = (filesystem_t *) malloc (sizeof(filesystem_t));
  if (filesystem == NULL) {
    perror("Error");
    exit(1);
  }

  filesystem->block_size = blockSize;
  filesystem->page_size = pageSize;
/* TODO: change it !!!
  loadBlog(filesystem,pp);
  if(*filesystem->log_length == 0){//create a BOL log for new blog
    filesystem->log[0].op = BOL;
    filesystem->log[0].r = 0;
    filesystem->log[0].l = 0;
    *filesystem->log_length = 1;
  }
  
  filesystem->log_map = MAP_INITIALIZE(log);
  if (filesystem->log_map == NULL) {
    fprintf(stderr, "ERROR: Allocation of log_map failed.\n");
    exit(-1);
  }
  filesystem->snapshot_map = MAP_INITIALIZE(snapshot);
  if (filesystem->snapshot_map == NULL) {
    fprintf(stderr, "ERROR: Allocation of snapshot_map failed.\n");
    exit(-1);
  }
*/
  filesystem->block_map = MAP_INITIALIZE(block);
  if (filesystem->block_map == NULL) {
    fprintf(stderr, "ERROR: Allocation of block_map failed.\n");
    exit(-1);
  }

  (*env)->SetObjectField(env, thisObj, hlc_id, hlc_object);
  (*env)->SetLongField(env, thisObj, long_id, (uint64_t) filesystem);

  //TODO: change it!!!
  replayLog(env, thisObj, filesystem);

  // start the write thread.
  filesystem->int_sec = FLUSH_INTERVAL_SEC;
  filesystem->alive = 1; // alive.
  
  //start blog writer thread
  if (pthread_create(&filesystem->writer_thrd, NULL, blog_writer_routine, (void*)filesystem)) {
    fprintf(stderr,"CANNOT create blogWriter thread, exit\n");
    exit(-1);
  }

  (*env)->ReleaseStringUTFChars(env, persPath, pp);
DEBUG_PRINT("initialize() done\n");
  return 0;
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
  
  // Ass the first two entries.
  log_entry = block->log;
  log_entry->op = BOL;
  log_entry->block_length = 0;
  log_entry->start_page = 0;
  log_entry->nr_pages = 0;
  log_entry->r = 0;
  log_entry->l = 0;
  log_entry->u = 0;
  log_entry->pages = NULL;
  log_entry++;
  log_entry->op = CREATE_BLOCK;
  log_entry->block_length = 0;
  log_entry->start_page = 0;
  log_entry->nr_pages = 0;
  tick_hybrid_logical_clock(env, get_hybrid_logical_clock(env, thisObj), mhlc);
  update_log_clock(env,mhlc,log_entry);
  log_entry->u = 0;
  log_entry->pages = NULL;
  
  // Update current state.
  block->status = ACTIVE;
  block->length = 0;
  block->pages_cap = 0;
  block->pages = NULL;
  
  // Create snapshot structures.
  block->log_map_hlc = MAP_INITIALIZE(log);
  block->log_map_ut = MAP_INITIALIZE(log);
  block->snapshot_map = MAP_INITIALIZE(snapshot);
  
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
 * estimate the user's timestamp where it is not avaialbe.
 * For deleteBlock(), we need this.
 * we assume that u = K*r; where K is a linear coefficient.
 * so u2 = u1/r1*r2
 */
long estimate_user_timestamp(block_t *block, long r){
  log_t *le = block->log+block->log_length-1;

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
  check_and_increase_log_cap(block);
  log_entry = block->log + block->log_length;
  log_entry->op = DELETE_BLOCK;
  log_entry->block_length = 0;
  log_entry->start_page = 0;
  log_entry->nr_pages = 0;
  tick_hybrid_logical_clock(env, get_hybrid_logical_clock(env, thisObj), mhlc);
  update_log_clock(env, mhlc, log_entry);
  log_entry->u = estimate_user_timestamp(block,log_entry->r);
  log_entry->pages = NULL;
  
  // Release the current state of the block.
  block->status = NON_ACTIVE;
  block->length = 0;
  if (block->pages_cap > 0)
    free(block->pages);
  block->pages_cap = 0;
  block->pages = NULL;
  return 0;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readBlock
 * Signature: (JJJIII[B)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_readBlock__JIII_3B
  (JNIEnv *env, jobject thisObj, jlong blockId, jint blkOfst, jint bufOfst, jint length, jbyteArray buf)
{
  DEBUG_PRINT("begin readBlock__JIII_3B.\n");
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  snapshot_t *snapshot;
  block_t *block;
  log_t *log_entry;
  uint64_t block_id = (uint64_t) blockId;
  uint32_t block_offset = (uint32_t) blkOfst;
  uint32_t buffer_offset = (uint32_t) bufOfst;
  uint32_t read_length = (uint32_t) length;
  uint64_t log_index;
  uint32_t cur_length, page_id, page_offset;
  char *page_data;
  
  // Find the block.
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) != 0 || block->status != ACTIVE) {
    fprintf(stderr, "WARNING: Block with id %" PRIu64 " has never been active\n", block_id);
    return -1;
  }
  log_index = block->log_length-1;
  MAP_UNLOCK(block, filesystem->block_map, block_id);
  
  // Find if snapshot exists for this log index.
  log_entry = block->log + log_index;
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
  
  // Fill the buffer.
  page_id = block_offset / filesystem->page_size;
  page_offset = block_offset % filesystem->page_size;
  page_data = snapshot->pages[page_id];
  page_data += page_offset;
  cur_length = filesystem->page_size - page_offset;
  if (cur_length >= read_length) {
    (*env)->SetByteArrayRegion(env, buf, buffer_offset, read_length, (jbyte*) page_data);
  } else {
  	(*env)->SetByteArrayRegion(env, buf, buffer_offset, cur_length, (jbyte*) page_data);
  	page_id++;
  	while (1) {
  		page_data = snapshot->pages[page_id];
  		if (cur_length + filesystem->page_size >= read_length) {
        (*env)->SetByteArrayRegion(env, buf, buffer_offset + cur_length, read_length - cur_length, (jbyte*) page_data);
        break;
      }
      (*env)->SetByteArrayRegion(env, buf, buffer_offset + cur_length, filesystem->page_size, (jbyte*) page_data);
      cur_length += filesystem->page_size;
      page_id++;
    }
  }
  DEBUG_PRINT("end readBlock__JIII_3B.\n");
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
DEBUG_PRINT("begin readBlock_JJIII.\n");
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  snapshot_t *snapshot;
  block_t *block;
  uint64_t snapshot_time = (uint64_t) t;
  uint64_t block_id = (uint64_t) blockId;
  uint32_t block_offset = (uint32_t) blkOfst;
  uint32_t buffer_offset = (uint32_t) bufOfst;
  uint32_t read_length = (uint32_t) length;
//  uint64_t *log_ptr;
//  uint64_t log_index;
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
  if (find_or_create_snapshot(block, snapshot_time, filesystem->page_size, &snapshot, byUserTimestamp==JNI_TRUE) < 0) {
      fprintf(stderr, "WARNING: Snapshot for time %" PRIu64 " cannot be created.\n", snapshot_time);
      return -1;
  }
  
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
  page_id = block_offset / filesystem->page_size;
  page_offset = block_offset % filesystem->page_size;
  page_data = snapshot->pages[page_id];
  page_data += page_offset;
  cur_length = filesystem->page_size - page_offset;
  if (cur_length >= read_length) {
    (*env)->SetByteArrayRegion(env, buf, buffer_offset, read_length, (jbyte*) page_data);
  } else {
  	(*env)->SetByteArrayRegion(env, buf, buffer_offset, cur_length, (jbyte*) page_data);
  	page_id++;
  	while (1) {
  		page_data = snapshot->pages[page_id];
  		if (cur_length + filesystem->page_size >= read_length) {
        (*env)->SetByteArrayRegion(env, buf, buffer_offset + cur_length, read_length - cur_length, (jbyte*) page_data);
        break;
      }
      (*env)->SetByteArrayRegion(env, buf, buffer_offset + cur_length, filesystem->page_size, (jbyte*) page_data);
      cur_length += filesystem->page_size;
      page_id++;
    }
  }
DEBUG_PRINT("end readBlock_JJIII.\n");
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
DEBUG_PRINT("begin getNumberOfBytes__J.\n");
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  block_t *block;
  uint64_t log_index;
  uint64_t block_id = (uint64_t) blockId;
  
  // Find the block.
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) != 0  || block->status != ACTIVE) {
    fprintf(stderr, "WARNING: Block with id %" PRIu64 " has never been active\n", block_id);
    return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);
  
  // Find the last entry.
  log_index = block->log_length-1;
  
  return (jint) block->log[log_index].block_length;
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
  if (find_or_create_snapshot(block, snapshot_time, filesystem->page_size, &snapshot, by_ut==JNI_TRUE) < 0) {
      fprintf(stderr, "WARNING: Snapshot for time %" PRIu64 " cannot be created.\n", snapshot_time);
      return -2;
  }
  
  // In case the block does not exist return an error.
  if (snapshot->status == NON_ACTIVE) {
      fprintf(stderr, "WARNING: Block with id %" PRIu64 " is not active at snapshot with rtc %" PRIu64 ".\n", block_id,
              snapshot_time);
      return -1;
  }
  
  return (jint) snapshot->length;
}

inline void * blog_allocate_pages(filesystem_t *fs,int npage){
  void *pages = NULL;
  pthread_mutex_lock(&fs->page_lock);
  if(*fs->page_nr + npage <= (PAGE_FILE_SIZE/fs->page_size)){
    pages = fs->page_base + fs->page_size**fs->page_nr;
    (*fs->page_nr) += npage;
  }else
    fprintf(stderr,"Cannot allocate pages: ask for %d, but we have only %ld\n",npage,(PAGE_FILE_SIZE/fs->page_size - *fs->page_nr));
  pthread_mutex_unlock(&fs->page_lock);
  return pages;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    writeBlock
 * Signature: (Ledu/cornell/cs/sa/VectorClock;JIII[B)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_writeBlock
  (JNIEnv *env, jobject thisObj, jobject mhlc, jlong userTimestamp, jlong blockId, jint blkOfst,
  jint bufOfst, jint length, jbyteArray buf)
{
DEBUG_PRINT("beging writeBlock.\n");
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
  //data = (char*) malloc((last_page-first_page+1) * page_size * sizeof(char));
  // check this part:
  data = (char*)blog_allocate_pages(filesystem,last_page - first_page + 1);
  temp_data = data;
  if (first_page_offset > 0) {
    memcpy(temp_data, block->pages[first_page], first_page_offset);
    temp_data += first_page_offset;
  }
  
  // Write all the data from the packet.
  if (first_page == last_page) {
    (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_length, (jbyte *) temp_data);
    temp_data += write_length;
    last_page_length = first_page_offset + write_length;
  } else {
    write_page_length = filesystem->page_size - first_page_offset;
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
  block_length = last_page * filesystem->page_size + last_page_length;
  if (block_length < block->length) {
    write_page_length = last_page*(page_size+1) <= block->length ? page_size - last_page_length
                                                                 : block_length%page_size - last_page_length;
    memcpy(temp_data, block->pages[last_page] + last_page_length, write_page_length);
    block_length = block->length;
  }
  // Create the corresponding log entry.
  log_index = block->log_length;
  check_and_increase_log_cap(block);
  log_entry = block->log + log_index;
  log_entry->op = WRITE;
  log_entry->block_length = block_length;
  log_entry->start_page = first_page;
  log_entry->nr_pages = last_page-first_page+1;
  tick_hybrid_logical_clock(env, get_hybrid_logical_clock(env, thisObj), mhlc);
  update_log_clock(env, mhlc, log_entry);
  log_entry->u = userTimestamp;
  //TODO: change pages to index...
  log_entry->pages = data;
  block->log_length += 1;

  // Fill block with the appropriate information.
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
    block->pages[log_entry->start_page+i] = data + i * filesystem->page_size;
DEBUG_PRINT("end writeBlock.\n");
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
DEBUG_PRINT("beging destroy.\n");
  //TODO: release all memory data? currently we leave it for OS.
  //TODO: recheck this part for the destroy.
/*
  // kill blog writer
  filesystem_t *fs;
  void * ret;
  
  fs = get_filesystem(env,thisObj);
  fs->alive = 0;
  if(pthread_join(fs->writer_thrd, &ret))
    fprintf(stderr,"waiting for blogWriter thread error...disk data may be corrupted\n");

  // close files
  if(fs->page_fd!=-1){
    munmap(fs->page_base,(size_t)fs->page_size*(*fs->page_nr));
    close(fs->log_fd);
    fs->page_fd=-1;
  }
  if(fs->log_fd!=-1){
    munmap(fs->log_base,(size_t)((void*)&fs->log[*fs->log_length] - fs->log_base));
    close(fs->log_fd);
    fs->log_fd=-1;
  }
*/
DEBUG_PRINT("end destroy.\n");
}

/**
 * set generation stamp a block. // this is for Datanode.
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_setGenStamp
  (JNIEnv *env, jobject thisObj, jobject mhlc, jlong blockId, jlong genStamp){
/* TODO: reimplement this part for Multi blog.
  filesystem_t *filesystem;
  uint64_t block_id = (uint64_t)blockId, log_index;
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
  pthread_rwlock_wrlock(&(filesystem->log_lock));
  log_index = *filesystem->log_length;
  tick_hybrid_logical_clock(env, get_hybrid_logical_clock(env, thisObj), mhlc);
  log_entry = filesystem->log + log_index;
  log_entry->block_id = block_id;
  log_entry->block_length = block->length;
  log_entry->op = SET_GENSTAMP;
  log_entry->first_pn = (uint64_t)genStamp;
  log_entry->nr_pages = 0;
  log_entry->previous = block->log_index;
  update_log_clock(env, mhlc, log_entry);
  (*filesystem->log_length) += 1;
  pthread_rwlock_unlock(&(filesystem->log_lock));
  //STEP 3: update block;
  block->log_index = log_index;
*/
  return 0;
}
