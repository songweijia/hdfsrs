#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

#include "JNIBlog.h"
#include "types.h"

MAP_DEFINE(block, block_t, BLOCK_MAP_SIZE);
MAP_DEFINE(log, int64_t, LOG_MAP_SIZE);
MAP_DEFINE(snapshot, snapshot_t, SNAPSHOT_MAP_SIZE);

void print_page(page_t *page)
{
  printf("%s\n", page->data);
}

void print_log(JNIEnv *env, log_t *log, int64_t index)
{
  jclass vc_class = (*env)->FindClass(env, "edu/cornell/cs/sa/VectorClock");
  jmethodID mid = (*env)->GetStaticMethodID(env, vc_class, "toString", "([B)Ljava/lang/String;");
  jbyteArray jbyteArray;
  jstring jstring;
  log_t *cur_log = log + index;
  const char *vc_string;
  int i;
  
  if (mid == NULL) {
    perror("Error: ");
    exit(-1);
  }
  jbyteArray = (*env)->NewByteArray(env, cur_log->vc_length);
  (*env)->SetByteArrayRegion (env, jbyteArray, 0, cur_log->vc_length, (jbyte *) cur_log->vc);
  jstring = (*env)->CallStaticObjectMethod(env, vc_class, mid, jbyteArray);
  vc_string = (*env)->GetStringUTFChars(env, jstring, NULL);
  
  printf("Block ID: %ld\n", cur_log->block_id);
  printf("Block Length: %d\n", cur_log->block_length);
  printf("Starting Page: %d\n", cur_log->page_id);
  printf("Number of Pages: %d\n", cur_log->nr_pages);
  for (i = 0; i < cur_log->nr_pages; i++) {
    printf("Page %d:\n", cur_log->page_id + i);
    print_page(cur_log->pages + i);
  }
  printf("RTC Value: %ld\n", cur_log->rtc);
  printf("VC Value: %s\n", vc_string);
  if (cur_log->previous != -1)
    printf("Previous: %ld\n", cur_log->previous);
}

void print_block(block_t *block, int page_size)
{
  int i = 0;

  printf("Length: %d\n", block->length);
  printf("Capacity: %d\n", block->cap);
  if (block->length != 0) {
    for (i = 0; i <= (block->length - 1) / page_size; i++) {
      printf("Page %d\n", i);
      print_page(block->pages[i]);
    }
  }
  printf("Last Log Entry: %ld\n", block->last_entry);
}

void print_snapshot(snapshot_t *snapshot, log_t *log, int page_size)
{
  block_t *block;
  int64_t *ids;
  int64_t length, i;
  
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

void print_filesystem(JNIEnv *env, filesystem_t *filesystem)
{
  block_t *block;
  snapshot_t *snapshot;
  int64_t *ids;
  int64_t length, i;
  int64_t *log_id;
  
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
    printf("RTC: %ld\n", ids[i]);
    printf("Last Log Entry: %ld\n", *log_id);
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
    printf("Log %ld\n", i);
    print_log(env, filesystem->log, i);
    printf("\n");
  }
  
  pthread_rwlock_unlock(&filesystem->lock);
}

block_t *find_or_allocate_snapshot_block(filesystem_t *filesystem, snapshot_t *snapshot, int64_t last_log_entry,
                                         int64_t block_id) {
  log_t *log = filesystem->log;
  log_t *cur_log;
  block_t *block;
  int64_t log_id;
  int i, nr_unfilled_pages;
  
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
  if (log_id == -1 || log[log_id].page_id == -2) {
    block->length = 0;
    block->cap = -1;
    block->pages = NULL;
    block->last_entry = -1;
  } else if (log[log_id].page_id == -1) {
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
      for (i = 0; i < cur_log->nr_pages; i++) {
        if (block->pages[cur_log->page_id + i] == NULL) {
          nr_unfilled_pages--;
          block->pages[cur_log->page_id + i] = cur_log->pages + i;
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

void check_and_increase_log_length(filesystem_t *filesystem)
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

int64_t read_local_rtc()
{
  int64_t rtc = 0;
  struct timeval tv;
  
  gettimeofday(&tv,NULL);
  rtc = tv.tv_sec;
//  rtc = (rtc<<20) | tv.tv_usec;
  rtc = tv.tv_sec*1000 + tv.tv_usec/1000;
  return rtc;
}

void tick_vector_clock(JNIEnv *env, jobject thisObj, jobject mvc, log_t* log)
{
  jclass thisClass = (*env)->GetObjectClass(env, thisObj);
  jfieldID vc_id = (*env)->GetFieldID(env, thisClass, "vc", "Ledu/cornell/cs/sa/VectorClock;");
  jobject vc = (*env)->GetObjectField(env, thisObj, vc_id);
  jclass vc_class = (*env)->GetObjectClass(env, vc);
  jmethodID mid1, mid2;
  jbyteArray jbyteArray;
  jbyte* jbytePointer;
  
  mid1 = (*env)->GetMethodID(env, vc_class, "tickOnRecv",
                             "(Ledu/cornell/cs/sa/ILogicalClock;)Ledu/cornell/cs/sa/ILogicalClock;");
  if (mid1 == NULL) {
    perror("Error");
    exit(-1);
  }
  mid2 = (*env)->GetMethodID(env, vc_class, "toByteArrayNoPid", "()[B");
  if (mid2 == NULL) {
    perror("Error");
    exit(-1);
  }
  vc = (*env)->CallObjectMethod(env, vc, mid1, mvc);
  jbyteArray = (*env)->CallObjectMethod(env, vc, mid2, NULL);
  log->vc_length = (*env)->GetArrayLength(env, jbyteArray);
  log->vc = (char *) malloc(log->vc_length*sizeof(char));
  (*env)->GetByteArrayRegion (env, jbyteArray, 0, log->vc_length, (jbyte *) log->vc);
}

void set_vector_clock(JNIEnv *env, jobject thisObj, size_t vc_length, char* vc)
{
  jclass thisClass = (*env)->GetObjectClass(env, thisObj);
  jmethodID mid = (*env)->GetMethodID(env, thisClass, "fromByteArrayNoPid", "([B)V");
  jbyteArray jbyteArray;
  
  if (mid == NULL) {
    perror("Error: ");
    exit(-1);
  }
  jbyteArray = (*env)->NewByteArray(env, vc_length);
  (*env)->SetByteArrayRegion (env, jbyteArray, 0, vc_length, (jbyte *) vc);
  (*env)->CallObjectMethod(env, thisObj, mid, jbyteArray);
}

int64_t get_vector_clock_value(JNIEnv *env, jobject vcObj, size_t vc_length, char* vc, int rank)
{
  jclass thisClass = (*env)->GetObjectClass(env, vcObj);
  jmethodID mid = (*env)->GetMethodID(env, thisClass, "fromByteArrayNoPid", "([B)V");
  jmethodID mid2 = (*env)->GetMethodID(env, thisClass, "GetVectorClockValue", "(I)J");
  jbyteArray jbyteArray;
  
  if (mid == NULL || mid2 == NULL) {
    perror("Error: ");
    exit(-1);
  }
  jbyteArray = (*env)->NewByteArray(env, vc_length);
  (*env)->SetByteArrayRegion (env, jbyteArray, 0, vc_length, (jbyte *) vc);
  (*env)->CallObjectMethod(env, vcObj, mid, jbyteArray);
  
  return (int64_t) (*env)->CallObjectMethod(env, vcObj, mid2, rank);
}


filesystem_t *get_filesystem(JNIEnv *env, jobject thisObj)
{
  jclass thisCls = (*env)->GetObjectClass(env,thisObj);
  jfieldID fid = (*env)->GetFieldID(env,thisCls,"jniData","J");
  
  return (filesystem_t *) (*env)->GetLongField(env, thisObj, fid);
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    initialize
 * Signature: (III)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_initialize
  (JNIEnv *env, jobject thisObj, jint rank, jint blockSize, jint pageSize)
{
  jclass thisCls = (*env)->GetObjectClass(env, thisObj);
  jfieldID long_id = (*env)->GetFieldID(env, thisCls, "jniData", "J");
  jfieldID vc_id = (*env)->GetFieldID(env, thisCls, "vc", "Ledu/cornell/cs/sa/VectorClock;");
  jclass vc_class = (*env)->FindClass(env, "edu/cornell/cs/sa/VectorClock");
  jmethodID cid = (*env)->GetMethodID(env, vc_class, "<init>", "(I)V");
  jobject vc_object = (*env)->NewObject(env, vc_class, cid, rank);
  filesystem_t *filesystem;
  int i;
  
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
    return -1;
  }
  filesystem->log_map = MAP_INITIALIZE(log);
  if (filesystem->log_map == NULL) {
    fprintf(stderr, "Initialize: Allocation of log_map failed.\n");
    return -1;
  }
  filesystem->snapshot_map = MAP_INITIALIZE(snapshot);
  if (filesystem->snapshot_map == NULL) {
    fprintf(stderr, "Initialize: Allocation of snapshot_map failed.\n");
    return -1;
  }
  filesystem->log_cap = 1024;
  filesystem->log_length = 0;
  filesystem->log = (log_t *) malloc (1024*sizeof(log_t));
  pthread_rwlock_init(&(filesystem->lock), NULL);
  
  (*env)->SetObjectField(env, thisObj, vc_id, vc_object);
  (*env)->SetLongField(env, thisObj, long_id, (int64_t) filesystem);
  
  return 0;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    createBlock
 * Signature: (Ledu/cornell/cs/sa/VectorClock;J)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_createBlock
  (JNIEnv *env, jobject thisObj, jobject mvc, jlong blockId)
{
  filesystem_t *filesystem;
  block_t *new_block;
  int64_t log_pos;
  
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
  filesystem->log[log_pos].page_id = -1;
  filesystem->log[log_pos].nr_pages = 0;
  filesystem->log[log_pos].pages = NULL;
  filesystem->log[log_pos].previous = -1;
  filesystem->log[log_pos].rtc = read_local_rtc();
  tick_vector_clock(env, thisObj, mvc, filesystem->log+log_pos);
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
  new_block = filesystem->block_map[blockId%BLOCK_MAP_SIZE].entry->value;
  return 0;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    deleteBlock
 * Signature: (Ledu/cornell/cs/sa/VectorClock;J)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_deleteBlock
  (JNIEnv *env, jobject thisObj, jobject mvc, jlong blockId)
{
  filesystem_t *filesystem;
  int block_pos;
  block_t *cur_block;
  block_t *last_block = NULL;
  block_t *new_block;
  int64_t log_pos;
  
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
  filesystem->log[log_pos].page_id = -2;
  filesystem->log[log_pos].nr_pages = 0;
  filesystem->log[log_pos].pages = NULL;
  filesystem->log[log_pos].previous = cur_block->last_entry;
  filesystem->log[log_pos].rtc = read_local_rtc();
  tick_vector_clock(env, thisObj, mvc, filesystem->log+log_pos);
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
  int page_id, page_offset, read_length, cur_length;
  char *page_data;
  int64_t *log_id;
  
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
    return read_length;
  }
  (*env)->SetByteArrayRegion(env, buf, bufOfst, cur_length, (jbyte*) page_data);
  page_id++;
  while (1) {
    page_data = block->pages[page_id]->data;
    if (cur_length + filesystem->page_size >= read_length) {
        (*env)->SetByteArrayRegion(env, buf, bufOfst + cur_length, read_length - cur_length, (jbyte*) page_data);
        return read_length;
    }
    (*env)->SetByteArrayRegion(env, buf, bufOfst + cur_length, filesystem->page_size, (jbyte*) page_data);
    cur_length += filesystem->page_size;
    page_id++;
  }
  
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
  int64_t *log_id;
  
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
  (JNIEnv *env, jobject thisObj, jobject mvc, jlong blockId, jint blkOfst,
  jint bufOfst, jint length, jbyteArray buf)
{
  filesystem_t *filesystem;
  block_t *block;
  page_t *new_pages;
  int first_page, last_page;
  int page_offset, block_offset, buffer_offset;
  int new_pages_length, new_pages_capacity, write_length, last_page_length;
  int64_t log_pos;
  char *pdata;
  int i;
  
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
  new_pages = (page_t*) malloc(new_pages_length*sizeof(page_t));
  new_pages[0].data = (char *) malloc(filesystem->page_size*sizeof(char));
  for (i = 0; i < page_offset; i++)
    new_pages[0].data[i] = block->pages[first_page]->data[i];
  if (first_page == last_page) {
    write_length = (jint) length;
    pdata = new_pages[0].data + page_offset;
#ifdef SECOND_EXPERIMENT
#else    
    (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_length, (jbyte *) pdata);
#endif
    last_page_length = page_offset + write_length;
  } else {
    write_length = filesystem->page_size - page_offset;
    pdata = new_pages[0].data + page_offset;
#ifdef SECOND_EXPERIMENT
#else
    (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_length, (jbyte *) pdata);
#endif
    buffer_offset += write_length;
    for (i = 1; i < new_pages_length-1; i++) {
      new_pages[i].data = (char *) malloc(filesystem->page_size*sizeof(char));
      write_length = filesystem->page_size;
      pdata = new_pages[i].data;
#ifdef SECOND_EXPERIMENT
#else
      (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_length, (jbyte *) pdata);
#endif
      buffer_offset += write_length;
    }
    new_pages[new_pages_length-1].data =  (char *) malloc(filesystem->page_size*sizeof(char));
    write_length = (int) bufOfst + (int) length - buffer_offset;
    pdata = new_pages[new_pages_length-1].data;
#ifdef SECOND_EXPERIMENT
#else
    (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_length, (jbyte *) pdata);
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
  filesystem->log[log_pos].page_id = first_page;
  filesystem->log[log_pos].nr_pages = new_pages_length;
  filesystem->log[log_pos].pages = new_pages;
  filesystem->log[log_pos].previous = block->last_entry;
  filesystem->log[log_pos].rtc = read_local_rtc();
#ifdef SECOND_EXPERIMENT
#else
#ifdef THIRD_EXPERIMENT
#else
  tick_vector_clock(env, thisObj, mvc, filesystem->log+log_pos);
#endif
#endif
  filesystem->log_length += 1;
  pthread_rwlock_unlock(&(filesystem->lock));
  block->last_entry = log_pos;
#endif
  return 0;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    since
 * Signature: (JLedu/cornell/cs/sa/VectorClock;)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_since__JLedu_cornell_cs_sa_VectorClock_2
  (JNIEnv *env, jobject thisObj, jlong rtc, jobject mvc)
{
  filesystem_t *filesystem;
  log_t *log;
  int64_t log_pos;
  int64_t cur_value;
  
  // Find the corresponding log.
  filesystem = get_filesystem(env, thisObj);
  if(filesystem->log_length == 0)
    return -1;
  log = filesystem->log;
  // If the real time cut is before the first log...
  if (log[0].rtc > rtc)
    return -1;
  // If the real time cut is after the last log...
  log_pos = filesystem->log_length-1;
  if (log[log_pos].rtc <= rtc) {
    set_vector_clock(env, mvc, log[log_pos].vc_length, log[log_pos].vc);
    return 0;
  }
  
  log_pos = filesystem->log_length / 2;
  cur_value = log_pos / 2;
  while ((log[log_pos].rtc > rtc) || (log[log_pos+1].rtc <= rtc)) {
    if (log[log_pos].rtc > rtc)
      log_pos -= cur_value;
    else
      log_pos += cur_value;
    if (cur_value > 1)
      cur_value /= 2;
  }
  set_vector_clock(env, mvc, log[log_pos].vc_length, log[log_pos].vc);
  return 0;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    since
 * Signature: (JIJLedu/cornell/cs/sa/VectorClock;)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_since__JIJLedu_cornell_cs_sa_VectorClock_2
  (JNIEnv *env, jobject thisObj, jlong rtc, jint rank, jlong lcv, jobject mvc)

{
  filesystem_t *filesystem;
  log_t *log;
  int64_t log_pos;
  int64_t cur_value;
  
  // Find the corresponding log.
  filesystem = get_filesystem(env, thisObj);
  if(filesystem->log_length == 0)
    return -1;
  log = filesystem->log;
  // If the real time cut is before the first log...
  if (log[0].rtc > rtc)
    return -1;
  // If the real time cut is after the last log...
  log_pos = filesystem->log_length-1;
  if (log[log_pos].rtc <= rtc) {
    while ((log_pos >= 0) && (get_vector_clock_value(env, mvc, log[log_pos].vc_length, log[log_pos].vc, rank) > lcv))
      log_pos--;
    if (log_pos == -1)
      return -1;
    set_vector_clock(env, mvc, log[log_pos].vc_length, log[log_pos].vc);
    return 0;
  }
  
  log_pos = filesystem->log_length / 2;
  cur_value = log_pos / 2;
  while ((log[log_pos].rtc > rtc) || (log[log_pos+1].rtc <= rtc)) {
    if (log[log_pos].rtc > rtc)
      log_pos -= cur_value;
    else
      log_pos += cur_value;
    if (cur_value > 1)
      cur_value /= 2;
  }
  while ((log_pos >= 0) && (get_vector_clock_value(env, mvc, log[log_pos].vc_length, log[log_pos].vc, rank) > lcv))
      log_pos--;
  if (log_pos == -1)
      return -1;
  set_vector_clock(env, mvc, log[log_pos].vc_length, log[log_pos].vc);
  return 0;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    createSnapshot
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_createSnapshot
  (JNIEnv *env, jobject thisObj, jlong rtc, jlong eid)
{
  filesystem_t *filesystem;
  snapshot_t *snapshot;
  snapshot_t *last_snapshot = NULL;
  int64_t id = (int64_t) rtc;
  int64_t *new_id;
  int i;
  
  // Find the corresponding position to write snapshot.
  filesystem = get_filesystem(env, thisObj);
  MAP_LOCK(log, filesystem->log_map, rtc, 'w');
  if (MAP_CREATE(log, filesystem->log_map, rtc) != 0) {
    MAP_UNLOCK(log, filesystem->log_map, rtc);
    fprintf(stderr, "Create Snapshot: Snapshot %ld already exists\n", rtc);
    return -1;
  }
  new_id = (int64_t *) malloc(sizeof(int64_t));
  *new_id = eid;
  if (MAP_WRITE(log, filesystem->log_map, rtc, new_id)) {
    MAP_UNLOCK(log, filesystem->log_map, rtc);
    fprintf(stderr, "Create Snapshot: Something is wrong with LOG_MAP_CREATE or LOG_MAP_WRITE\n");
    return -2;
  }
  MAP_LOCK(snapshot, filesystem->snapshot_map, eid, 'w');
  MAP_UNLOCK(log, filesystem->log_map, rtc);
  
  if (MAP_CREATE(snapshot, filesystem->snapshot_map, eid) != 0) {
    if (MAP_READ(snapshot, filesystem->snapshot_map, eid, &snapshot) != 0) {
      MAP_UNLOCK(snapshot, filesystem->snapshot_map, eid);
      fprintf(stderr, "Create Snapshot: Something is wrong with SNAPSHOT_MAP_CREATE or SNAPSHOT_MAP_READ\n");
    }
    snapshot->ref_count++;
    MAP_UNLOCK(snapshot, filesystem->snapshot_map, eid);
    return 0;
  }
    
  snapshot = (snapshot_t*) malloc(sizeof(snapshot_t));
  snapshot->block_map = MAP_INITIALIZE(block);
  if (snapshot->block_map == NULL) {
    fprintf(stderr, "Create Snapshot: Allocation of snapshot block map failed.\n");
    return -1;
  }
  snapshot->ref_count = 1;
  if (MAP_WRITE(snapshot, filesystem->snapshot_map, eid, snapshot)) {
    MAP_UNLOCK(snapshot, filesystem->snapshot_map, eid);
    fprintf(stderr, "Create Snapshot: Something is wrong with SNAPSHOT_MAP_CREATE or SNAPSHOT_MAP_WRITE\n");
    return -2;
  }
  MAP_UNLOCK(snapshot, filesystem->snapshot_map, eid);
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
