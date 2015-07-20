#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

#include "JNIBlog.h"
#include "types.h"

#define BLOCK_MAP_SIZE 4096
#define SNAPSHOT_MAP_SIZE 64

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

  printf("ID: %ld\n", block->id);
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
  int i = 0;
  
  printf("ID: %ld\n", snapshot->id);
  printf("Last Log Entry: %ld\n", snapshot->last_entry);
  printf("Blocks:\n");
  for (i = 0; i < BLOCK_MAP_SIZE; i++) {
    block = snapshot->block_map[i];
    while (block != NULL) {
      print_block(block, page_size);
      printf("\n");
      block = block->next;
    }
  }
  printf("\n");
}

void print_filesystem(JNIEnv *env, filesystem_t *filesystem)
{
  block_t *block;
  snapshot_t *snapshot;
  int64_t i = 0;
  
  printf("Filesystem\n");
  printf("----------\n");
  printf("Block Size: %zu\n", filesystem->block_size);
  printf("Page Size: %zu\n", filesystem->page_size);
  printf("Blocks:\n");
  for (i = 0; i < BLOCK_MAP_SIZE; i++) {
    block = filesystem->block_map[i];
    while (block != NULL) {
      print_block(block, filesystem->page_size);
      printf("\n");
      block = block->next;
    }
  }
  printf("\n");
  printf("Snapshots:\n");
  for (i = 0; i < SNAPSHOT_MAP_SIZE; i++) {
    snapshot = filesystem->snapshot_map[i];
    while (snapshot != NULL) {
      print_snapshot(snapshot, filesystem->log, filesystem->page_size);
      printf("\n");
      snapshot = snapshot->next;
    }
  }
  printf("\n");
  printf("Log Capacity: %zu\n", filesystem->log_cap);
  printf("Log Length: %zu\n", filesystem->log_length);
  for (i = 0; i < filesystem->log_length; i++) {
    printf("Log %ld\n", i);
    print_log(env, filesystem->log, i);
    printf("\n");
  }
}


block_t *allocate_block(block_t **block_map, int64_t block_id)
{
  block_t *last_block = NULL;
  int block_pos = block_id % BLOCK_MAP_SIZE;
  block_t *block;

  // Find the block position for the corresponding block.
  block = block_map[block_pos];
  while (block != NULL) {
    if (block->id == block_id)
      return NULL;
    last_block = block;
    block = block->next;
  }
  
  // Make a new block.
  block = (block_t *) malloc(sizeof(block_t));
  block->id = block_id;
  block->next = NULL;
  if (last_block == NULL)
    block_map[block_pos] = block;
  else
    last_block->next = block;
  return block;
}

block_t *find_block(block_t **block_map, int64_t block_id)
{
  block_t *block = block_map[block_id % BLOCK_MAP_SIZE];
  
  while (block != NULL && block->id != block_id)
    block = block->next;
  return block;
}

snapshot_t *find_snapshot(snapshot_t **snapshot_map, int64_t snapshot_id)
{
  snapshot_t *snapshot;

  snapshot = snapshot_map[snapshot_id % SNAPSHOT_MAP_SIZE];
  while (snapshot != NULL && snapshot->id != snapshot_id)
    snapshot = snapshot->next;
  return snapshot;
}

block_t *find_or_allocate_snapshot_block(filesystem_t *filesystem, snapshot_t *snapshot, int64_t block_id)
{
  log_t *log = filesystem->log;
  log_t *cur_log;
  int i, log_id, nr_unfilled_pages;
  block_t *block = find_block(snapshot->block_map, block_id);

  if (block != NULL)
    return block;
  
  block = allocate_block(snapshot->block_map, block_id);
  log_id = snapshot->last_entry;
  while ((log_id >= 0) && (log[log_id].block_id != block_id))
    log_id--;
  printf("LogID: %d\n", log_id);
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
  {
    jfieldID vcm_id = (*env)->GetFieldID(env, vc_class, "vc", "Ljava/util/Map;");
    jobject vcm = (*env)->GetObjectField(env, vc, vcm_id);
    jclass mapClass = (*env)->FindClass(env, "java/util/HashMap");
    jmethodID constructor = (*env)->GetMethodID(env, mapClass,"<init>", "(Ljava/util/Map;)V");
    jobject cvc = (*env)->NewObject(env, mapClass, constructor, vcm);
    (*env)->SetObjectField(env,mvc,vcm_id,cvc);
  }
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
  filesystem->block_map = (block_t**) malloc(BLOCK_MAP_SIZE*sizeof(block_t*));
  for (i = 0; i < BLOCK_MAP_SIZE; i++)
    filesystem->block_map[i] = NULL;
  filesystem->snapshot_map = (snapshot_t**) malloc(SNAPSHOT_MAP_SIZE*sizeof(snapshot_t*));
  for (i = 0; i < SNAPSHOT_MAP_SIZE; i++)
    filesystem->snapshot_map[i] = NULL;
  filesystem->log_cap = 1024;
  filesystem->log_length = 0;
  filesystem->log = (log_t *) malloc (1024*sizeof(log_t));
  
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
  
  // Create a new block.;
  filesystem = get_filesystem(env, thisObj);
  new_block = allocate_block(filesystem->block_map, (int64_t) blockId);
  if (new_block == NULL) {
    fprintf(stderr, "Filesystem already contains block %ld.\n", blockId);
    return -1;
  }
  
  // Create the corresponding log entry.
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

  // Create the block and fill the appropriate fields.
  new_block->length = 0;
  new_block->cap = 0;
  new_block->pages = NULL;
  new_block->last_entry = log_pos;
  new_block->next = NULL;

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
  
  // Find the corresponding block.
  filesystem = get_filesystem(env, thisObj);
  block_pos = blockId % BLOCK_MAP_SIZE;
  cur_block = filesystem->block_map[block_pos];
  while (cur_block != NULL && cur_block->id != blockId) {
    last_block = cur_block;
    cur_block = cur_block->next;
  }
  
  // In case you did not find it return an error.
  if (cur_block == NULL) {
      fprintf(stderr, "Block with id %ld is not present.\n", blockId);
      return -1;
  }
  
  // Create the corresponding log entry.
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
  
  // Remove the block and fill the appropriate fields.
  if (last_block == NULL) {
    filesystem->block_map[block_pos] = cur_block->next;
  } else {
    last_block->next = cur_block->next;
  }
  
  // Free the corresponding block.
  free(cur_block->pages);
  free(cur_block);

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
  
  // Find the corresponding block.
  filesystem = get_filesystem(env, thisObj);
  if (snapshotId == -1) {
    block = find_block(filesystem->block_map, blockId);
    if (block == NULL) {
      // In case you did not find the block return an error.
      fprintf(stderr, "Block with id %ld is not present.\n", blockId);
      return -1;
    }
  } else {
    snapshot = find_snapshot(filesystem->snapshot_map, snapshotId);
    if (snapshot == NULL) {
      // In case you did not find the snapshot return an error.
      fprintf(stderr, "Snapshot with id %ld is not present.\n", snapshotId);
      return -2;
    }
    block = find_or_allocate_snapshot_block(filesystem, snapshot, blockId);
    // If block did not exist at this point.
    if (block->cap == -1) {
      fprintf(stderr, "Block with id %ld is not present at snapshot with rtc %ld.\n", blockId, snapshotId);
      return -1;
    }
  }

  // In case the data you ask is not written return an error.
  if (blkOfst >= block->length) {
    fprintf(stderr, "Block %ld is not written at %d byte.\n", blockId, blkOfst);
    return -3;
  }
  
//  print_filesystem(env, filesystem);
  
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
  
  // Find the corresponding block.
  filesystem = get_filesystem(env, thisObj);
  if (snapshotId == -1) {
    block = find_block(filesystem->block_map, blockId);
    if (block == NULL) {
      // In case you did not find it return an error.
      fprintf(stderr, "Block with id %ld is not present.\n", blockId);
      return -1;
    }
  } else {
    snapshot = find_snapshot(filesystem->snapshot_map, snapshotId);
    if (snapshot == NULL) {
      // In case you did not find the snapshot return an error.
      fprintf(stderr, "Snapshot with id %ld is not present.\n", snapshotId);
      return -2;
    }
    block = find_or_allocate_snapshot_block(filesystem, snapshot, blockId);
    // If block did not exist at this point.
    if (block->cap == -1) {
      fprintf(stderr, "Block with id %ld is not present at snapshot with rtc %ld.\n", blockId, snapshotId);
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
  int first_page, last_page, new_pages_length, new_pages_capacity, page_offset, buffer_offset, write_length;
  int last_page_length, i;
  int64_t log_pos;
  char *pdata;
  
  // Find the corresponding block.
  filesystem = get_filesystem(env, thisObj);
  block = find_block(filesystem->block_map, blockId);
  if (block == NULL) {
    // In case you did not find it return an error.
    fprintf(stderr, "Block with id %ld is not present.\n", blockId);
    return -1;
  }
  
  // In case you cannot write in the required offset.
  if (blkOfst > block->length) {
    fprintf(stderr, "Block %ld cannot be written at byte %d.\n", blockId, blkOfst);
    return -3;
  }
  

  // Create the new pages.
  buffer_offset = (int) bufOfst;
  first_page = blkOfst / filesystem->page_size;
  last_page = (blkOfst + length - 1) / filesystem->page_size;
  page_offset = blkOfst % filesystem->page_size;
  new_pages_length = last_page - first_page + 1;
  new_pages = (page_t*) malloc(new_pages_length*sizeof(page_t));
  new_pages[0].data = (char *) malloc(filesystem->page_size*sizeof(char));
  for (i = 0; i < page_offset; i++)
    new_pages[0].data[i] = block->pages[first_page]->data[i];
  if (first_page == last_page) {
    write_length = (jint) length;
    pdata = new_pages[0].data + page_offset;
    (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_length, (jbyte *) pdata);
    last_page_length = page_offset + write_length;
  } else {
    write_length = filesystem->page_size - page_offset;
    pdata = new_pages[0].data + page_offset;
    (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_length, (jbyte *) pdata);
    buffer_offset += write_length;
    for (i = 1; i < new_pages_length-1; i++) {
      new_pages[i].data = (char *) malloc(filesystem->page_size*sizeof(char));
      write_length = filesystem->page_size;
      pdata = new_pages[i].data;
      (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_length, (jbyte *) pdata);
      buffer_offset += write_length;
    }
    new_pages[new_pages_length-1].data =  (char *) malloc(filesystem->page_size*sizeof(char));
    write_length = (int) bufOfst + (int) length - buffer_offset;
    pdata = new_pages[new_pages_length-1].data;
    (*env)->GetByteArrayRegion (env, buf, (jint) buffer_offset, (jint) write_length, (jbyte *) pdata);
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
  check_and_increase_log_length(filesystem);
  log_pos = filesystem->log_length;
  filesystem->log[log_pos].block_id = blockId;
  filesystem->log[log_pos].block_length = block->length;
  filesystem->log[log_pos].page_id = first_page;
  filesystem->log[log_pos].nr_pages = new_pages_length;
  filesystem->log[log_pos].pages = new_pages;
  filesystem->log[log_pos].previous = block->last_entry;
  filesystem->log[log_pos].rtc = read_local_rtc();
  tick_vector_clock(env, thisObj, mvc, filesystem->log+log_pos);
  filesystem->log_length += 1;
  block->last_entry = log_pos;
  
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
  int i;
  
  // Find the corresponding position to write snapshot.
  filesystem = get_filesystem(env, thisObj);
  snapshot = filesystem->snapshot_map[id % SNAPSHOT_MAP_SIZE];
  while (snapshot != NULL) {
    // If snapshot already exists return with -1.
    if (snapshot->id == id) {
      fprintf(stderr, "Snapshot %ld already exists\n", id);
      return -1;
    }
    last_snapshot == snapshot;
    snapshot = snapshot->next;
  }
  
  // Fill snapshot's metadata.
  snapshot = (snapshot_t*) malloc(sizeof(snapshot_t));
  snapshot->id = id;
  snapshot->last_entry = (int64_t) eid;
  snapshot->block_map = (block_t**) malloc(BLOCK_MAP_SIZE*sizeof(block_t*));
  for (i = 0; i < BLOCK_MAP_SIZE; i++)
    snapshot->block_map[i] = NULL;
  snapshot->next = NULL;
  
  // Include snapshot in snapshot map.
  if (last_snapshot == NULL)
    filesystem->snapshot_map[id % SNAPSHOT_MAP_SIZE] = snapshot;
  else
    last_snapshot->next = snapshot;
  return 0;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    deleteSnapshot
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_deleteSnapshot
  (JNIEnv *env, jobject thisObj, jlong rtc)
{
  filesystem_t *filesystem;
  snapshot_t *snapshot;
  snapshot_t *last_snapshot = NULL;
  block_t *cur_block, *next_block;
  int64_t id = (int64_t) rtc;
  int i;
  
  // Find the snapshot to delete.
  filesystem = get_filesystem(env, thisObj);
  snapshot = filesystem->snapshot_map[id % SNAPSHOT_MAP_SIZE];
  while (snapshot != NULL && snapshot->id != id) {
    last_snapshot = snapshot;
    snapshot = snapshot->next;
  }

  // If snapshot does not exist return -2.
  if (snapshot == NULL) {
    fprintf(stderr, "Snapshot %ld does not exist\n", id);
    return -1;
  }
  
  // Delete all blocks associated with snapshot.
  for (i = 0; i < BLOCK_MAP_SIZE; i++) {
    cur_block = snapshot->block_map[i];
    while (cur_block != NULL) {
      next_block = cur_block->next;
      free(cur_block->pages);
      free(cur_block);
      cur_block = next_block;
    }
  }
  
  // Delete snapshot from the snapshot map.
  if (last_snapshot == NULL)
    filesystem->snapshot_map[id % SNAPSHOT_MAP_SIZE] = snapshot->next;
  else
    last_snapshot->next = snapshot->next;
  
  // Free the corresponding memory.  
  free(snapshot);
  
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
  //TODO: release all memory data.
}
