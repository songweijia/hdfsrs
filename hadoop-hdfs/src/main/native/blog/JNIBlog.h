/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class edu_cornell_cs_blog_JNIBlog */

#ifndef _Included_edu_cornell_cs_blog_JNIBlog
#define _Included_edu_cornell_cs_blog_JNIBlog
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    initialize
 * Signature: (II)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_initialize
  (JNIEnv *, jobject, jint, jint, jstring);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    createBlock
 * Signature: (Ledu/cornell/cs/sa/HybridLogicalClock;J)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_createBlock
  (JNIEnv *, jobject, jobject, jlong);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    deleteBlock
 * Signature: (Ledu/cornell/cs/sa/HybridLogicalClock;J)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_deleteBlock
  (JNIEnv *, jobject, jobject, jlong);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readBlock
 * Signature: (JJIII[B)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_readBlock
  (JNIEnv *, jobject, jlong, jlong, jint, jint, jint, jbyteArray);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    getNumberOfBytes
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_getNumberOfBytes
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    writeBlock
 * Signature: (Ledu/cornell/cs/sa/HybridLogicalClock;JIII[B)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_writeBlock
  (JNIEnv *, jobject, jobject, jlong, jint, jint, jint, jbyteArray);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    createSnapshot
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_createSnapshot
  (JNIEnv *, jobject, jlong);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readLocalRTC
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_edu_cornell_cs_blog_JNIBlog_readLocalRTC
  (JNIEnv *, jclass);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    destroy
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_edu_cornell_cs_blog_JNIBlog_destroy
  (JNIEnv *, jobject);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpInitialize
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpInitialize
  (JNIEnv *, jclass, jlong, jlong);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpDestroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpDestroy
  (JNIEnv *, jclass, jlong);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpAllocateBuffer
 * Signature: (J)Ledu/cornell/cs/blog/JNIBlog/RBPBuffer;
 */
JNIEXPORT jobject JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpAllocateBuffer
  (JNIEnv *, jclass, jlong);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpReleaseBuffer
 * Signature: (Ledu/cornell/cs/blog/JNIBlog/RBPBuffer;)V
 */
JNIEXPORT void JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpReleaseBuffer
  (JNIEnv *, jclass, jobject);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpConnect
 * Signature: (JII)V
 */
JNIEXPORT void JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpConnect
  (JNIEnv *, jclass, jlong, jint, jint);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpRDMAWrite
 * Signature: (IJJ[J)V
 */
JNIEXPORT void JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpRDMAWrite
  (JNIEnv *, jclass, jint, jlong, jlong, jlongArray);

#ifdef __cplusplus
}
#endif
#endif
