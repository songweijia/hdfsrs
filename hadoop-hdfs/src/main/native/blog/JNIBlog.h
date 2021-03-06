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
 * Method:    initializeRDMA
 * Signature: (JIILjava/lang/String;Ljava/lang/String;I)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_initializeRDMA
  (JNIEnv *, jobject, jlong, jint, jint, jstring, jstring, jint);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    initialize
 * Signature: (JIILjava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_initialize
  (JNIEnv *, jobject, jlong, jint, jint, jstring);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    destroy
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_edu_cornell_cs_blog_JNIBlog_destroy
  (JNIEnv *, jobject);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    setGenStamp
 * Signature: (Ledu/cornell/cs/sa/HybridLogicalClock;JJ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_setGenStamp
  (JNIEnv *, jobject, jobject, jlong, jlong);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    createBlock
 * Signature: (Ledu/cornell/cs/sa/HybridLogicalClock;JJ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_createBlock
  (JNIEnv *, jobject, jobject, jlong, jlong);

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
 * Signature: (JIII[B)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_readBlock__JIII_3B
  (JNIEnv *, jobject, jlong, jint, jint, jint, jbyteArray);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readBlockRDMA
 * Signature: (JII[BIJ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_readBlockRDMA__JII_3BIJ
  (JNIEnv *, jobject, jlong, jint, jint, jbyteArray, jint, jlong);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readBlock
 * Signature: (JJIII[BZ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_readBlock__JJIII_3BZ
  (JNIEnv *, jobject, jlong, jlong, jint, jint, jint, jbyteArray, jboolean);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readBlockRDMA
 * Signature: (JJII[BIJZ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_readBlockRDMA__JJII_3BIJZ
  (JNIEnv *, jobject, jlong, jlong, jint, jint, jbyteArray, jint, jlong, jboolean);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    getNumberOfBytes
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_getNumberOfBytes__J
  (JNIEnv *, jobject, jlong);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    getNumberOfBytes
 * Signature: (JJZ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_getNumberOfBytes__JJZ
  (JNIEnv *, jobject, jlong, jlong, jboolean);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    writeBlockRDMA
 * Signature: (Ledu/cornell/cs/sa/HybridLogicalClock;Ledu/cornell/cs/blog/IRecordParser;JII[BIJ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_writeBlockRDMA
  (JNIEnv *, jobject, jobject, jobject, jlong, jint, jint, jbyteArray, jint, jlong);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    writeBlock
 * Signature: (Ledu/cornell/cs/sa/HybridLogicalClock;JJIII[B)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_writeBlock
  (JNIEnv *, jobject, jobject, jlong, jlong, jint, jint, jint, jbyteArray);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readLocalRTC
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_edu_cornell_cs_blog_JNIBlog_readLocalRTC
  (JNIEnv *, jclass);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    getPid
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_getPid
  (JNIEnv *, jclass);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpInitialize
 * Signature: (IILjava/lang/String;I)J
 */
JNIEXPORT jlong JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpInitialize
  (JNIEnv *, jclass, jint, jint, jstring, jint);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpDestroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpDestroy
  (JNIEnv *, jclass, jlong);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpAllocateBlockBuffer
 * Signature: (J)Ledu/cornell/cs/blog/JNIBlog/RBPBuffer;
 */
JNIEXPORT jobject JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpAllocateBlockBuffer
  (JNIEnv *, jclass, jlong);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpReleaseBuffer
 * Signature: (JLedu/cornell/cs/blog/JNIBlog/RBPBuffer;)V
 */
JNIEXPORT void JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpReleaseBuffer
  (JNIEnv *, jclass, jlong, jobject);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpConnect
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpConnect
  (JNIEnv *, jclass, jlong, jbyteArray);

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpRDMAWrite
 * Signature: ([BJ[J)V
 */
JNIEXPORT void JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpRDMAWrite
  (JNIEnv *, jobject, jbyteArray, jlong, jlongArray);

#ifdef __cplusplus
}
#endif
#endif
/* Header for class edu_cornell_cs_blog_JNIBlog_RBPBuffer */

#ifndef _Included_edu_cornell_cs_blog_JNIBlog_RBPBuffer
#define _Included_edu_cornell_cs_blog_JNIBlog_RBPBuffer
#ifdef __cplusplus
extern "C" {
#endif
#ifdef __cplusplus
}
#endif
#endif
