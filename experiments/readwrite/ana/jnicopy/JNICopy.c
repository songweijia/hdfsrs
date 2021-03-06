/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
#include <sys/time.h>
/* Header for class JNICopy */

#ifndef _Included_JNICopy
#define _Included_JNICopy
#ifdef __cplusplus
#endif

#define PGSZ (4096)
#define BUFSZ (1<<28)
volatile static char cbuf[1<<28]; //256MB

/*
 * Class:     JNICopy
 * Method:    javatoc
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_JNICopy_javatoc
  (JNIEnv * env, jclass cls, jbyteArray buf){
  //copy java to c by page, 
  int cnt = BUFSZ/PGSZ;
  int i=0;
  struct timeval tv1,tv2;
  long delta;
  gettimeofday(&tv1,NULL);
  for(i=0;i<cnt;i++){
    (*env)->GetByteArrayRegion(env, buf, (jint)i*PGSZ, (jint)PGSZ, (jbyte*)(cbuf+i*PGSZ));
  }
  gettimeofday(&tv2,NULL);
  delta = (tv2.tv_sec-tv1.tv_sec)*1000000+tv2.tv_usec-tv1.tv_usec;
  printf("Java-->C %dMB(%d) : %ldus\n", BUFSZ/1048576, PGSZ, delta);
}

/*
 * Class:     JNICopy
 * Method:    ctojava
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_JNICopy_ctojava
  (JNIEnv * env, jclass cls, jbyteArray buf){
  //copy c to java by page, 
  int cnt = BUFSZ/PGSZ;
  int i=0;
  struct timeval tv1,tv2;
  long delta;
  gettimeofday(&tv1,NULL);
  for(i=0;i<cnt;i++){
    (*env)->SetByteArrayRegion(env, buf, (jint)i*PGSZ, (jint)PGSZ, (jbyte*)(cbuf+i*PGSZ));
  }
  gettimeofday(&tv2,NULL);
  delta = (tv2.tv_sec-tv1.tv_sec)*1000000+tv2.tv_usec-tv1.tv_usec;
  printf("C-->Java %dMB(%d) : %ldus\n", BUFSZ/1048576, PGSZ, delta);
}

#ifdef __cplusplus
#endif
#endif
