#include <jni.h>
#include <stdio.h>
#include <vector>
#include "JNIBuffer.h"

std::vector<void *> data_bufs;

JNIEXPORT jobject JNICALL Java_JNIBuffer_createBuffer(JNIEnv *env, jobject thisObj, jint capacity) {
	void * data = malloc(capacity);
	data_bufs.push_back(data);

	jobject buf = env->NewDirectByteBuffer(data, capacity);
	jobject ref = env->NewGlobalRef(buf);
	return ref;
}

JNIEXPORT void JINCALL Java_JNIBuffer_deleteBuffers(JNIEnv *env, jobject thisObj) {
	for (std::vector<void *>::iterator it = data_bufs.begin(); it != data_bufs.end(); it++) {
		free(*it);
	}
	data_bufs.clear();
}
