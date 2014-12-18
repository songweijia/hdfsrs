package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.nio.ByteBuffer;;

public class JNIBuffer {
  static {
    System.loadLibrary("JNIBuffer");
  }
  
  public native ByteBuffer createBuffer(int capacity);
  public native void deleteBuffers();
}