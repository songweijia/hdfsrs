package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;

import  org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.util.Time;

class MemDatasetManager {
  class MemBlockMeta {
    long offset;
    long length;
    long genStamp;
    
    MemBlockMeta(long offset, long length, long genStamp) {
      this.offset = offset;
      this.length = length;
      this.genStamp = genStamp;
    }
  }
  
  private ByteBuffer memRegion;
  private HashMap<ExtendedBlockId, MemBlockMeta> memMaps; 
  private MemDatasetImpl dataset;
  private LinkedList<Long> availableAddr;
  private final long capacity;
  private final long blocksize; 
  
  MemDatasetManager(MemDatasetImpl dataset, Configuration conf) {
    this.dataset = dataset;
    this.blocksize = conf.getLongBytes(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT);
    this.capacity = conf.getLong("dfs.memory.capacity", 1024 * 1024 * 1024 * 1);
    
    // TODO: try to replace this buffer to the infiniband registered buffer
    this.memRegion = ByteBuffer.allocate((int)this.capacity);
    
    this.availableAddr = new LinkedList<Long>();
    for (long i = 0; i < this.capacity; i += this.blocksize)
      availableAddr.add(i);
  }
  
  long getCapacity() {
    return capacity;
  }
  
  MemBlockMeta get(String bpid, long blockId) {
    ExtendedBlockId key = new ExtendedBlockId(blockId, bpid);
    MemBlockMeta meta;
    synchronized(memMaps) {
      meta = memMaps.get(key);
    }
    return meta;
  }
  
  MemBlockMeta getNewBlock(String bpid, long blockId) {
    ExtendedBlockId key = new ExtendedBlockId(blockId, bpid);
    synchronized (memMaps) {
      if (availableAddr.size() > 0) {
        MemBlockMeta meta = new MemBlockMeta(availableAddr.get(0), 0, Time.now());
        memMaps.put(key, meta);
        availableAddr.remove(0);
        return meta;
      }
    }
    return null;
  }
  
  void deleteBlock(String bpid, long blockId) {
    ExtendedBlockId key = new ExtendedBlockId(blockId, bpid);
    synchronized (memMaps) {
      MemBlockMeta meta = memMaps.get(key);
      if (meta != null) {
        availableAddr.add(meta.offset);
        memMaps.remove(key);
      }
    }
  }
  
  InputStream getInputStream(long baseOffset, long offset) {
    ByteArrayInputStream in = new ByteArrayInputStream(memRegion.array());
    in.skip(baseOffset + offset);
    return in;
  }
  
  OutputStream getOutputStream(long offset) {
    // TODO: create a new OutputStream that can directly write to the ByteBuffer
    return null;
  }
}