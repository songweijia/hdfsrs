package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import  org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.Replica;


public class MemDatasetManager {
  static final Log LOG = LogFactory.getLog(MemDatasetManager.class);
  
  class MemAddr {
    int regionID;
    int offset;
    
    MemAddr(int rid, int offset) {
      this.regionID = rid;
      this.offset = offset;
    }
  }
  
  public class MemBlockMeta extends Block implements Replica {
    MemAddr offset;
    long bytesAcked;
    ReplicaState state;
    
    MemBlockMeta(MemAddr offset, long length, long genStamp, long blockId, ReplicaState state) {
      super(blockId, length, genStamp);
      this.offset = offset;
      this.state = state;
      this.bytesAcked = 0;
    }

    public ReplicaState getState() {
      return state;
    }
    
    public long getBytesOnDisk() {
      return bytesAcked;
    }

    public void setBytesOnDisk(long bytes) {
      bytesAcked = bytes;
    }
    
    public long getVisibleLength() {
      return bytesAcked;
    }

    public String getStorageUuid() {
      return "0";
    }
    
    public MemAddr getOffset() {
      return offset;
    }
    
    public long getBytesAcked() {
      return bytesAcked;
    }
    
    public void setBytesAcked(long bytes) {
      bytesAcked = bytes;
    }
  }

  class ByteBufferOutputStream extends OutputStream {
    ByteBuffer buf;
    
    ByteBufferOutputStream(byte[] buf, int offset) {
      LOG.warn("CQ: ByteBufferOutputStream: buf length:" + buf.length + " offset:" + offset);
      this.buf = ByteBuffer.wrap(buf, offset, buf.length - offset);
    }
    public synchronized void write(int b) throws IOException {
      buf.put((byte)b);
    }

    public synchronized void write(byte[] bytes, int off, int len) throws IOException {
      buf.put(bytes, off, len);
    } 
  }

  private ByteBuffer memRegions[];
  private LinkedHashMap<ExtendedBlockId, MemBlockMeta> memMaps; 
  private HashMap<ExtendedBlockId, String> diskMaps;
  private MemDatasetImpl dataset;
  private LinkedList<MemAddr> availableAddr;
  private final long capacity;
  private final long blocksize; 
  private final int maxRegionSize;
  
  private final int cacheSize;
  
  MemDatasetManager(MemDatasetImpl dataset, Configuration conf) {
    this.dataset = dataset;
    this.blocksize = conf.getLongBytes(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT);
    this.capacity = conf.getLong("dfs.memory.capacity", 1024 * 1024 * 1024 * 2);
    this.maxRegionSize = conf.getInt("dfs.memory.regionsize", 1024 * 1024 * 1024 * 1);
    
    this.memRegions = new ByteBuffer[(int)((this.capacity + this.maxRegionSize - 1) / this.maxRegionSize)];
    // TODO: try to replace this buffer to the infiniband registered buffer
    for (int i = 0; i < this.memRegions.length; i++)
      this.memRegions[i] = ByteBuffer.allocate(this.maxRegionSize);
    
    LOG.warn("CQ: MemDatasetManager: blocksize:" + blocksize + " maxregionsize:" + maxRegionSize + " capacity:" + capacity);
    availableAddr = new LinkedList<MemAddr>();
    for (int i = 0; i < this.memRegions.length; i++)
      for (int j = 0; j < this.maxRegionSize; j += this.blocksize)
        availableAddr.add(new MemAddr(i,j));
    
    this.cacheSize = (int)(this.capacity / this.blocksize);
    this.diskMaps = new HashMap<ExtendedBlockId, String>();
    this.memMaps = new LinkedHashMap<ExtendedBlockId, MemBlockMeta> (this.cacheSize + 1, 1F, true) {
      private static final long serialVersionUID = 1;
      
      protected boolean removeEldestEntry(Map.Entry<ExtendedBlockId, MemBlockMeta> eldest) { 
        if (size() > cacheSize) {
          availableAddr.add(eldest.getValue().getOffset());
          eldest.getValue().offset = null;
          diskMaps.put(eldest.getKey(), "");
          return true;
        }
        return false;
      } 
    };
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
  
  MemBlockMeta getNewBlock(String bpid, long blockId, long genStamp) {
    ExtendedBlockId key = new ExtendedBlockId(blockId, bpid);
    synchronized (memMaps) {
      if (availableAddr.size() > 0) {
        MemBlockMeta meta = new MemBlockMeta(availableAddr.poll(), 0, genStamp, blockId, ReplicaState.TEMPORARY);
        memMaps.put(key, meta);
        LOG.warn("CQ: getNewBlock: regionID:" + meta.offset.regionID + " addr:" + meta.offset.offset);
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
  
  InputStream getInputStream(MemAddr baseOffset, long offset) {
    ByteArrayInputStream in = new ByteArrayInputStream(memRegions[baseOffset.regionID].array());
    in.skip(baseOffset.offset + offset);
    return in;
  }
  
  OutputStream getOutputStream(MemAddr baseOffset, long offset) {
    if (offset < 0) offset = 0;
    return new ByteBufferOutputStream(memRegions[baseOffset.regionID].array(), (int)(baseOffset.offset + offset));
  }
  
  List<Block> getBlockMetas(String bpid, ReplicaState state) {
    LinkedList<Block> results = new LinkedList<Block>();
    synchronized (memMaps) {
      for (Entry<ExtendedBlockId, MemBlockMeta> entry: memMaps.entrySet())
        if (entry.getKey().getBlockPoolId().equals(bpid) && (state == null || entry.getValue().getState() == state))
          results.add(entry.getValue());
    }
    return results;
  }
}
