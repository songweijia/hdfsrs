/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;

/** 
 * This class defines a replica in a pipeline, which
 * includes a persistent replica being written to by a dfs client or
 * a temporary replica being replicated by a source datanode or
 * being copied for the balancing purpose.
 * 
 * The base class implements a temporary replica
 */
public class MemReplicaInPipeline extends ReplicaInfo
                        implements ReplicaInPipelineInterface {
  private long bytesAcked;
  private byte[] lastChecksum;  
  private Thread writer;

  public MemReplicaInPipeline(long blockId, long genStamp, MemDatasetManager memManager) {
    
  }

  @Override
  public long getVisibleLength() {
    return -1;
  }
  
  @Override  //ReplicaInfo
  public ReplicaState getState() {
    return ReplicaState.TEMPORARY;
  }
  
  @Override // ReplicaInPipelineInterface
  public long getBytesAcked() {
    return bytesAcked;
  }
  
  @Override // ReplicaInPipelineInterface
  public void setBytesAcked(long bytesAcked) {
    this.bytesAcked = bytesAcked;
  }
  
  @Override // ReplicaInPipelineInterface
  public long getBytesOnDisk() {
    return 0;
  }
  
  @Override // ReplicaInPipelineInterface
  public synchronized void setLastChecksumAndDataLen(long dataLength, byte[] lastChecksum) {
    this.bytesAcked = dataLength;
    this.lastChecksum = lastChecksum;
  }
  
  @Override // ReplicaInPipelineInterface
  public synchronized ChunkChecksum getLastChecksumAndDataLen() {
    return new ChunkChecksum(getBytesOnDisk(), lastChecksum);
  }

  /**
   * Set the thread that is writing to this replica
   * @param writer a thread writing to this replica
   */
  public void setWriter(Thread writer) {
    this.writer = writer;
  }
  
  @Override  // Object
  public boolean equals(Object o) {
    return super.equals(o);
  }
  
  /**
   * Interrupt the writing thread and wait until it dies
   * @throws IOException the waiting is interrupted
   */
  public void stopWriter(long xceiverStopTimeout) throws IOException {
    if (writer != null && writer != Thread.currentThread() && writer.isAlive()) {
      writer.interrupt();
      try {
        writer.join(xceiverStopTimeout);
        if (writer.isAlive()) {
          final String msg = "Join on writer thread " + writer + " timed out";
          DataNode.LOG.warn(msg + "\n" + StringUtils.getStackTrace(writer));
          throw new IOException(msg);
        }
      } catch (InterruptedException e) {
        throw new IOException("Waiting for writer thread is interrupted.");
      }
    }
  }
  
  @Override  // Object
  public int hashCode() {
    return super.hashCode();
  }
  
  @Override // ReplicaInPipelineInterface
  public ReplicaOutputStreams createStreams(boolean isCreate, 
      DataChecksum requestedChecksum, long offset)
          throws IOException {
    return null;
  }
  
  @Override
  public String toString() {
    return super.toString()
        + "\n  bytesAcked=" + bytesAcked;
  }
}
