package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

/**
 * @author weijia
 * Send block message using RDMA.
 */
public class RDMABlockSender {

  /** the block to read from */
  protected final ExtendedBlock block;
  /** Position of first byte to read from block file */
  protected final long startOffset;
  /** Position of last byte to read from block file */
  protected final long endOffset;
  /** DateNode */
  protected DataNode datanode;
  /** peer */
  protected final String clientIp;
  /** remote vaddress */
  protected final long vaddr;
  
  /**
   * Constructor 
   * @param block Block that is being read
   * @param startOffset starting 
   * @param length
   * @param datanode
   */
  RDMABlockSender(ExtendedBlock block, 
      long startOffset, long length,
      DataNode datanode, String clientIp, long vaddr){
    this.block = block;
    this.startOffset = startOffset;
    this.endOffset = Math.min(startOffset + length, this.block.getNumBytes());
    this.clientIp = clientIp;
    this.vaddr = vaddr;
  }
  
  /**
   * doSend
   */
  void doSend()
  throws IOException{
    
  }
}
