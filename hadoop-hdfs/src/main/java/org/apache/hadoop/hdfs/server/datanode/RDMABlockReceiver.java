/**
 * 
 */
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemDatasetManager;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemDatasetManager.MemBlockMeta;
import org.apache.zookeeper.common.IOUtils;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemDatasetImpl;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.RDMAWriteAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.RDMAWritePacketProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import edu.cornell.cs.sa.HybridLogicalClock;
import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
/**
 * @author weijia
 *
 */
public class RDMABlockReceiver implements Closeable {

  private final String inAddr,myAddr;
  private DataNode datanode;
  private final String clientName;
  private final DataInputStream in;
  private final ExtendedBlock block;
  private final long vaddr;
  private final MemBlockMeta replicaInfo;
  private final LinkedList<RDMAWriteAckProto> ackQueue;
  private long lastSeqno;
  private volatile boolean isClosed;
  private Thread responderThd;
  static final Log LOG = DataNode.LOG;
  
  /**
   * constructor
   * @param block
   * @param in
   * @param inAddr
   * @param myAddr
   * @param bytesRcvd
   * @param newGs
   * @param clientName
   * @param datanode
   * @param vaddr
   * @param mhlc
   * @throws IOException
   */
  RDMABlockReceiver(final ExtendedBlock block, 
      final DataInputStream in,
      final String inAddr, 
      final String myAddr,
      final long bytesRcvd,
      final long newGs, 
      final String clientName,
      final DataNode datanode, 
      final long vaddr, 
      HybridLogicalClock mhlc)
  throws IOException{
    //STEP 1: initialize the receiver
    this.inAddr = inAddr;
    this.myAddr = myAddr;
    this.datanode = datanode;
    this.clientName = clientName;
    this.in = in;
    this.block = block;
    this.vaddr = vaddr;
    this.ackQueue = new LinkedList<RDMAWriteAckProto>();
    this.lastSeqno = -1L;
    this.isClosed = false;
    
    
    //STEP 2: create replicaInfo
    if(datanode.data.getReplica(block)==null){
      //create new replica
      replicaInfo = (MemBlockMeta)datanode.data.createRbw(block,mhlc);
    }else{
      //open existing replica
      replicaInfo = (MemBlockMeta)datanode.data.append(block, newGs, bytesRcvd);
      if(datanode.blockScanner!=null)
        datanode.blockScanner.deleteBlock(block.getBlockPoolId(), block.getLocalBlock());
      block.setGenerationStamp(newGs);
      datanode.notifyNamenodeReceivingBlock(block, replicaInfo.getStorageUuid());
    }
  }
  
  class Responder implements Runnable{

    final OutputStream replyOutputStream;
    
    Responder(OutputStream out){
      this.replyOutputStream = out;
    }
    
    @Override
    public void run() {
      while(!isClosed){
        synchronized(ackQueue){
          if(ackQueue.isEmpty())
            try {
              ackQueue.wait();
              continue;
            } catch (InterruptedException e) {
              //do nothing
            }
          if(ackQueue.isEmpty()) // probably closed.
            continue;
          RDMAWriteAckProto ack = ackQueue.getFirst();
          try {
            ack.writeDelimitedTo(replyOutputStream);
          } catch (IOException e) {
            LOG.error("RDMABlockReceiver.Responder: Cannot send ack:"+ack);
          }
        }
      }
    }
  }
  
  boolean receiveNextPacket()throws IOException{
    RDMAWritePacketProto proto = RDMAWritePacketProto.parseFrom(vintPrefixed(in));
    long seqno = proto.getSeqno();
    boolean islast = proto.getIsLast();
    long length = proto.getLength();
    long offset = proto.getOffset();
    HybridLogicalClock mhlc = PBHelper.convert(proto.getMhlc());
    //STEP 0: validate:
    if(seqno != -1L || seqno != lastSeqno + 1){
      throw new IOException("RDMABlockReceiver out-of-order packet received: expecting seqno[" +
        (lastSeqno+1) + "] but received seqno["+seqno+"]");
    }
    //STEP 1: handle normal write
    if(seqno > 0L){
      replicaInfo.writeByRDMA((int)offset, (int)length, this.inAddr, this.vaddr, mhlc);
      lastSeqno ++;
    }
    
    //STEP 2: enqueue ack...
    RDMAWriteAckProto ack = RDMAWriteAckProto.newBuilder()
        .setSeqno(seqno)
        .setStatus(Status.SUCCESS)
        .setMhlc(PBHelper.convert(mhlc))
        .build();
    synchronized(this.ackQueue){
      this.ackQueue.addLast(ack);
      ackQueue.notifyAll();
    }

    //STEP 3: wait and stop.
    if(seqno == -1L && islast){
      synchronized(this.ackQueue){
        while(this.ackQueue.isEmpty()){
          try {
            this.ackQueue.wait();
          } catch (InterruptedException e) {
            //do nothing
          }
        }
        isClosed = true;
        this.ackQueue.notifyAll();
      }
      return false;
    }
    return true;
  }
  
  /**
   * Receive all packages to a block
   * @param replyOut
   */
  void receiveBlock(DataOutputStream replyOut)throws IOException{
    this.isClosed = true;
    //STEP 1: run responder. 
    this.responderThd = new Thread(new Responder(replyOut));
    this.responderThd.start();
    //STEP 2: do receive packet.
    while(receiveNextPacket());
    try {
      this.responderThd.join();
    } catch (InterruptedException e) {
      LOG.info("RDMABlockReceiver: fail to join responder thread.");
    }
    replyOut.flush();
  }
  
  @Override
  public void close() throws IOException {
    // do nothing here...
  }

}
