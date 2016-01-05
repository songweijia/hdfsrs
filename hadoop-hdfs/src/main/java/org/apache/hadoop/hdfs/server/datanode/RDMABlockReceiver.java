/**
 * 
 */
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemDatasetManager;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemDatasetManager.MemBlockMeta;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemDatasetImpl;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.RDMAWriteAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.RDMAWritePacketProto;
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

  private final MemDatasetManager mdm;
  private final String inAddr,myAddr;
  private DataNode datanode;
  private final String clientName;
  private final DataInputStream in;
  private final ExtendedBlock block;
  private final long vaddr;
  private final MemBlockMeta replicaInfo;
  
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
    this.mdm = ((MemDatasetImpl)(datanode.data)).getManager();
    
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
  
  boolean receiveNextPacket()throws IOException{
    RDMAWritePacketProto proto = RDMAWritePacketProto.parseFrom(vintPrefixed(in));
    long seqno = proto.getSeqno();
    boolean islast = proto.getIsLast();
    long length = proto.getLength();
    long offset = proto.getOffset();
    HybridLogicalClock mhlc = PBHelper.convert(proto.getMhlc());
    //TODO: do write.
    //STEP 1:
    //STEP 2:
    return false;
  }
  
  /**
   * Receive all packages to a block
   * @param replyOut
   */
  void receiveBlock(DataOutputStream replyOut)throws IOException{
    //TODO
    while(receiveNextPacket());
  }
  
  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    
  }

}
