/**
 * 
 */
package org.apache.hadoop.hdfs;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.util.EnumSet;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferEncryptor;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.RDMAWriteAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.RDMAWritePacketProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import com.google.protobuf.TextFormat;

import edu.cornell.cs.blog.JNIBlog;
import edu.cornell.cs.blog.JNIBlog.RBPBuffer;
import edu.cornell.cs.sa.HybridLogicalClock;
import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;

/**
 * @author weijia
 *
 */
public class DFSRDMAOutputStream extends SeekableDFSOutputStream{
  ///////////////////////////////////////////////////////
  // Data Structures.
  static long hRDMABufferPool = JNIBlog.getRDMABufferPool();
  static final int RDMA_CON_PORT;
  // Status for the DataStreamer
  enum DSS{
    UNKNOWN,
    STREAMING,
    CREATE,
    APPEND,
    OVERWRITE,
    ERROR,
  };

  static{
    Configuration conf = new HdfsConfiguration();
    RDMA_CON_PORT = conf.getInt(DFSConfigKeys.DFS_RDMA_CON_PORT_KEY, DFSConfigKeys.DFS_RDMA_CON_PORT_DEFAULT);
  }

  protected final DFSClient dfsClient;
  protected Socket s; // socket for datanode connection
  protected volatile boolean closed = false;
  protected String src; // file
  protected final long fFileId;
  protected final long fBlockSize;
  protected final long fInitialFileSize;
  protected final LinkedList<Packet> fDataQueue = new LinkedList<Packet>();
  protected final LinkedList<Packet> fAckQueue = new LinkedList<Packet>();
  protected final RBPBuffer fBlockBuffer;
  long dataStartPos;//, dataEndPos; // all for the buffer.
  protected Packet currentPacket = null;
  protected DataStreamer streamer = null;
  protected long currentSeqno = 0;
  protected long bytesCurBlock = 0l; // number of bytes in current block;
  protected long lastFlushOffset = 0l;
  protected long curFileSize = 0l;
  protected long pos = 0l;
  protected final Progressable fProgress;
  
  // configurables
  protected final int fAutoFlushSize;
  // internal classes
  public class Packet{
    long seqno;   //sequence number.
    long blkno; //block index in file start from 0
    long offset;  // offset into the block
    long length;  // length of the data
    boolean last; // if it is the last packet.

    @Override
    public String toString(){
      return "Packet seqno="+seqno+",blkno="+blkno+",offset="+offset+",length="+length+",last="+last;
    }
    
    public void writeTo(DataOutputStream stm)throws IOException{
      HybridLogicalClock mhlc = DFSClient.tickAndCopy();
      RDMAWritePacketProto proto = RDMAWritePacketProto.newBuilder()
          .setSeqno(seqno)
          .setOffset(offset)
          .setLength(length)
          .setIsLast(last)
          .setMhlc(PBHelper.convert(mhlc))
          .build();
      proto.writeDelimitedTo(stm);
    }
    
    public void readFields(InputStream in)throws IOException {
      RDMAWritePacketProto proto = RDMAWritePacketProto.parseFrom(vintPrefixed(in));
      this.seqno = proto.getSeqno();
      this.blkno = 0; // the receiver does not need this.
      this.offset = proto.getOffset();
      this.length = proto.getLength();
      this.last = proto.getIsLast();
    }
  }
  
  public class PacketAck{
    RDMAWriteAckProto proto;
    public final static long FLUSH_SEQNO = -1;
    
    public PacketAck() {}
    
    public long getSeqno() {
      return proto.getSeqno();
    }
    
    public Status getReply(){
      return proto.getStatus();
    }
    
    public boolean isSuccess(){
      return proto.getStatus() == Status.SUCCESS;
    }
    
    public HybridLogicalClock getHLC(){
      return PBHelper.convert(proto.getMhlc());
    }
    
    public void readFields(InputStream in)throws IOException {
      proto = RDMAWriteAckProto.parseFrom(vintPrefixed(in));
    }
    
    public void writeTo(OutputStream out) throws IOException {
      proto.writeDelimitedTo(out);
    }

    @Override
    public String toString() {
      return TextFormat.shortDebugString(proto);
    }
  }
  
  private void queuePacket(Packet pkt){
    synchronized(fDataQueue) {
      if(pkt == null) return;
      //CLIENT: "inserting dQ"
      fDataQueue.addLast(pkt);
      fDataQueue.notifyAll();
    }
  }
  
  class DataStreamer implements Runnable{
    //DataStreamer data strcutures.
    private ExtendedBlock block;
    private long blockNumber;
    private long maxBlockNumber; // this is maximum Blkno we ever seen.
    private Token<BlockTokenIdentifier> accessToken;
    private DataOutputStream blockStream;
    private DataInputStream blockReplyStream;
    private Thread streamerThd;
    private ResponseProcessor responder;
    volatile boolean bRunning; // dataQueue needs to be locked to change this flag.
    long lastSentSeqno;
    DSS stat;
    DatanodeInfo [] nodes = null;
    Exception lastException; // lastException
    
    
    /** Default construction for file create */
    protected DataStreamer(long maxBlockNumber)
    throws Exception{
      block = null;
      blockNumber = -1;
      streamerThd = null;
      responder = null;
      bRunning = false;
      lastSentSeqno = -1;
      stat = DSS.UNKNOWN;
      lastException = null;
      this.maxBlockNumber = maxBlockNumber;
    }
    
    public synchronized void start(){
      streamerThd = new Thread(this);
      responder = null;
      bRunning = true;
      streamerThd.start();
    }
    
    private class ResponseProcessor implements Runnable{
      
      boolean responderClosed = false;
      Thread thd = null;

      @Override
      public void run() {
        while(!responderClosed && dfsClient.clientRunning){
          
          //STEP 1: pick a packet from aQ
          Packet pkt = null;
          synchronized(fDataQueue){
            while(fAckQueue.isEmpty() && !responderClosed)
              try{
                fDataQueue.wait();
              }catch(InterruptedException e){
                DFSClient.LOG.debug("Responder is interrupted on wait.");
              }
            if(!responderClosed)continue; // exit on stop
            pkt = fAckQueue.getFirst();
          }

          //STEP 2: wait for ack:
          PacketAck ack = new PacketAck();
          try{
            ack.readFields(blockReplyStream);
          }catch(IOException e){
            lastException = e;
            DFSClient.LOG.error("Responder cannot read response from peer."+e);
          }
          if(ack.getSeqno() != pkt.seqno){
            lastException = new Exception("Responder gets out-of-order ack. expecting seqno=" + pkt.seqno +
                "but receiving seqno="+ack.getSeqno());
            DFSClient.LOG.error(lastException);
          }
          DFSClient.tickOnRecv(ack.getHLC());
          if(!ack.isSuccess()){
            lastException = new Exception("Responder gets unsuccessful ack: ack.status="+ack.getReply());
            DFSClient.LOG.error(lastException);
          }
          
          //STEP 3: remove it from aQ
          synchronized(fDataQueue){
            fAckQueue.removeFirst();
            fDataQueue.notifyAll();
          }
        }
      }
      
      void start(){
        thd = new Thread(this);
        thd.start();
      }
      
      void close(){
        if(thd!=null){
          this.responderClosed = true;
          this.thd.interrupt();
        }
      }
    }
    
    
    private boolean checkDatanodes(String msg){
      boolean bRet = true;
      if(nodes == null || nodes.length < 1){
        this.lastException = new IOException(msg);
        DFSClient.LOG.error(this.lastException);
        stat = DSS.ERROR;
        bRet = false;
      }
      return bRet;
    }

    /**
     * allocate a new block and continue to streaming.
     * Here we assume the following variables have been setup:
     * - blockNumber: nextblock to create
     * - block: last block to be closed, can be null for the first block
     * On successful return, the following variables are set accordingly:
     * - block: current block
     * - accessToken:
     * - nodes: datanodes
     * - state: DSS.STREAMING
     */
    public void doCreate(){
      //STEP 1: get LocatedBlock,
      HybridLogicalClock mhlc = DFSClient.tickAndCopy();
      ExtendedBlock oldBlock = block;
      LocatedBlock lb = null;
      try {
        lb = dfsClient.namenode.addBlock(src, 
            dfsClient.clientName, oldBlock, null, fFileId, null, mhlc);
      } catch (Exception e) {
        DFSClient.LOG.error("doCreate() throw exceptions:" + e);
        this.lastException = e;
        stat = DSS.ERROR;
        return;
      }
      //STEP 2: setup pipeline,
      //STEP 2.1: get block information
      block = lb.getBlock();
      accessToken = lb.getBlockToken();
      nodes = lb.getLocations();
      
      //STEP 2.2: create output stream
      try{
        if(!this.createBlockOutputStream(nodes,0L))
          return;
      }catch(IOException e){
        this.lastException = e;
        stat = DSS.ERROR;
        return;
      }
      
      //STEP 2.3: start Responder
      this.responder = new ResponseProcessor();
      responder.start();
      
      //STEP 3: change state
      stat = DSS.STREAMING;
    }
    
    /**
     * appending to a block
     * Here we assume the following variables have been setup 
     * - blockNumber
     * - stat
     * - block
     * After successful return, we assume the following variables have been setup:
     * - accessToken
     * - nodes
     */
    public void doAppend(){
      //STEP 1: setup pipeline
      //STEP 1.1: update generation stamp and access token
      LocatedBlock lb;
      try {
        lb = dfsClient.namenode.updateBlockForPipeline(block,dfsClient.clientName);
        long newGS = lb.getBlock().getGenerationStamp();
        accessToken = lb.getBlockToken();
        nodes = lb.getLocations();
        //STEP 1.2: create output stream
        if(!this.createBlockOutputStream(nodes,newGS))
          return;
        //STEP 1.3: update pipeline at namenode
        ExtendedBlock newBlock = new ExtendedBlock(
            block.getBlockPoolId(),
            block.getBlockId(),
            block.getLocalBlock().getLongSid(),
            block.getNumBytes(),
            newGS);
        dfsClient.namenode.updatePipeline(dfsClient.clientName, block, newBlock, nodes, 
            lb.getStorageIDs());
        block = newBlock;
        //STEP 1.4: start Responser
        this.responder = new ResponseProcessor();
        this.responder.start();
      } catch (IOException e) {
        this.lastException = e;
        DFSClient.LOG.error("doAppend() failed with exception:"+e);
        return;
      }
      //STEP 2: change state
      stat = DSS.STREAMING;
    }
    
    
    /**
     * overwrite to a block
     * We assume the following variables has been setup:
     * - blockNumber
     * After this is set, the following variables are setup:
     * - block
     * - nodes
     * - accessToken
     * - bytesCurBlock
     */
    public void doOverwrite(){
      //STEP 1: get locatedBlock
      HybridLogicalClock mhlc = DFSClient.tickAndCopy();
      try {
        LocatedBlock lb = dfsClient.namenode.overwriteBlock(src, 
          block, (int)(blockNumber), fFileId, dfsClient.clientName, mhlc);
        DFSClient.tickOnRecv(mhlc);
        accessToken = lb.getBlockToken();
        block = lb.getBlock();
        bytesCurBlock = block.getNumBytes();
        nodes = lb.getLocations();
        //STEP 2: setup pipeline
        lb = dfsClient.namenode.updateBlockForPipeline(block,dfsClient.clientName);
        long newGS = lb.getBlock().getGenerationStamp();
        accessToken = lb.getBlockToken();
        //STEP 2.2: create output stream
        if(!this.createBlockOutputStream(nodes,newGS))
          return;
        //STEP 2.3: update pipeline at namenode
        ExtendedBlock newBlock = new ExtendedBlock(
            block.getBlockPoolId(),
            block.getBlockId(),
            block.getLocalBlock().getLongSid(),
            block.getNumBytes(),
            newGS);
        dfsClient.namenode.updatePipeline(dfsClient.clientName, block, newBlock, nodes, 
            lb.getStorageIDs());
        block = newBlock;
        //STEP 2.4: start Responser
        this.responder = new ResponseProcessor();
        this.responder.start();
      } catch (IOException e) {
        this.lastException = e;
        DFSClient.LOG.error("doOverwrite() failed with exception:"+e);
        return;
      }
      //STEP 3: change state
      stat = DSS.STREAMING;
    }
    
    private boolean validatePacket(Packet pkt){
      return this.lastSentSeqno == -1 || pkt.seqno == this.lastSentSeqno + 1;
    }
    
    /**
     * write a flush pkt to inform
     * @param bFinishBlock true to disconnect. false for just flush.
     * @return
     */
    private boolean flushInternal(boolean bFinishBlock){
      Packet fPkt = null;
      try {
        if(bFinishBlock){
          fPkt = new Packet();
          fPkt.blkno = this.blockNumber;
          fPkt.last = true;
          fPkt.seqno = -1; // flush packet always has seqno -1
          fPkt.writeTo(blockStream);
        }
        synchronized(fDataQueue){
          if(bFinishBlock)
            fAckQueue.addLast(fPkt);
          // STREAMER: wait till all pkts are acked.
          while(!fAckQueue.isEmpty()){
            try {
              fDataQueue.wait();
            } catch (InterruptedException e) {
              //do nothing
            }
          }
        }
        return true;
      } catch (IOException e) {
        DFSClient.LOG.error("flushInternal("+bFinishBlock+") failed with sending packet:"+fPkt);
        this.lastException = e;
        this.stat = DSS.ERROR;
        return false;
      }
    }
    
    private void closeResponder(){
      this.responder.close();
    }
    
    private void setLastException(Exception e){
      this.lastException = e;
    }
    
    private void closeStream(){
      if (blockStream != null) {
        try {
          blockStream.close();
        } catch (IOException e) {
          setLastException(e);
        } finally {
          blockStream = null;
        }
      }
      if (blockReplyStream != null) {
        try {
          blockReplyStream.close();
        } catch (IOException e) {
          setLastException(e);
        } finally {
          blockReplyStream = null;
        }
      }
      if (null != s) {
        try {
          s.close();
        } catch (IOException e) {
          setLastException(e);
        } finally {
          s = null;
        }
      }
    }
    
    private void closeInternal(){
      closeResponder();
      closeStream();
      synchronized(fDataQueue){
        fDataQueue.notifyAll();
      }
    }
    
    /**
     * endBlock need to
     * 1) stop responser
     * 2) close blockStream and blockReplyStream
     * 3) set maxBlockNumber and reset blockNumber
     * 4) clear nodes
     * 4) change state to CREATE or OVERWRITE -- how to?
     * @param nextBlock
     */
    private void endBlock(long nextBlock){
      // 1)  & 2)
      closeInternal();
      // 3) set maxBlockNumber and reset blockNumber
      maxBlockNumber = Math.max(maxBlockNumber, blockNumber);
      blockNumber = nextBlock;
      // 4) clear nodes and block
      nodes = null;
      // 5) change state to CREATE or OVERWRITE
      if(nextBlock <= maxBlockNumber)
        stat = DSS.OVERWRITE;
      else
        stat = DSS.CREATE;
    }
    
    /**
     * send pkts till existing data of a same block is sent.
     * NOTE: we assume lock on dQ and pkts are in dQ.
     */
    private void doStreamingInternal(){
      while(!fDataQueue.isEmpty()){
        Packet pkt = fDataQueue.getFirst();
        if(!validatePacket(pkt)){
          lastException = new Exception("Invalid pkt:"+pkt);
          DFSClient.LOG.error("doStreaming() get invalid packet from dataQueue:"+pkt);
          return;
        }
        if(pkt.blkno == this.blockNumber){//write to the same blk,
          try {
            pkt.writeTo(this.blockStream);
          } catch (IOException e) {
            DFSClient.LOG.error("doStreaming() fail sending packet:"+pkt);
            this.lastException = e;
            this.stat = DSS.ERROR;
            return;
          }
          fDataQueue.removeFirst();
          // STREAMER: notify responder. 
          fAckQueue.addLast(pkt);
          fDataQueue.notifyAll();
        }else{//go to different blk.
          if(!this.flushInternal(true))return;
          endBlock(pkt.blkno);
          break;
        }
      }
    }
    
    /**
     * do Streaming
     */
    void doStreaming(){
      //lockdQ
      synchronized(fDataQueue){
        // STREAMER: wait pkts data from client.
        while(fDataQueue.isEmpty() && this.bRunning){
          try {
            fDataQueue.wait();
          } catch (InterruptedException e) {
            //do nothing.
          }
        }
        if(fDataQueue.isEmpty() && !this.bRunning){// stop...
          flushInternal(true);
          closeInternal();
        }else
          doStreamingInternal();
      }
    }
    
    /**
     * stop the thread. this is called from CLIENT,
     */
    void stop(){
      if(this.bRunning){
        synchronized(fDataQueue){
          this.bRunning = false;
          // CLIENT(STOP): notify streamer that is has been stopped.
          fDataQueue.notifyAll();
        }
        try {
          this.streamerThd.join();
        } catch (InterruptedException e) {
          DFSClient.LOG.warn("InterruptException received during waiting for streamer to stop gracely");
        }
      }
    }
    
    @Override
    public void run() {
      while(bRunning){
        switch(stat){
        case STREAMING:
          doStreaming();
          break;
        case CREATE:
          doCreate();
          break;
        case APPEND:
          doAppend();
          break;
        case OVERWRITE:
          doOverwrite();
          break;
        default:
          DFSClient.LOG.fatal(" DFSRDMAOutputStream started with error state:"+stat);
          bRunning = false;
        }
      }
    }
    
    
    /**
     * Before createBlockOutputStream, the follwoing variables have been setup:
     * - nodes
     * - newGS
     * After createBlockOutputStream, the following variables are setup:
     * - blockStream:
     * - replyBlockStream:
     * @param nodes
     * @param newGS
     * @return
     * @throws IOException 
     */
    private boolean createBlockOutputStream(DatanodeInfo [] nodes,long newGS) 
        throws IOException{
      if(!checkDatanodes("nodes for block is empty! block:"+block))
        return false;
      //STEP 1: connect to datanode
      DataOutputStream out = null;
      long writeTimeout = dfsClient.getDatanodeWriteTimeout(nodes.length);
      try {
        s = createSocketForPipeline(nodes[0], 1, dfsClient);
        OutputStream unbufOut = NetUtils.getOutputStream(s, writeTimeout);
        InputStream unbufIn = NetUtils.getInputStream(s);
        if (dfsClient.shouldEncryptData()  && 
            !dfsClient.trustedChannelResolver.isTrusted(s.getInetAddress())) {
          IOStreamPair encryptedStreams =
              DataTransferEncryptor.getEncryptedStreams(unbufOut,
                  unbufIn, dfsClient.getDataEncryptionKey());
          unbufOut = encryptedStreams.out;
          unbufIn = encryptedStreams.in;
        }
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            HdfsConstants.SMALL_BUFFER_SIZE));
        blockReplyStream = new DataInputStream(unbufIn);
      } catch (IOException e) {
        this.lastException = e;
        DFSClient.LOG.error("doCreate() cannot connect to the datanode");
        stat = DSS.ERROR;
        return false;
      }
      //STEP 2: send write block request
      // send the request
      HybridLogicalClock hlc = DFSClient.tickAndCopy();
      new Sender(out).writeBlockRDMA(this.block, 
          this.accessToken, dfsClient.clientName, 
          nodes, fBlockBuffer.address, block.getNumBytes(), newGS, hlc);
      
      //receive ack for connect
      BlockOpResponseProto resp = BlockOpResponseProto.parseFrom(
          vintPrefixed(blockReplyStream));
      if(resp.getStatus() != Status.SUCCESS){
        this.lastException = new IOException("Cannot connect to datanode"+nodes[0]+" for RDMA write:status="+resp.getStatus());
        DFSClient.LOG.error(this.lastException);
        stat = DSS.ERROR;
        return false;
      }
      
      //STEP 3: setup streamer variables
      assert null == blockStream : "Previous blockStream unclosed.";
      blockStream = out;
      
      return true;
    }
  }
  ///////////////////////////////////////////////////////
  
  /** construct a new output stream for creating a file  */
  protected DFSRDMAOutputStream(DFSClient dfsClient, String src,
      Progressable progress, HdfsFileStatus stat, int autoFlushSize)
          throws Exception {
    super(null, 0, 0); // we don't need checksum for RDMA
    this.dfsClient = dfsClient;
    this.src = src;
    this.fFileId = stat.getFileId();
    this.fBlockSize = stat.getBlockSize();
    this.fAutoFlushSize=autoFlushSize;
    this.fInitialFileSize = stat.getLen();
    this.curFileSize = this.fInitialFileSize;
    this.pos = 0;
    this.fProgress = progress;
    this.fBlockBuffer = JNIBlog.rbpAllocateBlockBuffer(hRDMABufferPool);
    this.bytesCurBlock = 0;
    this.resetBlockBuffer((int)bytesCurBlock);
    if((progress != null) && DFSClient.LOG.isDebugEnabled()){
      DFSClient.LOG.debug(
          "Set non-null progress callback on DFSOutputStream " + src);
    }
    //create and initialize the streamer. 
    this.streamer = new DataStreamer(-1);
    this.streamer.stat = DSS.CREATE;
    this.streamer.block = null;
    this.streamer.blockNumber = 0;
  }
  
  /** construct a new output stream for appending a file  */
  protected DFSRDMAOutputStream(DFSClient dfsClient, String src,
      Progressable progress, LocatedBlock lastBlock, 
      HdfsFileStatus stat, int autoFlushSize)throws Exception{
    super(null, 0, 0); // we don't need checksum for RDMA
    this.dfsClient = dfsClient;
    this.src = src;
    this.fFileId = stat.getFileId();
    this.fBlockSize = stat.getBlockSize();
    this.fAutoFlushSize=autoFlushSize;
    this.fInitialFileSize = stat.getLen();
    this.curFileSize = this.fInitialFileSize;
    this.fProgress = progress;
    this.fBlockBuffer = JNIBlog.rbpAllocateBlockBuffer(hRDMABufferPool);
    if((progress != null) && DFSClient.LOG.isDebugEnabled()){
      DFSClient.LOG.debug(
          "Set non-null progress callback on DFSOutputStream " + src);
    }
    this.curFileSize = fInitialFileSize;
    this.pos = fInitialFileSize;
    this.streamer = new DataStreamer(fInitialFileSize/this.fBlockSize - 1); // maxBlockNumber = fInitialFileSize/this.fBlockSize
    this.streamer.blockNumber = pos/this.fBlockSize;
    if(lastBlock != null) {
      bytesCurBlock = lastBlock.getBlockSize();
      this.streamer.stat = DSS.APPEND;
      this.streamer.block = lastBlock.getBlock();
    } else {
      bytesCurBlock = 0;
      this.streamer.block = null;
      this.streamer.stat = DSS.CREATE;
    }
    this.resetBlockBuffer((int)this.bytesCurBlock);
  }

  /**
   * This is copied from DFSOutputStream.java
   * Create a socket for a write pipeline
   * @param first the first datanode 
   * @param length the pipeline length
   * @param client client
   * @return the socket connected to the first datanode
   */
  static Socket createSocketForPipeline(final DatanodeInfo first,
      final int length, final DFSClient client) throws IOException {
    final String dnAddr = first.getXferAddr(
        client.getConf().connectToDnViaHostname);
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Connecting to datanode " + dnAddr);
    }
    final InetSocketAddress isa = NetUtils.createSocketAddr(dnAddr);
    final Socket sock = client.socketFactory.createSocket();
    final int timeout = client.getDatanodeReadTimeout(length);
    NetUtils.connect(sock, isa, client.getRandomLocalInterfaceAddr(), client.getConf().socketTimeout);
    sock.setSoTimeout(timeout);
    if(HdfsConstants.getDataSocketSize() > 0)
      sock.setSendBufferSize(HdfsConstants.getDataSocketSize());
    sock.setTcpNoDelay(true);
    if(DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Send buf size " + sock.getSendBufferSize());
    }
    return sock;
  }
  
  static DFSRDMAOutputStream newStreamForCreate(DFSClient dfsClient, String src,
      FsPermission masked,EnumSet<CreateFlag> flag, boolean createParent,
      long blockSize, Progressable progress) throws Exception{
    final HdfsFileStatus stat;
    try{
      HybridLogicalClock mhlc = DFSClient.tickAndCopy();
      stat = dfsClient.namenode.create(src, masked, dfsClient.clientName,
          new EnumSetWritable<CreateFlag>(flag), createParent, (short)1,
          blockSize,mhlc);
      DFSClient.tickOnRecv(mhlc);
    }catch(RemoteException re){
      throw re.unwrapRemoteException(AccessControlException.class,
          DSQuotaExceededException.class,
          FileAlreadyExistsException.class,
          FileNotFoundException.class,
          ParentNotDirectoryException.class,
          NSQuotaExceededException.class,
          SafeModeException.class,
          UnresolvedPathException.class,
          SnapshotAccessControlException.class);
    }
    final DFSRDMAOutputStream out = new DFSRDMAOutputStream(dfsClient, src, progress,
        stat, dfsClient.getConf().rdmaWriterFlushSize);
    out.start();
    return out;
  }
  
  static DFSRDMAOutputStream newStreamForAppend(DFSClient dfsClient, String src,
      Progressable progress, LocatedBlock lastBlock,
      HdfsFileStatus stat) throws Exception{
    final DFSRDMAOutputStream out = new DFSRDMAOutputStream(dfsClient, src,
        progress, lastBlock, stat, dfsClient.getConf().rdmaWriterFlushSize);
    out.start();
    return out;
  }
 
  private int dataEndPos(){
    return this.fBlockBuffer.buffer.position();
  }
  
  private synchronized void checkAutoFlush() throws IOException{
    if(dataEndPos() - this.dataStartPos >= this.fAutoFlushSize)
      flushBuffer();
  }
  
  /* (non-Javadoc)
   * @see java.io.OutputStream#write(int)
   */
  @Override
  public synchronized void write(int b) throws IOException {
    if(dataEndPos() >= this.fBlockSize){
      flushAll();
    }
    this.fBlockBuffer.buffer.put((byte)b);
    pos++;
    this.curFileSize = Math.max(pos, this.curFileSize);
    this.checkAutoFlush();
  }

  @Override
  public void setDropBehind(Boolean dropCache) throws IOException, UnsupportedOperationException {
    // do nothing...
  }

  @Override
  // in FFFS, sync() == flush()
  public void sync() throws IOException {
    flushAll();
  }

  @Override
  public void hflush() throws IOException {
    flushAll();
  }

  @Override
  public void hsync() throws IOException {
    flushAll();
  }
  
  /* (non-Javadoc)
   * @see java.io.OutputStream#write(byte[])
   */
  @Override
  public void write(byte[] b) throws IOException {
    write(b,0,b.length);
  }

  /* (non-Javadoc)
   * @see java.io.OutputStream#write(byte[], int, int)
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if(dataEndPos() >= this.fBlockSize)flush();
    int writeLen = len;
    int writeOfst = off;
    while(writeLen > 0){
      int toWrite = (int)Math.min(writeLen, this.fBlockSize - dataEndPos()); 
      this.fBlockBuffer.buffer.put(b, writeOfst, toWrite);
      if(dataEndPos() >= this.fBlockSize)
      flush();
      writeLen -= toWrite;
      writeOfst += toWrite;
      pos += toWrite;
    }
    checkAutoFlush();
  }

  /**
   * Flush buffer to queue
   */
  public synchronized void flushBuffer(){
    if(this.dataStartPos < dataEndPos()){
      Packet pkt = new Packet();
      pkt.blkno = this.pos/this.fBlockSize;
      pkt.last = false;
      pkt.length = dataEndPos() - this.dataStartPos;
      pkt.offset = this.dataStartPos;
      pkt.seqno = this.currentSeqno ++;
      queuePacket(pkt);
      this.dataStartPos = dataEndPos();
    }
  }
  
  public synchronized void flushAll(){
    flushBuffer();
    synchronized(fDataQueue){
      // CLIENT: waiting for pkts been acked.
      while(!fAckQueue.isEmpty() || !fDataQueue.isEmpty()){
        try {
          fDataQueue.wait();
        } catch (InterruptedException e) {
          DFSClient.LOG.warn("Interrupted while flushBufferAndQueue waiting on fDataQueue.");
        }
      }
    }
  }
  
  /* (non-Javadoc)
   * @see java.io.OutputStream#flush()
   */
  @Override
  public synchronized void flush() throws IOException {
    flushAll();
  }

  /* (non-Javadoc)
   * @see java.io.OutputStream#close()
   */
  @Override
  public void close() throws IOException {
    if(streamer.bRunning){
      flushAll();
      synchronized(fDataQueue){
        this.streamer.stop();
      }
    }
  }

  @Override
  public int getCurrentBlockReplication() throws IOException {
    return 1;
  }

  @Override
  public void hsync(EnumSet<SyncFlag> syncFlags) throws IOException {
    flushAll();
  }

  private void resetBlockBuffer(int offset){
    this.fBlockBuffer.buffer.reset();
    this.dataStartPos = offset;
    this.fBlockBuffer.buffer.position(offset);
  }
  
  @Override
  public synchronized void seek(long newPos) throws IOException {
    if(newPos > this.curFileSize)
      throw new IOException("Cannot seek to pos:"+newPos+" because filesize is " + this.curFileSize);
    else if(pos/this.fBlockSize != newPos/this.fBlockSize || newPos < pos)
      flushAll(); // flushAll for block switch or write backward.
    else
      flushBuffer(); // or just flush the buffer to queue.
    pos = newPos;
    this.resetBlockBuffer((int)(pos % this.fBlockSize));
  }

  @Override
  public synchronized long getPos() throws IOException {
    return this.pos;
  }

  @Override
  protected void writeChunk(byte[] b, int offset, int len, byte[] checksum) throws IOException {
    // This should never be called
    throw new IOException("writeChunk is called in "+DFSRDMAOutputStream.class.getName());
  }

  @Override
  protected synchronized void checkClosed() throws IOException {
    if (closed) {
      throw new ClosedChannelException();
    }
  }
  
  private synchronized void start(){
    this.streamer.start();
  }
}
