/**
 * 
 */
package edu.cornell.cs.blog;

import edu.cornell.cs.sa.HybridLogicalClock;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemDatasetManager;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemDatasetManager.MemBlockMeta;

/**
 * @author Weijia
 * Blog = Block log
 */
/**
 * @author root
 *
 */
public class JNIBlog {
  static{
    System.loadLibrary("edu_cornell_cs_blog_JNIBlog");
  }

  static public long CURRENT_SNAPSHOT_ID = -1l;
  private HybridLogicalClock hlc;
  private long jniData;
  private String persPath;
  private String bpid; // initialize it!!!
  private MemDatasetManager dsmgr;
  final String BLOCKMAP_FILE = "bmap._dat";

  /**
   * Initialize both the block log and the block map.
   * @param blockSize
   * @param pageSize
   * @param persPath
   * @param port - the port number for RDMA based Blog
   */
  public void initialize(MemDatasetManager dsmgr, String bpid, int blockSize, int pageSize, String persPath, int port){
    this.dsmgr = dsmgr;
    this.bpid = bpid;
    this.persPath = persPath;
    // initialize blog
    initialize(1l<<30, (int)blockSize, pageSize, persPath, port);
    // LOAD blockmap
    loadBlockMap();
    Runtime.getRuntime().addShutdownHook(new Thread(){
      @Override
      public void run(){
        JNIBlog.this.destroy();
      }
    });
  }
  
  // write the block map to disk
  // format:
  // int: number of blocks
  // for each block:
  //   [long:block id][long:timestamp][boolean:isdeleted][int:state]
  private void flushBlockMap(){
    FileOutputStream fos = null;
    DataOutputStream dos = null;
    try{
      fos = new FileOutputStream(this.persPath+System.getProperty("file.separator")+BLOCKMAP_FILE);
      dos = new DataOutputStream(fos);
      dos.writeInt(this.blockMaps.size()); // write number of blocks
      for(Entry<Long, MemBlockMeta> entry: this.blockMaps.entrySet()){
        MemBlockMeta mbm = entry.getValue();
        dos.writeLong(entry.getKey()); // write block id
        dos.writeLong(mbm.getGenerationStamp()); // write timestamp
        dos.writeBoolean(mbm.isDeleted()); // write delete
        dos.writeInt(mbm.getState().getValue()); // write state
      }
    }catch(IOException ioe){
      //LOG error
      System.err.println("ERROR:Cannot open blockmap file for write:"+ioe);
    }finally{
      try{
        if(dos!=null)dos.close();
        if(fos!=null)fos.close();
      }catch(IOException e){
        //do nothing
      }
    }
  }
  // load the block map from disk
  private void loadBlockMap(){
    FileInputStream fis = null;
    DataInputStream dis = null;
    try{
      this.blockMaps = new HashMap<Long, MemBlockMeta>();
      fis = new FileInputStream(this.persPath+System.getProperty("file.separator")+BLOCKMAP_FILE);
      dis = new DataInputStream(fis);
      int nblock = dis.readInt();
      while(nblock-- > 0){
        long bid = dis.readLong();
        long gen = dis.readLong();
        boolean del = dis.readBoolean();
        ReplicaState rs = ReplicaState.getState(dis.readInt());
        MemBlockMeta mbm = dsmgr.new MemBlockMeta(this, gen, bid, rs);
        if(del)mbm.delete();
        this.blockMaps.put(bid, mbm);
      }
    }catch(IOException ioe){
      System.err.println("WARNING:Cannot open blockmap file for read: "+ioe+". This may be caused by a new startup");
    }finally{
      try{
        if(dis!=null)dis.close();
        if(fis!=null)fis.close();
      }catch(IOException e){
        //do nothing
      }
    }
  }
  
  /**
   * initialization
   * @param blockSize - block size for each block
   * @param poolSize - memory pool size
   * @param pageSize - page size
   * @param persPath - initialize it
   * @param port - server port for the RDMA datanode.
   * @return error code, 0 for success.
   */
  private native int initialize(long poolSize, int blockSize, int pageSize, String persPath, int port);
  
  /**
   * destroy the blog
   */
  public native void destroy();
  
  /**
   * create a block
   * @param mvc - vector clock of the driven message
   * @param blockId - block identifier
   * @return error code, 0 for success.
   */
  public native int createBlock(HybridLogicalClock mhlc, long blockId);
  
  /**
   * @param mvc - vector clock of the driven message
   * @param blockId - block identifier
   * @return error code, 0 for success.
   */
  public native int deleteBlock(HybridLogicalClock mhlc, long blockId);
  
  /**
   * @param blockId
   * @param rtc - 0 for the current version
   * @param blkOfst - block offset
   * @param bufOfst - buffer offset
   * @param length - how many bytes to read
   * @param buf[OUTPUT] - for read data
   * @return error code, number of bytes read for success.
   */
  public native int readBlock(long blockId, long rtc, int blkOfst, int bufOfst, int length, byte buf[]);
  
  /**
   * get number of bytes we have in the block
   * @param blockId
   * @param rtc
   * @return
   */
  public native int getNumberOfBytes(long blockId, long rtc);
  
  /**
   * @param mvc
   * @param blockId
   * @param blkOfst
   * @param bufOfst
   * @param length
   * @param buf
   * @return
   */
  public native int writeBlock(HybridLogicalClock mhlc, long blockId, int blkOfst, int bufOfst, int length, byte buf[]);
  
  /**
   * create a local realtime snapshot
   * @param eid log entry id, corresponding to the value in vector clock.
   * @return
   */
  public native int createSnapshot(long rtc);
  
  /**
   * readLocalRTC clock. all timestamp in the log should read
   * from here.
   * @return microseconds from the Epoch(1970-01-01 00:00:00 +0000(UTC))
   */
  public static native long readLocalRTC();
  
  
  //////////////////////////////////////////////////////////////////////
  // Move Pool Data to JNIBlog.
  public HashMap<Long,MemBlockMeta> blockMaps;
  //////////////////////////////////////////////////////////////////////
  
  //////////////////////////////////////////////////////////////////////
  // The interface for RDMA access library
  public static class RBPBuffer{
    public long handle; // handle to the RDMABufferPool
    public long offset; // offset of this buffer.
    public ByteBuffer buffer; // the buffer  
  }
  /**
   * function: initialize an RDMA Buffer Pool.
   * @param psz - size of the buffer pool = 1l<<psz.
   * @param align - alignment for the buffer/page allocation = 1l<<align
   * @param port - port for the blog server to listen for. ZERO FOR CLIENT.
   * @return handle of the RDMA Block Pool
   * @throws Exception
   */
  static public native long rbpInitialize(int psz, int align, int port) throws Exception;
  /**
   * function: destroy the RDMA buffer Pool.
   * @param size - size of the buffer tool
   * @throws Exception
   */
  static public native void rbpDestroy(long hRDMABufferPool) throws Exception;
  /**
   * function: allocate a block buffer.
   * @return allocated Block Buffer
   * @throws Exception
   */
  static public native RBPBuffer rbpAllocateBlockBuffer() throws Exception;
  /**
   * function: release a buffer.
   * @param buf
   * @throws Exception
   */
  static public native void rbpReleaseBuffer(RBPBuffer buf) throws Exception;
  /**
   * function: connect to RDMA datanode
   * @rbpBuffer handle of the buffer
   * @param hostIp
   * @param port
   * @throws Exception
   */
  static public native void rbpConnect(long rbpBuffer, byte hostIp[], int port) throws Exception;
  
  /**
   * function: do RDMA Write, this is called by the DataNode.
   * @param clientIp
   * @param offset
   * @param length
   * @param pageList
   * @throws Exception
   */
  static public native void rbpRDMAWrite(int clientIp, long offset, long length, long []pageList) throws Exception;
  
  //////////////////////////////////////////////////////////////////////
  // Tests.
  public void testBlockCreation(HybridLogicalClock mhlc)
  {
    for (long i = 0; i < 100; i++) {
      mhlc.tick();
      if (createBlock(mhlc,i) == 0)
        writeLine("Block " + i + " was created.");
    }
    for (long i = 4096; i < 4196; i++) {
      mhlc.tick();
      if (createBlock(mhlc,i) == 0)
        writeLine("Block " + i + " was created.");
    }
    mhlc.tick();
    if (createBlock(mhlc,1) == 0)
      writeLine("Block 1 was created.");
    mhlc.tick();
    if (createBlock(mhlc,4096) == 0)
      writeLine("Block 4096 was created.");
    mhlc.tick();
    if (createBlock(mhlc,4100) == 0)
      writeLine("Block 4100 was created.");
    mhlc.tick();
    if (createBlock(mhlc,5000) == 0)
      writeLine("Block 5000 was created.");
  }
  
  public void testBlockDeletion(HybridLogicalClock mhlc)
  { 
    for (long i = 0; i < 100; i++) {
      mhlc.tick();
      if (deleteBlock(mhlc,i) == 0)
        writeLine("Block " + i + " was deleted.");
    }
    mhlc.tick();
    if (deleteBlock(mhlc,1) == 0)
      writeLine("Block 1 was deleted.");
    mhlc.tick();
    if (deleteBlock(mhlc,4500) == 0)
      writeLine("Block 4500 was deleted.");
    mhlc.tick();
    if (deleteBlock(mhlc,4100) == 0)
      writeLine("Block 4100 was deleted.");
  }
  
  public long testWrite(HybridLogicalClock mhlc) throws InterruptedException
  {
    String a = "Hello Theo & Weijia. I am working well!!";
    String b = "This should not be in the snapshot read.";
    String c = "Laaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.";
    long rtc;

    mhlc.tick();
    if (writeBlock(mhlc, 4101, 0, 0, a.length(), a.getBytes()) == 0)
      writeLine("Block 4101 was written.");
    mhlc.tick();
    if (writeBlock(mhlc, 4100, 0, 0, 20, a.getBytes()) == 0)
      writeLine("Block 4100 was written.");
    mhlc.tick();
    if (writeBlock(mhlc, 4101, 100, 0, 20, a.getBytes()) == 0)
      writeLine("Block 4101 was written.");
    writeLine("Before Snapshot: " + hlc.toString());
    Thread.sleep(1);
    rtc = readLocalRTC();
    writeLine("RTC: " + new Long(rtc).toString());
    createSnapshot(rtc);
    writeLine("After Snapshot: " + hlc.toString());
    mhlc.tick();
    if (writeBlock(mhlc, 4101, 0, 0, b.length(), b.getBytes()) == 0)
      writeLine("Block 4101 was written.");
    mhlc.tick();
    if (writeBlock(mhlc, 5000, 0, 0, c.length(), c.getBytes()) == 0)
      writeLine("Block 5000 was written.");
    
    return rtc;
  }
  
  public void testRead()
  {
    byte[] mybuf = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".getBytes();

    readBlock(4100, -1, 0, 0, 20, mybuf);
    readBlock(4101, -1, 100, 0, 20, mybuf);
    readBlock(4101, -1, 0, 0, 40, mybuf);
    writeLine("Read: " + new String(mybuf, Charset.forName("UTF-8")));
  }
  
  public void testSnapshot(long rtc)
  {
    byte[] mybuf = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".getBytes();
    long cut;
    int nrBytes;
    
    readBlock(4101, rtc, 0, 0, 40, mybuf);
    writeLine("Read Snapshot: " + new String(mybuf, Charset.forName("UTF-8")));
    nrBytes = getNumberOfBytes(5000,rtc);
    writeLine("Bytes: " + nrBytes);
    if (readBlock(5000, rtc, 0, 0, 40, mybuf) != -3)
      writeLine("There is a problem here.");
    if (readBlock(5001, rtc, 0, 0, 40, mybuf) != -1)
      writeLine("There is a problem here.");
  }
  
  static public void writeLine(String str)
  {
    System.out.println(str);
    System.out.flush();
  }
  
  /**
   * Test stub
   * @param args
   */
  public static void main(String[] args) throws InterruptedException
  {
    JNIBlog bl = new JNIBlog();
    HybridLogicalClock mhlc = new HybridLogicalClock();
    long rtc;
  
    writeLine("Begin Initialize.");
    bl.initialize(null,null,1024*1024, 1024, "testbpid", 0); // TODO: modify
/*
    writeLine(bl.hlc.toString());
    bl.testBlockCreation(mhlc);
    writeLine(bl.hlc.toString());
    bl.testBlockDeletion(mhlc);
    writeLine(bl.hlc.toString());
    rtc = bl.testWrite(mhlc);
    writeLine(bl.hlc.toString());
*/
    bl.testRead();
//    bl.testSnapshot(rtc);
    bl.testSnapshot(1445569148646l);
//    bl.destroy();
  }
}
