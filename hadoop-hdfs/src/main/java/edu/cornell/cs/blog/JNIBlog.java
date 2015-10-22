/**
 * 
 */
package edu.cornell.cs.blog;

import edu.cornell.cs.sa.HybridLogicalClock;

import java.nio.charset.*;

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
  
  /**
   * initialization
   * @param rank - rank of the current node
   * @param blockSize - block size for each block
   * @param pageSize - page size
   * @param persPath - initialize it
   * @return error code, 0 for success.
   */
  public native int initialize(int blockSize, int pageSize, String persPath);
  
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
    bl.initialize(1024*1024, 1024, "testbpid");
    writeLine(bl.hlc.toString());
    bl.testBlockCreation(mhlc);
    writeLine(bl.hlc.toString());
    bl.testBlockDeletion(mhlc);
    writeLine(bl.hlc.toString());
    rtc = bl.testWrite(mhlc);
    writeLine(bl.hlc.toString());
    bl.testRead();
    bl.testSnapshot(rtc);
    bl.destroy();
  }
}
