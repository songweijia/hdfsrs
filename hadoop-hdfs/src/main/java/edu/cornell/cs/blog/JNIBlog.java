package edu.cornell.cs.blog;

import edu.cornell.cs.sa.HybridLogicalClock;
import java.nio.charset.*;

public class JNIBlog
{
  static {
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
   * @return error code, 0 for success.
   */
  public native int initialize(int blockSize, int pageSize);
  
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
   * @param blkOfst - block offset
   * @param bufOfst - buffer offset
   * @param length - how many bytes to read
   * @param buf[OUTPUT] - for read data
   * @return error code, number of bytes read for success.
   */
  public int readBlock(long blockId, int blkOfst, int bufOfst, int length, byte buf[])
  {
	return readBlock(blockId, hlc.r, hlc.c, blkOfst, bufOfst, length, buf);
  }
  
  /**
   * @param blockId
   * @param r - real time component
   * @param l - logical time component
   * @param blkOfst - block offset
   * @param bufOfst - buffer offset
   * @param length - how many bytes to read
   * @param buf[OUTPUT] - for read data
   * @return error code, number of bytes read for success.
   */
  private native int readBlock(long blockId, long r, long l, int blkOfst, int bufOfst, int length, byte buf[]);
  
  /**
   * @param blockId
   * @param t - time to read from
   * @param blkOfst - block offset
   * @param bufOfst - buffer offset
   * @param length - how many bytes to read
   * @param buf[OUTPUT] - for read data
   * @return error code, number of bytes read for success.
   */
  public native int readBlock(long blockId, long t, int blkOfst, int bufOfst, int length, byte buf[]);
  
  /**
   * get number of bytes we have in the block
   * @param blockId
   * @return
   */
  public int getNumberOfBytes(long blockId)
  {
	  return getNumberOfBytes(blockId, hlc.r, hlc.c);
  }
  
  /**
   * get number of bytes we have in the block
   * @param blockId
   * @param r
   * @param l
   * @return
   */
  private native int getNumberOfBytes(long blockId, long r, long l); 
  
  /**
   * get number of bytes we have in the block
   * @param blockId
   * @param t
   * @return
   */
  public native int getNumberOfBytes(long blockId, long t);
  
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
      assert (createBlock(mhlc,i) == 0);
      writeLine("Block " + i + " was created.");
    }
    for (long i = 4096; i < 4196; i++) {
      assert (createBlock(mhlc,i) == 0);
      writeLine("Block " + i + " was created.");
    }
    assert (createBlock(mhlc,1) == -1);
    assert (createBlock(mhlc,4096) == -1);
    assert (createBlock(mhlc,4100) == -1);
    assert (createBlock(mhlc,5000) == 0);
    writeLine("Block 5000 was created.");
  }
  
  public void testBlockDeletion(HybridLogicalClock mhlc)
  { 
    for (long i = 0; i < 100; i++) {
      assert (deleteBlock(mhlc,i) == 0);
      writeLine("Block " + i + " was deleted.");
    }
    assert (deleteBlock(mhlc,1) == -1);
    assert (deleteBlock(mhlc,4500) == -1);
    assert (deleteBlock(mhlc,4100) == 0);
    writeLine("Block 4100 was deleted.");
  }
  
  public long testWrite(HybridLogicalClock mhlc) throws InterruptedException
  {
    String a = "Hello Theo & Weijia. I am working well!!";
    String b = "This should not be in the snapshot read.";
    String c = "Laaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.";
    long rtc;

    assert (writeBlock(mhlc, 4101, 0, 0, a.length(), a.getBytes()) == 0);
    writeLine("Block 4101 was written.");
    assert (writeBlock(mhlc, 4100, 0, 0, 20, a.getBytes()) == -1);
    assert (writeBlock(mhlc, 4101, 100, 0, 20, a.getBytes()) == -2);
    Thread.sleep(1);
    rtc = readLocalRTC();
    createSnapshot(rtc);
    assert (writeBlock(mhlc, 4101, 0, 0, b.length(), b.getBytes()) == 0);
    writeLine("Block 4101 was written.");
    assert (writeBlock(mhlc, 5000, 0, 0, c.length(), c.getBytes()) == 0);
    writeLine("Block 5000 was written.");
    
    return rtc;
  }
  
  public void testRead()
  {
	String a = "Hello Theo & Weijia. I am working well!!";
	String b = "This should not be in the snapshot read.";
	String c = "Laaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.";
	String temp;
    byte[] mybuf = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".getBytes();

    assert (readBlock(4100, 0, 0, 40, mybuf) == -1);
    assert (readBlock(4101, 100, 0, 40, mybuf) == -2);
    assert (readBlock(4101, 0, 0, 40, mybuf) == 40);
    temp = new String(mybuf, Charset.forName("UTF-8"));
    writeLine("Block 4101: " + temp);
    assert (b.compareTo(temp) == 0);
  }
  
  public void testSnapshot(long rtc)
  {
	String a = "Hello Theo & Weijia. I am working well!!";
	String b = "This should not be in the snapshot read.";
	String c = "Laaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.";
	String temp;
    byte[] mybuf = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".getBytes();
    long cut;
    int nrBytes;
    
    assert (readBlock(4101, rtc+10000, 0, 0, 40, mybuf) == -1);
    assert (readBlock(5001, rtc, 0, 0, 40, mybuf) == -2);
    assert (readBlock(5000, rtc, 0, 0, 40, mybuf) == -3);
    assert (readBlock(4101, rtc, 0, 0, 40, mybuf) == 40);
    temp = new String(mybuf, Charset.forName("UTF-8"));
    writeLine("Snapshot " + Long.toString(rtc)  + ", Block 4101: " + temp);
    assert (a.compareTo(temp) == 0);
    assert (getNumberOfBytes(4101,rtc) == 40);
    assert (getNumberOfBytes(5000) == 40);
  }
  
  static public void writeLine(String str) {
    System.out.println(str);
    System.out.flush();
  }
  
  /**
   * Test stub
   * @param args
   */
  public static void main(String[] args) throws InterruptedException{
    JNIBlog bl = new JNIBlog();
    HybridLogicalClock mhlc = new HybridLogicalClock();
    long rtc;
    
    writeLine("Initialize.");
    bl.initialize(1024*1024, 1024);
    writeLine("Create Blocks.");
    bl.testBlockCreation(mhlc);
    writeLine("Delete Blocks.");
    bl.testBlockDeletion(mhlc);
    writeLine("Write Blocks.");
    rtc = bl.testWrite(mhlc);
    Thread.sleep(2);
    writeLine("Read Blocks.");
    bl.testRead();
    writeLine("Read Snapshot Blocks.");
    bl.testSnapshot(rtc);
  }
}