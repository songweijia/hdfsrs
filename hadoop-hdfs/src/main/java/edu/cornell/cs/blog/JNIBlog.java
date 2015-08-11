/**
 * 
 */
package edu.cornell.cs.blog;
import edu.cornell.cs.sa.*;
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
  private VectorClock vc;
  private long jniData;

  /**
   * @return my rank
   */
  public int getMyRank(){
    return vc.pid;
  }
  
  /**
   * initialization
   * @param rank - rank of the current node
   * @param blockSize - block size for each block
   * @param pageSize - page size
   * @return error code, 0 for success.
   */
  public native int initialize(int rank, int blockSize, int pageSize);
  
  /**
   * create a block
   * @param mvc - vector clock of the driven message
   * @param blockId - block identifier
   * @return error code, 0 for success.
   */
  public native int createBlock(VectorClock mvc,
      long blockId);
  
  /**
   * @param mvc - vector clock of the driven message
   * @param blockId - block identifier
   * @return error code, 0 for success.
   */
  public native int deleteBlock(VectorClock mvc,
      long blockId);
  
  /**
   * @param blockId
   * @param rtc - 0 for the current version
   * @param blkOfst - block offset
   * @param bufOfst - buffer offset
   * @param length - how many bytes to read
   * @param buf[OUTPUT] - for read data
   * @return error code, number of bytes read for success.
   */
  public native int readBlock(long blockId,
      long rtc, int blkOfst, int bufOfst,
      int length, byte buf[]);
  
  /**
   * get number of bytes we have in the block
   * @param blockId
   * @param rtc
   * @return
   */
  public native int getNumberOfBytes(long blockId,
      long rtc);
  
  /**
   * @param mvc
   * @param blockId
   * @param blkOfst
   * @param bufOfst
   * @param length
   * @param buf
   * @return
   */
  public native int writeBlock(VectorClock mvc,
      long blockId, int blkOfst, int bufOfst,
      int length, byte buf[]);
  
  /**
   * Get the vc of the latest log before rtc.
   * @param rtc realtime clock for expected snapshot
   * @param vc[OUTPUT] vector clock
   * @return
   */
  public native int since(long rtc, VectorClock vc);
  
  /**
   * Get the vc of the latest log before rtc.
   * @param rtc realtime clock for expected snapshot
   * @param nnrank rank for the namenode
   * @param nnver version for the vectorclock
   * @param vc[OUTPUT] vector clock
   * @return
   */
  public native int since(long rtc, int nnrank, long nnver, VectorClock vc);
  
  /**
   * create a local realtime snapshot
   * @param rtc realtime clock as identifier of the snapshot
   * @param eid log entry id, corresponding to the value
   *   in vector clock.
   * @return
   */
  public native int createSnapshot(long rtc, long eid);
  
  /**
   * readLocalRTC clock. all timestamp in the log should read
   * from here.
   * @return microseconds from the Epoch(1970-01-01 00:00:00 +0000(UTC))
   */
  public static native long readLocalRTC();
  
  // Tests.
  public void testBlockCreation(VectorClock mvc)
  {
    for (long i = 0; i < 100; i++) {
      mvc.tick();
      if (createBlock(mvc,i) == 0)
        System.out.println("Block " + i + " was created.");
    }
    for (long i = 4096; i < 4196; i++) {
      mvc.tick();
      if (createBlock(mvc,i) == 0)
        System.out.println("Block " + i + " was created.");
    }
    mvc.tick();
    if (createBlock(mvc,1) == 0)
      System.out.println("Block 1 was created.");
    mvc.tick();
    if (createBlock(mvc,4096) == 0)
      System.out.println("Block 4096 was created.");
    mvc.tick();
    if (createBlock(mvc,4100) == 0)
      System.out.println("Block 4100 was created.");
    mvc.tick();
    if (createBlock(mvc,5000) == 0)
      System.out.println("Block 5000 was created.");
  }
  
  public void testBlockDeletion(VectorClock mvc)
  {
    for (long i = 0; i < 100; i++) {
      mvc.tick();
      if (deleteBlock(mvc,i) == 0)
        System.out.println("Block " + i + " was deleted.");
    }
    mvc.tick();
    if (deleteBlock(mvc,1) == 0)
      System.out.println("Block 1 was deleted.");
    mvc.tick();
    if (deleteBlock(mvc,4500) == 0)
      System.out.println("Block 4500 was deleted.");
    mvc.tick();
    if (deleteBlock(mvc,4100) == 0)
      System.out.println("Block 4100 was deleted.");
  }
  
  public long testWrite(VectorClock mvc)
  {
    String a = "Hello Theo & Weijia. I am working well!!";
    String b = "This should not be in the snapshot read.";
    String c = "Laaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.";
    long res;

    mvc.tick();
    if (writeBlock(mvc, 4101, 0, 0, a.length(), a.getBytes()) == 0)
      System.out.println("Block 4101 was written.");
    mvc.tick();
    if (writeBlock(mvc, 4100, 0, 0, 20, a.getBytes()) == 0)
      System.out.println("Block 4100 was written.");
    mvc.tick();
    if (writeBlock(mvc, 4101, 100, 0, 20, a.getBytes()) == 0)
      System.out.println("Block 4101 was written.");
    res = readLocalRTC();
    mvc.tick();
    if (writeBlock(mvc, 4101, 0, 0, b.length(), b.getBytes()) == 0)
      System.out.println("Block 4101 was written.");
    if (writeBlock(mvc, 5000, 0, 0, c.length(), c.getBytes()) == 0)
      System.out.println("Block 5000 was written.");
    
    return res;
  }
  
  public void testRead()
  {
    byte[] mybuf = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".getBytes();

    readBlock(4100, -1, 0, 0, 20, mybuf);
    readBlock(4101, -1, 100, 0, 20, mybuf);
    readBlock(4101, -1, 0, 0, 40, mybuf);
    System.out.println("Read: " + new String(mybuf, Charset.forName("UTF-8")));
  }
  
  public void testSnapshot(long rtc)
  {
    VectorClock mvc = new VectorClock(2);
    byte[] mybuf = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".getBytes();
    long cut;
    int nrBytes;
    
    System.out.println("VC: " + mvc.toString());
    since(rtc,1,310,mvc);
    System.out.println("VC: " + mvc.toString());
    cut = mvc.vc.get(0);
    System.out.println("Cut: " + cut);
    createSnapshot(rtc, cut);
    readBlock(4101, rtc, 0, 0, 40, mybuf);
    System.out.println("Read Snapshot: " + new String(mybuf, Charset.forName("UTF-8")));
    nrBytes = getNumberOfBytes(5000,rtc);
    System.out.println("Bytes: " + nrBytes);
    if (readBlock(5000, rtc, 0, 0, 40, mybuf) != -3)
      System.out.println("There is a problem here.");
    if (readBlock(5001, rtc, 0, 0, 40, mybuf) != -1)
      System.out.println("There is a problem here.");
  }
  
  /**
   * Test stub
   * @param args
   */
  public static void main(String[] args){
    JNIBlog bl = new JNIBlog();
    VectorClock mvc = new VectorClock(1);
    long rtc;
    
    bl.initialize(0, 1024*1024, 1024);
    // System.out.println("pointer="+Long.toHexString(bl.jniData));
    System.out.println(bl.vc);
    bl.testBlockCreation(mvc);
    System.out.println(bl.vc);
    bl.testBlockDeletion(mvc);
    System.out.println(bl.vc);
    rtc = bl.testWrite(mvc);
    System.out.println(bl.vc);
    bl.testRead();
    bl.testSnapshot(rtc);
  }
}
