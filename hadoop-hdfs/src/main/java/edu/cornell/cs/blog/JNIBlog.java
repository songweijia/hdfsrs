/**
 * 
 */
package edu.cornell.cs.blog;
import edu.cornell.cs.sa.*;

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
    System.loadLibrary("JNIBlog");
  }
  
  private long jniData = 0;
  
  /**
   * initialization
   * @param nNode - number of nodes in the system including
   *  namenode and datanodes.
   * @param blockSize - block size for each block
   * @param pageSize - page size
   * @return error code, 0 for success.
   */
  public native int initialize(
      int nNode,int blockSize,int pageSize);
  
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
   * @return error code, 0 for success.
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
   * create a local realtime snapshot
   * @param rtc realtime clock as identifier of the snapshot
   * @param eid log entry id, corresponding to the value
   *   in vector clock.
   * @return
   */
  public native int createSnapshot(long rtc, long eid);
  
  /**
   * delete a snapshot
   * @param rtc
   * @return
   */
  public native int deleteSnapshot(long rtc);
  
  /////////////////////
  // tools
  /**
   * readLocalRTC clock. all timestamp in the log should read
   * from here.
   * @return microseconds from the Epoch(1970-01-01 00:00:00 +0000(UTC))
   */
  public native long readLocalRTC();
  /////////////////////
  
  /**
   * Test stub
   * @param args
   */
  public static void main(String[] args){
    JNIBlog bl = new JNIBlog();
    bl.initialize(10, 1024*1024, 1024);
    long rtc = bl.readLocalRTC();
    long sec = rtc >> 32;
    long usec = rtc&0xfffffff;
    System.out.println("Current Time="+sec+"."+usec);
    System.out.println("pointer="+Long.toHexString(bl.jniData));
  }
}
