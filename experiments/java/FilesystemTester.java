import java.io.PrintStream;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Class that implements correctness tests for file system.
 */
public class FilesystemTester extends Configured implements Tool {
  public static final int BLOCK_SIZE = 64*1024*1024;
  public static final int PACKET_SIZE = 16*1024;

  /**
   * Make starting test string.
   * @param testString Name of the test.
   * @return starting test string.
   */
  private static String getStartingTestString(String testString) {
    String res = testString + "\n";

    for (int i = 0; i < testString.length(); i++)
      res += "-";
    res += "\n";

    return res;
  }

  /**
   * Make ending test string.
   * @param testString Name of the test.
   * @return ending test string.
   */
  private static String getEndingTestString(String testString) {
    String res = "";

    for (int i = 0; i < testString.length(); i++)
      res += "-";
    res += "\n";

    return res;
  }

  /**
   * Fill packet with packetId bytes.
   * @param buf Buffer to fill.
   * @param packetId Packet ID to fill the buffer with.
   */
  private static void fillPacket(byte[] buf, int packetId) {
    byte b = (byte) ('a' + (packetId % 26));

    for (int i = 0; i < PACKET_SIZE; i++)
      buf[i] = b;
  }

  /**
   * Write file where the first PACKET_SIZE bytes are 0, the next PACKET_SIZE bytes are 1, and so on.
   * @param fsos     Output stream to write at.
   * @param nrBlocks Number of blocks to write.
   */
  private static void writeFile(FSDataOutputStream fsos, int nrBlocks) throws Exception {
    byte [] buf = new byte[PACKET_SIZE];
    int nrWrites = nrBlocks*(BLOCK_SIZE/PACKET_SIZE);

    for (int i = 0; i < nrWrites; i++) {
      fillPacket(buf, i);
      fsos.write(buf, 0, PACKET_SIZE);
    }
  }

  /**
   * Read specified packet and compare it with given buffer.
   * @param fsis     Input stream to read from.
   * @param pos      Position to read from.
   * @param checkBuf Buffer to compare with read value.
   */
  @Test
  private static void readAndValidatePacket(FSDataInputStream fsis, int pos, byte[] checkBuf) throws Exception {
    byte [] buf = new byte[PACKET_SIZE];
    int nread;

    fsis.seek(pos);
    nread = fsis.read(buf, 0, PACKET_SIZE);
    assertEquals("size is not equal to " + PACKET_SIZE, PACKET_SIZE, nread);
    assertArrayEquals("check buffer is different from the result", checkBuf, buf);
  }

  /**
   * Test regular Read and Write API of file system.
   * @param fs File system to check.
   * @param path1 Path to file which a simple "Hello World!!!" read and write is tested.
   * @param path2 Path to file which a few numbers of blocks are going to be read and written.
   */
  public void readAndWriteStandardFile(FileSystem fs, String path1, String path2) throws Exception {
    // Write hello world.
    FSDataOutputStream fsos = fs.create(new Path(path1));
    PrintStream ps = new PrintStream(fsos);
    String helloWorld = "Hello World!!!";

    ps.print(helloWorld);
    System.out.println("Simple \"" + helloWorld + "\" write is successful.");
    ps.close();
    fsos.close();

    // Read hello world.
    FSDataInputStream fsis = fs.open(new Path(path1));
    int readSize = helloWorld.getBytes().length;
    ByteBuffer bb = ByteBuffer.allocate(readSize);
    int nread;

    nread = fsis.read(bb);
    assertEquals("size is not equal to " + PACKET_SIZE, readSize, nread);
    assertArrayEquals("check buffer is different from the result", helloWorld.getBytes(), bb.array());
    System.out.println("Simple read of \"" + helloWorld + "\" is successful.");
    fsis.close();
    fs.delete(new Path(path1));

    // Write two blocks.
    int nrBlocks = 2;

    fsos = fs.create(new Path(path2));
    writeFile(fsos, nrBlocks);
    System.out.println("Two blocks worth of writes are successful.");
    fsos.close();

    // Read two packets one in each block.
    byte [] buf = new byte[PACKET_SIZE];
    int packetId;

    fsis = fs.open(new Path(path2));
    packetId = BLOCK_SIZE/PACKET_SIZE-1;
    fillPacket(buf, packetId);
    readAndValidatePacket(fsis, packetId*PACKET_SIZE, buf);
    System.out.println("Read last packet in first block is successful.");
    packetId += 2;
    fillPacket(buf, packetId);
    readAndValidatePacket(fsis, packetId*PACKET_SIZE, buf);
    System.out.println("Read second packet in second block is successful.");
    fsis.close();
    fs.delete(new Path(path2));
  }

  /**
   * File to test the overwrite capability of file system.
   * @param fs File system to test.
   * @param path Path to file to use for overwrite.
   */
  @Test
  public void overwriteFile(FileSystem fs, String path) throws Exception {
    // Write five blocks.
    FSDataOutputStream fsos = fs.create(new Path(path));
    int nrBlocks = 5;

    writeFile(fsos, nrBlocks);
    System.out.println("Five blocks worth of writes are successful.");
    fsos.close();

    // Make random writes.
    byte [] buf = new byte[PACKET_SIZE];
    int seekPos;

    fillPacket(buf, 10);
    fsos = fs.append(new Path(path));

    // Make one write in the beginning of the third block.
    seekPos = 2*BLOCK_SIZE;
    fsos.seek(seekPos);
    fsos.write(buf, 0, PACKET_SIZE);
    System.out.println("Overwrite packet at the beginning of the block is successful.");

    // Make one write at the middle of the fifth block.
    seekPos = 4*BLOCK_SIZE+32*PACKET_SIZE+128;
    fsos.seek(seekPos);
    fsos.write(buf, 0, PACKET_SIZE);
    System.out.println("Overwrite packet at the middle of the block is successful.");

    // Make one write at the end of the first block.
    seekPos = BLOCK_SIZE-PACKET_SIZE;
    fsos.seek(seekPos);
    fsos.write(buf, 0, PACKET_SIZE);
    System.out.println("Overwrite packet at the end of the block is successful.");

    fsos.close();

    // Make sure reads match writes.
    FSDataInputStream fsis = fs.open(new Path(path));

    seekPos = 2*BLOCK_SIZE;
    readAndValidatePacket(fsis, seekPos, buf);
    seekPos = 4*BLOCK_SIZE+32*PACKET_SIZE+128;
    readAndValidatePacket(fsis, seekPos, buf);
    seekPos = BLOCK_SIZE-PACKET_SIZE;
    readAndValidatePacket(fsis, seekPos, buf);

    fsis.close();
    fs.delete(new Path(path));
  }

  /**
   * Test reads with a specific timestamp (old interface).
   * @param fs File system to test.
   * @param path Path to file to use for timestamp read.
   */
  @Test
  public void readFromTimestamp(FileSystem fs, String path) throws Exception {
    FSDataOutputStream fsos;
    byte [] buf1 = new byte[PACKET_SIZE];
    byte [] buf2 = new byte[PACKET_SIZE];
    long ts1, ts2, ts3, ts4;

    for (int i = 0; i < PACKET_SIZE; i++) {
      buf1[i] = (byte) 1;
      buf2[i] = (byte) 2;
    }

    ts1 = System.currentTimeMillis();
    // System.out.println("TS1: " + ts1);
    TimeUnit.SECONDS.sleep(1);
    fsos = fs.create(new Path(path));
    TimeUnit.SECONDS.sleep(1);
    ts2 = System.currentTimeMillis();
    // System.out.println("TS2: " + ts2);
    TimeUnit.SECONDS.sleep(1);
    fsos.write(buf1, 0, PACKET_SIZE);
    fsos.hflush();
    TimeUnit.SECONDS.sleep(1);
    ts3 = System.currentTimeMillis();
    // System.out.println("TS3: " + ts3);
    TimeUnit.SECONDS.sleep(1);
    fsos.seek(0);
    fsos.write(buf2, 0, PACKET_SIZE);
    fsos.hflush();
    TimeUnit.SECONDS.sleep(1);
    ts4 = System.currentTimeMillis();
    // System.out.println("TS4: " + ts4);
    fsos.close();
    System.out.println("The two writes are successful.");

    // Read before creation of the file.
    FSDataInputStream fsis = fs.open(new Path(path), 4096, ts1, false);
    byte [] buf = new byte[PACKET_SIZE];
    int nread;

    nread = fsis.read(buf, 0, PACKET_SIZE);
    assertEquals("Should get -1 instead of " + nread, -1, nread);
    fsis.close();
    System.out.println("Read before creation of the file is successful");

    // Read after creation of the file.
    fsis = fs.open(new Path(path), 4096, ts2, false);
    nread = fsis.read(buf, 0, PACKET_SIZE);
    assertEquals("Should get -1 instead of " + nread, -1, nread);
    fsis.close();
    System.out.println("Read after creation of the file is successful");

    // Read after the first write.
    fsis = fs.open(new Path(path), 4096, ts3, false);
    nread = fsis.read(buf, 0, PACKET_SIZE);
    assertArrayEquals("Result is different from test buffer.", buf, buf1);
    fsis.close();
    System.out.println("Read after the first write is successful.");

    // Read after the second write.
    fsis = fs.open(new Path(path), 4096, ts4, false);
    nread = fsis.read(buf, 0, PACKET_SIZE);
    assertArrayEquals("Result is different from test buffer.", buf, buf2);
    fsis.close();
    System.out.println("Read after the second write is successful");

    // Delet file.
    fs.delete(new Path(path));
  }

  /**
   *    Test writes and reads PMU data.
   *    @param fs File system to test.
   *    @param path Path to file to use for PMU data.
   */
  @Test
  public void readAndWritePMUFile(FileSystem fs, String path) throws Exception {
    FSDataOutputStream fsos;
    FSDataInputStream fsis;
    int buffSize = 16;
    byte[] buff1 = new byte[buffSize];
    byte[] buff2 = new byte[buffSize];
    byte[] buff = new byte[buffSize];
    int nread;

    // Initialize buffer with character 'a'.
    for (int i = 0; i < buffSize; i++)
      buff1[i] = 'a';
    buff1[2] = 0;
    buff1[3] = (byte) buffSize;
    buff1[6] = 0;
    buff1[7] = 0;
    buff1[8] = 0;
    buff1[9] = 1;
    buff1[11] = 0;
    buff1[12] = 0;
    buff1[13] = 0;

    // Create file.
    fsos = fs.create(new Path(path));

    // Write packet 1.
    fsos.write(buff1,0,buffSize);
    fsos.seek(0);

    // Change timestamp and data.
    for (int i = 0; i < buffSize; i++)
      buff2[i] = 'b';
    buff2[2] = 0;
    buff2[3] = (byte) buffSize;
    buff2[6] = 0;
    buff2[7] = 0;
    buff2[8] = 0;
    buff2[9] = 3;
    buff2[11] = 0;
    buff2[12] = 0;
    buff2[13] = 0;

    // Write packet 2.
    fsos.write(buff2,0,buffSize);
    fsos.close();
    System.out.println("Write is successful.");

    // Read before first packet was written.
    fsis = fs.open(new Path(path + "@u999999"));
    nread = fsis.read(buff);
    fsis.close();
    if (nread != -1)
      throw new Exception("Should have read -1. Instead got " + nread + " bytes.");
    System.out.println("Read before the first write is successful.");

    // Read after first packet was written and before the second one.
    fsis = fs.open(new Path(path + "@u2000000"));
    nread = fsis.read(buff);
    fsis.close();
    assertArrayEquals("Result is different from the test buffer", buff, buff1);
    System.out.println("Read after the first write is successful.");

    // Read after second packet is written.
    fsis = fs.open(new Path(path + "@u4000000"));
    nread = fsis.read(buff);
    fsis.close();
    assertArrayEquals("Result is different from the test buffer", buff, buff2);
    System.out.println("Read after the second write is successful.");
    fs.delete(new Path(path));
  }

  /**
   * Run all tests.
   * @param args Directory of filesystem to use for tests.
   */
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    FileSystem fs = FileSystem.get(conf);
    String dirname, filename1, filename2, filename3, testname;

    if (args.length != 1) {
      System.out.println("Usage: hadoop jar FilesystemTester.java <dirname>");
      return 0;
    }
    dirname = args[0];
    filename1 = dirname + "/test1.txt";
    filename2 = dirname + "/test2.txt";
    filename3 = dirname + "/test3.se";

    testname = "Read and Write Standard File Test";
    System.out.print(getStartingTestString(testname));
    this.readAndWriteStandardFile(fs, filename1, filename2);
    System.out.println(getEndingTestString(testname));

    testname = "Overwrite File Test";
    System.out.print(getStartingTestString(testname));
    this.overwriteFile(fs, filename1);
    System.out.println(getEndingTestString(testname));

    testname = "Read from Timestamp Test";
    System.out.print(getStartingTestString(testname));
    this.readFromTimestamp(fs, filename1);
    System.out.println(getEndingTestString(testname));

    testname = "Read and Write PMU File Test";
    System.out.print(getStartingTestString(testname));
    this.readAndWritePMUFile(fs, filename3);
    System.out.println(getEndingTestString(testname));
    return 0;
  }

  public static void main(String[] args) {
    int res;

    ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    try {
      res = ToolRunner.run(new Configuration(), new FilesystemTester(), args);
      System.exit(res);
    } catch(Exception e) {
      System.out.println(e);
      e.printStackTrace();
    }
  }
}
