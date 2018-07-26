import java.io.PrintStream;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.nio.ByteOrder;
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
public class PMUTest extends Configured implements Tool {
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

  private static long longFromArray(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);

    buffer.order(ByteOrder.BIG_ENDIAN);
    return buffer.getLong();
  }

  private static byte[] fillZeros(byte[] payload, int offset, int length) {
    for (int i = 0; i < length; i++)
      payload[offset+i] = 0;
    return payload;
  }

  /**
   *    Test writes and reads PMU data.
   *    @param fs File system to test.
   *    @param path Path to file to use for PMU data.
   */
  @Test
  public void readAndValidatePMUTimestamp(FileSystem fs, String path, long ts) throws Exception {
    FSDataInputStream fsis = fs.open(new Path(path + "@u" + Long.toString(ts)));
    int buffsize = 24;
    byte[] buff = new byte[buffsize];
    byte[] tempBuff;
    int nread = fsis.read(buff);
    long s, us, res;

    fsis.close();
    tempBuff = fillZeros(Arrays.copyOfRange(buff,2,10),0,4);
    s = longFromArray(tempBuff);
    tempBuff = fillZeros(Arrays.copyOfRange(buff,6,14),0,5);
    us = longFromArray(tempBuff);
    res = 1000000*s + us;
    if (res != ts) {
      System.err.println("Received " + Long.toString(res) + " instead of " + Long.toString(ts));
      return;
    }
    System.out.println("Timestamps was found successfully.");
  }

  /**
   * Run all tests.
   * @param args Directory of filesystem to use for tests.
   */
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    FileSystem fs = FileSystem.get(conf);
    String filename, testname;
    long ts;

    if (args.length != 2) {
      System.out.println("Usage: hadoop jar PMUTest.java <filename> <timestamp>");
      return 0;
    }
    filename = args[0];
    ts = Long.parseLong(args[1]);

    testname = "Read and Validate PMU timestamp";
    System.out.print(getStartingTestString(testname));
    this.readAndValidatePMUTimestamp(fs, filename, ts);
    System.out.println(getEndingTestString(testname));

    return 0;
  }

  public static void main(String[] args) {
    int res;

    ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    try {
      res = ToolRunner.run(new Configuration(), new PMUTest(), args);
      System.exit(res);
    } catch(Exception e) {
      System.out.println(e);
      e.printStackTrace();
    }
  }
}
