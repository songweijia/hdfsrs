import java.io.IOException;
import java.util.Random;
import java.lang.Thread;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.ipc.RemoteException;
public class MultipleFileThroughput {

  public static void main(String[] args) {

   if (args.length == 0) {
       System.out.println("Usage: hadoop jar RW_Exp.jar {setup, run}) [args]\n\n"+
         "Example 1: To create a 2 MB file of random bytes called example.txt:\n\t" +
         "hadoop jar RW_Exp.jar setup 2 example.txt\n"+
         "Example 1: To time 3000 read/write requests to/from example.txt of size 32KB with 75% reads:\n\t" +
         "hadoop jar RW_Exp.jar run 3000 32 0.75 example.txt\n");
   } else if (args[0].equals("setup")) {
       if (args.length < 3) {
          System.out.println("Not Enough Arguments\n" +
            "Usage: hadoop jar RW_Exp.jar setup <File Size in MB> <File Name>" );
          return;
       }
       setup(Integer.parseInt(args[1]), args[2]);
   } else if (args[0].equals("run")) {
        if (args.length < 5) {
          System.out.println("Not Enough Arguments\n" +
            "Usage: hadoop jar RW_Exp.jar run <Number of Requests>"+
            " <Request Size (KB)>" + 
            " <Read/Write Ratio>" + 			
            " <File Name>" );
          return;
        }
        run(Integer.parseInt(args[1]), Integer.parseInt(args[2]), Double.parseDouble(args[3]), args[4]);
    }
  }

  public static void run(int N_REQUESTS, int REQUEST_SIZE, double READ_RATIO, String filename) {

    try {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);

      //File to write to
      Path file = new Path(filename);

      Random rand = new Random(System.currentTimeMillis());

      //32 kilobytes of random data
      byte[] randBuf = new byte[REQUEST_SIZE * 1024];
      rand.nextBytes(randBuf);

      FileStatus fileStat = fs.getFileStatus(file);
	  
      //Length of the file
      int FILE_SIZE = (int) fileStat.getLen();

      //Open reader and writer
      FSDataInputStream reader = fs.open(file);
	  FSDataOutputStream writer = fs.append(file);

      long startTime = System.currentTimeMillis();

      for (int i=0; i < N_REQUESTS; i++) {
          int newIndex = rand.nextInt(FILE_SIZE - randBuf.length);
          if (rand.nextFloat() < READ_RATIO) {
              //Read 32 KB
              reader.seek(newIndex);
              reader.read(randBuf);
          } else {
              //Write 32 KB
              writer.seek(newIndex);
              writer.write(randBuf);
			  
              rand.nextBytes(randBuf);
              }
          }
      }
	  
	  System.out.println(System.currentTimeMillis() - startTime);
	  writer.close();
      reader.close();

    } catch (Exception e) {
      System.out.println(e);
      e.printStackTrace();
    }
  }

  public static void setup(int FILE_SIZE, String filename) {

    try {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);

      //File to write to
      Path file = new Path(filename);
      FSDataOutputStream out = fs.create(file);

      //Create a random byte array
      Random rand = new Random(System.currentTimeMillis());
      //1 MB
      byte[] mbBuffer = new byte[1048576];

      //Write 1GB to file
      for (int i=0; i<FILE_SIZE; i++) {
        rand.nextBytes(mbBuffer);
        out.write(mbBuffer);
        if (i % 100 == 0) {
            System.out.println( "Wrote " + ((double) i / 1000.0) + " GB");
        }
      }

      out.close();
    } catch (Exception e) {
      System.out.println(e);
      e.printStackTrace();
    }
  }
}
