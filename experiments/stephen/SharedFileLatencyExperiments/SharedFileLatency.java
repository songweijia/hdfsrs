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
public class SharedFileLatency {

  public static void main(String[] args) {

   if (args.length == 0) {
       System.out.println("Usage: hadoop jar RW_Exp.jar {setup, run}) [args]\n\n"+
         "Example 1: To create a 2 MB file of random bytes:\n\t" +
         "hadoop jar RW_Exp.jar setup 2\n"+
         "Example 1: To time 3000 read/write requests of size 32KB with 75% reads:\n\t" +
         "hadoop jar RW_Exp.jar run 3000 32 0.75\n");
   } else if (args[0].equals("setup")) {
       if (args.length < 2) {
          System.out.println("Not Enough Arguments\n" +
            "Usage: hadoop jar RW_Exp.jar setup <File Size in MB>" );
          return;
       }
       setup(Integer.parseInt(args[1]));
   } else if (args[0].equals("run")) {
        if (args.length < 4) {
          System.out.println("Not Enough Arguments\n" +
            "Usage: hadoop jar RW_Exp.jar run <Number of Requests>"+
            " <Request Size (KB)>" + 
            " <Read/Write Ratio>" );
          return;
        }
        run(Integer.parseInt(args[1]), Integer.parseInt(args[2]), Double.parseDouble(args[3]));
    }
  }

  public static void run(int N_REQUESTS, int REQUEST_SIZE, double READ_RATIO) {

    try {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);

      //File to write to
      Path file = new Path("/file.txt");

      Random rand = new Random(System.currentTimeMillis());

      //32 kilobytes of random data
      byte[] randBuf = new byte[REQUEST_SIZE * 1024];
      rand.nextBytes(randBuf);

      FileStatus fileStat = fs.getFileStatus(file);
	  
      //Length of the file
      int FILE_SIZE = (int) fileStat.getLen();

      //Open reader
      FSDataInputStream reader = fs.open(file);

      long startTime = 0;
      long currentTime = 0; 
      int sleepTime = 1;//Time to sleep after failed write 

      for (int i=0; i < N_REQUESTS; i++) {
          int newIndex = rand.nextInt(FILE_SIZE - randBuf.length);
          if (rand.nextFloat() < READ_RATIO) {
              //Read 32 KB
			  startTime = System.currentTimeMillis();
              reader.seek(newIndex);
              reader.read(randBuf);
              //Print time to read
              currentTime = System.currentTimeMillis();
		      System.out.println(currentTime - startTime);
          } else {
              //Write 32 KB
              boolean success = false;
              startTime = System.currentTimeMillis();
              while (!success) {
                  try {
                    FSDataOutputStream writer = fs.append(file);
                    writer.seek(newIndex);
                    writer.write(randBuf);
                    writer.close();
                    //Print to write
                    currentTime = System.currentTimeMillis();
		            System.out.println(currentTime - startTime);

                    rand.nextBytes(randBuf);
                    sleepTime = Math.max(1, sleepTime - 1);
                    success = true;

                  } catch (org.apache.hadoop.ipc.RemoteException e) {
                    Thread.sleep(sleepTime);//sleep for 10 ms
                    sleepTime = Math.min(32, sleepTime * 2);
                  }
              }
          }
      }
      reader.close();

    } catch (Exception e) {
      System.out.println(e);
      e.printStackTrace();
    }
  }

  public static void setup(int FILE_SIZE) {
    try {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);

      //File to write to
      Path file = new Path("/file.txt");
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
            //System.out.println( "Wrote " + ((double) i / 1000.0) + " GB");
        }
      }

      out.close();
    } catch (Exception e) {
      System.out.println(e);
      e.printStackTrace();
    }
  }
}
