import java.io.PrintStream;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.net.Socket;
import java.nio.ByteBuffer;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;

public class DataCollector extends Configured implements Tool
{
  public static void main(String[] args) {
    int res;

    try {
      res = ToolRunner.run(new Configuration(), new DataCollector(), args);
      System.exit(res);
    } catch(Exception e) {
      System.out.println(e);
      e.printStackTrace();
    }
  }

  public class Worker extends Thread
  {
    private FileSystem fs;
    private String hostname;
    private int pmuID; 

    public Worker(FileSystem fs, String hostname, int pmuID) {
      this.fs = fs;
      this.hostname = hostname;
      this.pmuID = pmuID;
    }

    @Override
    public void run() {
      Socket client;
      DataInputStream input;
      FSDataOutputStream os;
      byte[] byteArray = new byte[65536];
      int datasize;

      try {
        os = fs.create(new Path(pmuID + ".pmu"));
      } catch (IOException e) {
        System.out.println(e);
        return;
      }
      try {
        client = new Socket(hostname,10000+pmuID);
      } catch (Exception e) {
        System.out.println(e);
        return;
      }

      try {
        input = new DataInputStream(client.getInputStream());
      } catch (IOException e) {
        System.out.println(e);
        return;
      }

      while (true) {
        try {
          input.read(byteArray, 0, 4);
          datasize = byteArray[2]*256 + byteArray[3];
          input.read(byteArray, 4, datasize-4);
          os.write(byteArray, 0, datasize);
          os.seek(0);
        } catch (IOException e) {
          System.out.println(e);
          try {
            os.close();
          } catch (IOException e2) {
            System.out.println(e2);
            return;
          }
          return;
        }
      }
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    FileSystem fs = FileSystem.get(conf);
    String hostname;
    int nrPMUs, collectorID, nrCollectors, nrWorkers;
    Worker[] workers;
    int workerId = 0;

    if (args.length != 4) {
      System.out.println("Usage: hadoop jar DataCollector <server IP> <nr PMUs> <Collector ID> <nr Collectors>");
      return -1;
    }
    hostname = args[0];
    nrPMUs = Integer.parseInt(args[1]);
    collectorID = Integer.parseInt(args[2]);
    nrCollectors = Integer.parseInt(args[3]);
    if (nrPMUs % nrCollectors >= collectorID)
      nrWorkers = nrPMUs / nrCollectors + 1;
    else
      nrWorkers = nrPMUs / nrCollectors;
    workers = new Worker[nrWorkers];
    for (int i = 0; i < nrPMUs; i++) {
      if ((i % nrCollectors) == collectorID) {
        System.out.println("Started Collector for PMU " + (i+1));
        workers[workerId] = new Worker(fs,hostname,i+1);
        workers[workerId].start();
        workerId++;
      }
    }

    for (int i = 0; i < nrWorkers; i++)
      workers[i].join();
    return 0;
  }
}
