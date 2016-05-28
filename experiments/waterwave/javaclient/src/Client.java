import java.io.EOFException;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.util.GenericOptionsParser;

public class Client extends Thread {
  final int sleepInt;
  final int writeInt;
  final int bufferSize;
  final long timeLimit;
  final Configuration conf;
  final Path file;
  final Path filldata;
  final FileSystem fs;
  final short si;
  final short sj;
  final short ei;
  final short ej;
  final String op;
  final long starttime;

  final int readsize;
  final byte[] readbuf;
  FSDataInputStream in;
  FSDataOutputStream out;

  public Client(Configuration conf, short si, short sj, short ei, short ej, Path filldata, String op, long starttime) throws Exception {
    this.bufferSize = conf.getInt("test.buffersize", 4096);
    this.timeLimit = conf.getLong("test.timelimit", 2000);
    this.writeInt = conf.getInt("test.writeinterval", 50);
    this.sleepInt = writeInt / 10;
    this.fs = FileSystem.get(conf);
    this.conf = conf;
    this.si = si;
    this.sj = sj;
    this.ei = ei;
    this.ej = ej;
    this.file = new Path("/data", "part_" + si + "_" + sj + "_" + ei + "_" + ej);
    this.filldata = filldata;
    this.op = op;
    this.starttime = starttime;
    
    this.readsize = 4*(ei-si)*(ej-sj);
    this.readbuf = new byte[this.readsize];
    this.in = fs.open(filldata.suffix("_" + String.valueOf(si)+"_" + String.valueOf(sj)+"_"+String.valueOf(ei)+"_"+String.valueOf(ej)), bufferSize);
    this.out = fs.create(file, true, bufferSize);
  }

  public void run() {
    try {
      if (this.op.equals("FFFSwrite")) {
        in.readFully(readbuf);
        out.write(readbuf, 0, readsize);
        out.hsync();
        in.seek(0);
        out.seek(0);
      }
      while (System.currentTimeMillis() < starttime) {
        sleep(sleepInt);
      }

      //System.out.println("timelimt:" + timeLimit + " buffersize:" + bufferSize + " timestamp:" + System.currentTimeMillis());
      if (this.op.equals("FFFSwrite")) {
        FFFSWrite();
      } else {
        HDFSWrite();
      }
      System.out.println("(" + si + "," + sj + "," + ei + "," + ej + ") finish timestamp:" + System.currentTimeMillis());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void FFFSWrite() throws Exception {
    long step = 0, count = 1;
    while (step < timeLimit) {
      /*
      if (step % 500 == 0) {
        System.out.println("(" + si + "," + sj + "," + ei + "," + ej + ") step " + step);
      }
      */
      try {
        step += count;
        while (count > 0) {
          in.readFully(readbuf);
	/*
        String[] items = in.readLine().split(" ");
        int k = 0;
        for (int i = si; i < ei; i++)
          for (int j = sj; j < ej; j++) {
            height = Float.parseFloat(items[k++]);
            out.writeFloat(height);
          }
	*/
          count--;
        }
        out.write(readbuf, 0, readsize);
        out.hsync();
      } catch (Exception e) {
        e.printStackTrace();
        break;
      }
      out.seek(0);
      while (System.currentTimeMillis() - starttime < writeInt*step) {
      	Thread.sleep(sleepInt);
      }
      count = (System.currentTimeMillis() - starttime) / writeInt;
      count = (count < timeLimit) ? count + 1 - step : timeLimit - step;
    }
    in.close();
    out.close();
  }

  public void HDFSWrite() throws Exception {
    long step = 0, count = 1;
    while (step < timeLimit) {
      /*
      if (step % 500 == 0) {
        System.out.println("(" + si + "," + sj + "," + ei + "," + ej + ") step " + step);
      }
      */
      try {
        step += count;
        while (count > 0) {
          in.readFully(readbuf);
        /*
        String[] items = in.readLine().split(" ");
        int k = 0;
        for (int i = si; i < ei; i++)
          for (int j = sj; j < ej; j++) {
            height = Float.parseFloat(items[k++]);
            out.writeFloat(height);
          }
        */
          //out.write(readbuf, 0, readsize);
          count--;
        }
        out.write(readbuf, 0, readsize);
        out.close();
      } catch(Exception e) {
        e.printStackTrace();
        break;
      }
      out = fs.append(file, bufferSize);
      while (System.currentTimeMillis() - starttime < writeInt*step) {
        Thread.sleep(sleepInt);
      }
      count = (System.currentTimeMillis() - starttime) / writeInt;
      count = (count < timeLimit) ? count + 1 - step : timeLimit - step;
    }
    in.close();
    out.close();
  }

  public static void read(Configuration conf, Path dir, short id, short m, short n, long t) throws Exception {
    final FileSystem fs = FileSystem.get(conf);
    FileStatus[] files = fs.listStatus(dir);
    float mat[][] = new float[m][n];
    
    int si, sj, ei, ej;
    for (int k = 0; k < files.length; k++) {
      FSDataInputStream in = null;
      if (t == 0) in = fs.open(files[k].getPath());
      else in = fs.open(files[k].getPath(), conf.getInt("test.buffersize", 4096), t, false);
      
      String[] names = files[k].getPath().getName().split("_");
      si = Short.parseShort(names[1]);
      sj = Short.parseShort(names[2]);
      ei = Short.parseShort(names[3]);
      ej = Short.parseShort(names[4]);
      try {
        if (t == 0) in.seek(files[k].getLen()-4*(ei-si)*(ej-sj));
        for (int i = si; i < ei; i++)
          for (int j = sj; j < ej; j++)
            mat[i][j] = in.readFloat();
      } catch (Exception e) {
	System.out.println("file:" + files[k].getPath().toString() + " len:" + files[k].getLen() + " size:" + (4*(ei-si)*(ej-sj)));
      } finally {
        in.close();
      }
    }
    
    FileOutputStream out = new FileOutputStream(new File("data1/data" + id));
    for (si = 0; si < m; si++) {
      StringBuilder sb = new StringBuilder();
      sb.append(mat[si][0]);
      for (sj = 1; sj < n; sj++) {
        sb.append(" ").append(mat[si][sj]);
      }
      sb.append("\n");
      out.write(sb.toString().getBytes());
    }
    out.close();
    mat = null;
  }
 
  public static void snapshot(Configuration conf, Path path, long starttime, long msInt, int count) throws Exception {
    while (System.currentTimeMillis() < starttime) {
      Thread.sleep(1);
    }

    final FileSystem fs = FileSystem.get(conf);

    long st = System.currentTimeMillis();
    int curc = 0;
    while(curc < count){
      //long sts = System.nanoTime();
      fs.createSnapshot(path);
      //long ets = System.nanoTime();
      //System.out.println("s" + curc + ":" + (double)(ets-sts)/1000000);
      curc++;

      while(System.currentTimeMillis()<(st+curc*msInt)) {
        Thread.sleep(1);
      }
    }
    System.out.println("s100 finish!");
  }

  /**
   * main method for running it as a stand alone command. 
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 3) {
      System.err.println("Usage: Client <id> <read|FFFSwrite|HDFSwrite> <data path>");
      return;
    }

    String[] indexes = otherArgs[0].split(",");
    if (otherArgs[1].equals("read")) {
      short id = Short.parseShort(indexes[0]);
      short m = Short.parseShort(indexes[1]);
      short n = Short.parseShort(indexes[2]);
      long t = Long.parseLong(indexes[3]);
      read(conf, new Path(otherArgs[2]), id, m, n, t);
    } else if (otherArgs[1].equals("snapshot")) {
      long st = Long.parseLong(indexes[0]);
      long msInt = Long.parseLong(indexes[1]);
      int count = Integer.parseInt(indexes[2]);
      snapshot(conf, new Path(otherArgs[2]), st, msInt, count);
    } else {
      short si = Short.parseShort(indexes[0]);
      short sj = Short.parseShort(indexes[1]);
      short ei = Short.parseShort(indexes[2]);
      short ej = Short.parseShort(indexes[3]);
      short mstep = Short.parseShort(indexes[4]);
      short nstep = Short.parseShort(indexes[5]);
      long t = Long.parseLong(indexes[6]);
      
      int numClient = ((ei-si)/mstep) * ((ej-sj)/nstep);
      Client[] clts = new Client[numClient];
      int c = 0;
      for (short i = si; i < ei; i += mstep)
        for (short j = sj; j < ej; j += nstep) {
          clts[c++] = new Client(conf, i, j, (short)(i+mstep), (short)(j+nstep), new Path(otherArgs[2]), otherArgs[1], t);
        }
      for (c = 0; c < numClient; c++) clts[c].start();
      for (c = 0; c < numClient; c++) clts[c].join();
      System.out.println("Client[" + si + "," + sj + "," + ei + "," + ej + "] All finish!");
    }
  }
}
