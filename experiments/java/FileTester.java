import java.io.PrintStream;
import java.io.IOException;
import java.io.FileOutputStream;
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
//import edu.cornell.cs.blog.JNIBlog;
import java.nio.ByteBuffer;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;

public class FileTester extends Configured implements Tool {
  public static void main(String[] args){
    int res;
    try{
      res = ToolRunner.run(new Configuration(), new FileTester(), args);
      System.exit(res);
    }catch(Exception e){
      System.out.println(e);
      e.printStackTrace();
    }
  }
  
  void overwriteFile(FileSystem fs, String path, String... args)
  throws Exception{
    if(args.length != 2){
      throw new IOException("Please specify the pos and data");
    }
    FSDataOutputStream fsos = fs.append(new Path(path));
    fsos.seek(Long.parseLong(args[0]));
    System.out.println("seek done");
    PrintStream ps = new PrintStream(fsos);
    ps.print(args[1]);
    System.out.println("write done");
    ps.close();
    fsos.close();
    System.out.println("close done");
  }

  void appendFile(FileSystem fs, String path, String... args)
  throws IOException{
    if(args.length != 1){
      throw new IOException("Please specify the data");
    }
    FSDataOutputStream fsos = fs.append(new Path(path));
    PrintStream ps = new PrintStream(fsos);
    ps.print(args[0]);
    ps.close();
    fsos.close();
  }

  void timeAppend(FileSystem fs, String path, String... args)
  throws IOException{

    if(args.length != 2)
      throw new IOException("Please specify the write length and duration in second");

    int wsize = Integer.parseInt(args[0]);
    long dur = Long.parseLong(args[1]);
    long nCnt = 0;
    byte buf[] = new byte[wsize];

    for(int i=0;i<wsize;i++)buf[i]=(byte)'X';

    FSDataOutputStream fsos = fs.append(new Path(path));
    long t_start = System.nanoTime();
    long t_end = t_start;
    while(t_end - t_start < dur*1000000000L){
      fsos.write(buf);
      t_end = System.nanoTime();
      nCnt ++;
    }
    fsos.flush();
    long nanoDur = System.nanoTime() - t_start;

    System.out.println(String.format("%1$.3f",((double)nCnt*wsize/(t_end - t_start)))+" GB/s");

    fsos.close();
  }

  void write1g(FileSystem fs, String path, int bfsz)
  throws IOException{
    FSDataOutputStream fsos = fs.create(new Path(path));
    byte [] buf = new byte[bfsz];
    int i;
    for(i=0;i<bfsz;i++)buf[i]=(byte)'a';
    for(i=0;i<((1<<30)/bfsz);i++)
      fsos.write(buf,0,bfsz);
    fsos.close();
  }

  // Note: please set the block size to 1MB
  void randomWrite(FileSystem fs, String path)
  throws IOException{
/*    Path p = new Path(path);
    byte [] buf = new byte[4096];
    int i;
    // 0) Initialize write 4KB for 1024 times.
    for(i=0;i<4096;i++)buf[i]='I';
    FSDataOutputStream fsos = fs.create(new Path(path));
    for(i=0;i<1024;i++)fsos.write(buf,0,4096);
//    fsos.close();
    // 1) write 4K at 0; 4K at 1044480; 4K at 100000
    for(i=0;i<4096;i++)buf[i]='1';
    fsos.seek(0);fsos.write(buf,0,4096);
    fsos.seek(1044480);fsos.write(buf,0,4096);
    fsos.seek(100000);fsos.write(buf,0,4096);
//    fsos.close();
    // 2) write cross blocks
    // from 1048000 -> 1049000
    for(i=0;i<4096;i++)buf[i]='2';
    fsos.seek(1048000);fsos.write(buf,0,1000);
    // from 2097000 to 3146000
    fsos.seek(2097000);
    for(int j=0;j<1049;j++)fsos.write(buf,0,1000);
    fsos.close();
*/  }

  void snapshot(FileSystem fs,String path, long msInt, int count)
  throws IOException{
    long st = System.currentTimeMillis();//JNIBlog.readLocalRTC();
    int curc = 0;
    while(curc < count){
      long sts = System.nanoTime();
      fs.createSnapshot(new Path(path),""+curc);
      long ets = System.nanoTime();
      System.out.println((double)(ets-sts)/1000000);
      curc++;
      while(/*JNIBlog.readLocalRTC()*/System.currentTimeMillis()<(st+curc*msInt)){
        try{Thread.sleep(1);}catch(InterruptedException ie){}
      }
    }
  }

  void zeroCopyRead(FileSystem fs,String path,
    int readSize,int nloop) throws IOException{

    long start_ts,end_ts,len=0;

    ByteBuffer bb = ByteBuffer.allocate(readSize);
    HdfsDataInputStream fsis = (HdfsDataInputStream)fs.open(new Path(path));
    for(int i=0;i<nloop;i++){
      fsis.seek(0);
      len=0;
      long ts1,ts2;
      start_ts = System.nanoTime();
      while(true){
	bb = fsis.rdmaRead(readSize);
        if(bb==null)break;
        len += bb.remaining();
      };
      end_ts = System.nanoTime();
      System.out.println(((double)len/(end_ts - start_ts))+" GB/s");
    }
    fsis.close();
  }

  void standardRead(FileSystem fs,String path,
    int readSize,int nloop)
  throws IOException{
    long start_ts,end_ts,len=0;
    int nRead;
    ByteBuffer bb = ByteBuffer.allocate(readSize);
    FSDataInputStream fsis = fs.open(new Path(path));
    for(int i=0;i<nloop;i++){
      fsis.seek(0);
      len=0;
      long ts1,ts2;
      start_ts = System.nanoTime();
      while(true){
        nRead=fsis.read(bb);
        if(nRead <= 0)break;
        len+=nRead;
        bb.clear();
      };
      end_ts = System.nanoTime();
      System.out.println(String.format("%1$.3f",((double)len/(end_ts - start_ts)))+" GB/s");
    }
    fsis.close();
  }

  void addTimestamp(long ts,byte[] buf){
    StringBuffer sb = new StringBuffer();
    sb.append(ts);
    sb.append(" ");
    byte bs[] = sb.toString().getBytes();
    System.arraycopy(bs,0,buf,0,bs.length);
  }

  void timewrite(FileSystem fs,String path,int bufsz, int dur)
  throws IOException{
    byte buf[] = new byte[bufsz];
    for(int i=0;i<bufsz;i++)buf[i]='P';
    buf[bufsz-1]='\n';
    FSDataOutputStream fsos = fs.create(new Path(path));
    long end = System.currentTimeMillis()+dur*1000;
    long cur = System.currentTimeMillis()+33;//JNIBlog.readLocalRTC()+33;
    while(System.currentTimeMillis() < end){
      //write a packet
      if(/*JNIBlog.readLocalRTC()*/ System.currentTimeMillis() >= cur){
        addTimestamp(cur,buf);
        fsos.write(buf,0,bufsz);
        fsos.hflush();
        cur += 33;
      }
    }
    fsos.close();
  }

  void pmuwrite(FileSystem fs,String path,
    int pmuid, int recordsize, int dur)
  throws IOException{
    byte buf[] = new byte[recordsize];
    for(int i=0;i<recordsize;i++)buf[i]='P';
    buf[recordsize-1]='\n';
    FSDataOutputStream fsos = fs.create(new Path(path+"/pmu"+pmuid));
    long end = System.currentTimeMillis()+dur*1000;
    long cur = System.currentTimeMillis()+33;//JNIBlog.readLocalRTC()+33;
    while(System.currentTimeMillis() < end){
      //write a packet
      if(/*JNIBlog.readLocalRTC()*/System.currentTimeMillis() >= cur){
        addTimestamp(cur,buf);
        fsos.write(buf,0,recordsize);
        fsos.hflush();
        cur += 33;
      }
    }
    fsos.close();
  }

  private List<Long> getSnapshots(FileSystem fs)
  throws IOException{
    List<Long> lRet = new Vector<Long>(256);
    for(FileStatus stat: fs.listStatus(new Path("/.snapshot"))){
      Long snapts = Long.parseLong(stat.getPath().getName());
      lRet.add(snapts);
    }
    return lRet;
  }

  class FileTuple{
    String name; // file name
    long sts; // start time stamp
    long ets; // end time stamp
  }

  private long readFirstTs(FileSystem fs,Path path)
  throws IOException{
    FSDataInputStream fsis = fs.open(path);
    long lRet = -1;
    if(fsis.available()>13){
      byte buf[] = new byte[13];
      fsis.readFully(0,buf);
      lRet = Long.parseLong(new String(buf));
    }
    fsis.close();
    return lRet;
  }

  private long readLastTs(FileSystem fs,Path path)
  throws IOException{
    long lRet = -1;
    if(!fs.exists(path)) return -1;
    FSDataInputStream fsis = null;
    try{
      fsis = fs.open(path);
      int back = 64;
      int flen = fsis.available();
      if(flen > 0)
        while(true){
          int try_pos = Math.max(flen - back, 0);
          byte buf[] = new byte[flen - try_pos];
          fsis.readFully(try_pos,buf);
          if(buf[buf.length-1]=='\n')
            buf[buf.length-1]='P';
          String line = new String(buf);
          int pts = line.lastIndexOf('\n');
          if(pts == -1 && try_pos != 0)continue;
          if(pts != -1 && pts+14 <= line.length())
            lRet = Long.parseLong(line.substring(pts+1,pts+14));
           break;
        }
    }catch(IOException ioe){
      return -1;
    }finally{
      if(fsis!=null)fsis.close();
    }
    return lRet;
  }

  private List<FileTuple> getFiles(FileSystem fs, String path)
  throws IOException{
    List<FileTuple> lRet = new Vector<FileTuple>(32);
    for(FileStatus stat: fs.listStatus(new Path(path))){
      FileTuple ft = new FileTuple();
      ft.name = stat.getPath().getName();
      ft.sts = readFirstTs(fs,stat.getPath());
      ft.ets = readLastTs(fs,stat.getPath());
      lRet.add(ft);
    }
    return lRet;
  }

  void analyzesnap(FileSystem fs, String path)
  throws IOException{
    // STEP 1: get snapshot/timestamp list
    List<Long> lSnap = getSnapshots(fs);
    // STEP 2: get the real start/end timestamp for each file.
    List<FileTuple> lFile = getFiles(fs,path);
    // STEP 3: spit data
    for(long snap: lSnap){
      for(FileTuple ft: lFile){
        if(ft.sts > snap)continue;
        Path p = new Path("/.snapshot/"+snap+path+"/"+ft.name);
        long delta = snap - ft.sts;
        long ets = readLastTs(fs,p);

        if(ets!=-1){
          delta = snap - ets;
          if(snap > ft.ets){
            delta = ft.ets - ets;
            if(delta <= 0) continue;
          }
        }

        System.out.println(ft.name+" "+snap+" "+delta);
      }
    }
  }

  void readto(FileSystem fs, String srcpath, String destpath, long timestamp, boolean bUserTimestamp)
  throws IOException{
    System.out.println("user timestamp="+bUserTimestamp);
    //STEP 1 open source file.
    FSDataInputStream fin = fs.open(new Path(srcpath),4096,timestamp,bUserTimestamp);
    //STEP 2 open destination file.
    FileOutputStream fos = new FileOutputStream(destpath);
    //STEP 3 copy.
    byte buf[] = new byte[4096];
    do{
      int nRead;
      nRead = fin.read(buf);
      if(nRead < 0)break;
      fos.write(buf,0,nRead);
    }while(true);
    //STEP 4 done.
    fos.close();
    fin.close();
  }

  void write_ts64(FileSystem fs, String path, int ncount) throws Exception{
    //STEP 1 create file.
    FSDataOutputStream os = fs.create(new Path(path+".ts64"));
    //STEP 2 write numbers.
    byte buf[] = new byte[8];
    for(int i=0;i<ncount;i++){
      buf[7]++;
      os.write(buf,0,8);
      os.hflush();
    }
    os.close();
  }

  @Override
  public int run(String[] args) throws Exception{
    Configuration conf = this.getConf();
    FileSystem fs = FileSystem.get(conf);

    if(args.length < 1){
      System.out.println("args: <cmd:=append|overwrite>\n"+
        "\tappend <file> <data>\n"+
        "\ttimeappend <file> <ws> <dur>\n"+
        "\toverwrite <file> <pos> <data>\n"+
        "\twrite1g <file> <bfsz>\n"+ //set buffersize
        "\trandomwrite <file>\n"+ 
        "\tsnapshot <path> <interval_ms> <number>\n"+
        "\ttimewrite <path> <bfsz> <duration>\n"+
        "\tpmuwrite <path> <pmuid> <recordsize> <duration>\n"+
        "\tzeroCopyRead <path> <readSize> <nloop>\n"+
        "\tstandardRead <path> <readSize> <nloop>\n"+
        "\tanalyzesnap <path>\n"+
        "\treadto <srcpath> <destpath> <timestamp> <bUserTimestamp>\n"+
        "\twrite.ts64 <filepath> <ncount>\n" +
        "\tr113");
      return -1;
    }
    if("append".equals(args[0]))
      this.appendFile(fs,args[1],args[2]);
    else if("timeappend".equals(args[0]))
      this.timeAppend(fs,args[1],args[2],args[3]);
    else if("overwrite".equals(args[0]))
      this.overwriteFile(fs,args[1],args[2],args[3]);
    else if("write1g".equals(args[0]))
      this.write1g(fs,args[1],Integer.parseInt(args[2]));
    else if("randomwrite".equals(args[0]))
      this.randomWrite(fs,args[1]);
    else if("snapshot".equals(args[0]))
      this.snapshot(fs,args[1],Long.parseLong(args[2]),Integer.parseInt(args[3]));
    else if("standardRead".equals(args[0]))
      this.standardRead(fs,args[1],Integer.parseInt(args[2]),Integer.parseInt(args[3]));
    else if("zeroCopyRead".equals(args[0]))
      this.zeroCopyRead(fs,args[1],Integer.parseInt(args[2]),Integer.parseInt(args[3]));
    else if("timewrite".equals(args[0]))
      this.timewrite(fs,args[1],Integer.parseInt(args[2]),Integer.parseInt(args[3]));
    else if("pmuwrite".equals(args[0]))
      this.pmuwrite(fs,args[1],Integer.parseInt(args[2]),Integer.parseInt(args[3]),Integer.parseInt(args[4]));
    else if("analyzesnap".equals(args[0]))
      this.analyzesnap(fs,args[1]);
    else if("readto".equals(args[0]))
      this.readto(fs,args[1],args[2],Long.parseLong(args[3]),Boolean.parseBoolean(args[4]));
    else if("write.ts64".equals(args[0]))
      this.write_ts64(fs,args[1],Integer.parseInt(args[2]));
    else
      throw new Exception("invalid command:"+args[0]);
    return 0;
  }
}