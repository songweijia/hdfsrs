import java.io.PrintStream;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Exp1 extends Configured implements Tool {
  public static void main(String[] args){
    int res;
    try{
      res = ToolRunner.run(new Configuration(), new Exp1(), args);
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
    Path p = new Path(path);
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
  }

  void multitest(FileSystem fs, String path, int num)
  throws IOException{
	Path p = new Path(path);
	int flen = (int)fs.getFileStatus(new Path(path)).getLen();
        Random rand = new Random(System.currentTimeMillis());
	String [] buf = new String[num];
	int [] pos = new int[num];
	System.out.print("Preparing randome write info:"
			+ "\nfile = " + path
			+ "\nwriteSize = 256"
			+ "\ncount = " + num
			+ "\n...");
	for(int i=0;i<num;i++){
		byte tbuf[] = new byte[256];
		pos[i] = rand.nextInt(flen-256);
		rand.nextBytes(tbuf);
    	for(int j=0;j<256;j++)
    		tbuf[j] = (byte)(tbuf[j]%5+'5');
    	buf[i] = new String(tbuf);
	}
	System.out.print("...done"
			+ "\nPerforming test...");

	long tsStart = System.nanoTime();
    FSDataOutputStream fsos = fs.append(new Path(path));
    for(int i=0;i<num;i++){
    	fsos.seek(pos[i]);
    	if(buf[i].length()!=256)
    		System.out.println(buf[i].length());
    	fsos.writeBytes(buf[i]);
    }
    fsos.close();
	long tsEnd = System.nanoTime();

	System.out.println("...done");
	System.out.println("Total Time = " 
			+ (tsEnd-tsStart)/1000 + "." + (tsEnd-tsStart)%1000
			+ " microseconds");
  }

  @Override
  public int run(String[] args) throws Exception{
    Configuration conf = this.getConf();
    FileSystem fs = FileSystem.get(conf);

    if(args.length < 1){
      System.out.println("args: <cmd:=append|overwrite>\n"+
        "\tappend <file> <data>\n"+
        "\toverwrite <file> <pos> <data>\n"+
        "\tmultitest <file> <num>\n"+
        "\twrite1g <file> <bfsz>\n"+ //set buffersize
        "\trandomwrite <file>\n");
      return -1;
    }
    if("append".equals(args[0]))
      this.appendFile(fs,args[1],args[2]);
    else if("overwrite".equals(args[0]))
      this.overwriteFile(fs,args[1],args[2],args[3]);
    else if("multitest".equals(args[0]))
      this.multitest(fs,args[1],Integer.parseInt(args[2]));
    else if("write1g".equals(args[0]))
      this.write1g(fs,args[1],Integer.parseInt(args[2]));
    else if("randomwrite".equals(args[0]))
      this.randomWrite(fs,args[1]);
    else
      throw new Exception("invalid command:"+args[0]);
    return 0;
  }
}
