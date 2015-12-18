package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.EnumSet;
import java.util.zip.Checksum;

import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;

public abstract class SeekableDFSOutputStream extends FSOutputSummer 
implements Syncable, CanSetDropBehind{
  protected SeekableDFSOutputStream(Checksum sum, int maxChunkSize, int checksumSize) {
    super(sum, maxChunkSize, checksumSize);
    // TODO Auto-generated constructor stub
  }
  public abstract int getCurrentBlockReplication() throws IOException;
  public abstract void hsync(EnumSet<SyncFlag> syncFlags)throws IOException;
  public abstract void seek(long pos)throws IOException;
  public abstract long getPos() throws IOException;
}
