/**
 * 
 */
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.zip.Checksum;

import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;

/**
 * @author weijia
 *
 */
class DFSRDMAOutputStream extends SeekableDFSOutputStream{

  
  protected DFSRDMAOutputStream(Checksum sum, int maxChunkSize, int checksumSize) {
    super(sum, maxChunkSize, checksumSize);
    // TODO Auto-generated constructor stub
  }

  static DFSRDMAOutputStream newStreamForCreate()
  throws IOException{
    //TODO
    return null;
  }
  
  static DFSRDMAOutputStream newStreamForAppend()
      throws IOException{
    //TODO
    return null;
  }

  /* (non-Javadoc)
   * @see java.io.OutputStream#write(int)
   */
  @Override
  public void write(int b) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setDropBehind(Boolean dropCache) throws IOException, UnsupportedOperationException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void sync() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void hflush() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void hsync() throws IOException {
    // TODO Auto-generated method stub
    
  }
  
  /* (non-Javadoc)
   * @see java.io.OutputStream#write(byte[])
   */
  @Override
  public void write(byte[] b) throws IOException {
    // TODO Auto-generated method stub
    super.write(b);
  }

  /* (non-Javadoc)
   * @see java.io.OutputStream#write(byte[], int, int)
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // TODO Auto-generated method stub
    super.write(b, off, len);
  }

  /* (non-Javadoc)
   * @see java.io.OutputStream#flush()
   */
  @Override
  public void flush() throws IOException {
    // TODO Auto-generated method stub
    super.flush();
  }

  /* (non-Javadoc)
   * @see java.io.OutputStream#close()
   */
  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    super.close();
  }

  @Override
  public int getCurrentBlockReplication() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void hsync(EnumSet<SyncFlag> syncFlags) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void seek(long pos) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public long getPos() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  protected void writeChunk(byte[] b, int offset, int len, byte[] checksum) throws IOException {
    // This should never be called
    throw new IOException("writeChunk is called in "+DFSRDMAOutputStream.class.getName());
  }

  @Override
  protected void checkClosed() throws IOException {
    // TODO Auto-generated method stub
    
  }
}
