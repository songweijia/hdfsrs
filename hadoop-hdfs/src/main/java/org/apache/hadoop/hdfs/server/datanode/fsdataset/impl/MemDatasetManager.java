package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_VCPID;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_MEMBLOCK_PAGESIZE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DEFAULT_DFS_VCPID;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DEFAULT_DFS_MEMBLOCK_PAGESIZE;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import  org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.VCOutputStream;

import edu.cornell.cs.blog.JNIBlog;
import edu.cornell.cs.sa.*;

public class MemDatasetManager {
  static final Log LOG = LogFactory.getLog(MemDatasetManager.class);
  
  class PoolData{
    HashMap<Long,MemBlockMeta> blockMaps; // blockId->MemBlockMeta data
    JNIBlog blog; // blog for the memory
  }
  
//  private HashMap<ExtendedBlockId, String> diskMaps;
//  private MemDatasetImpl dataset;
  private Map<String, PoolData> poolMap; // where data is stored.
  private int myVCRank; // vector clock rank
  private final long capacity;
  private final long blocksize; 
  private final int pagesize;

  
  public class MemBlockMeta extends Block implements Replica {
    boolean isDeleted;
    JNIBlog blog;
    ReplicaState state;
    
    MemBlockMeta(String bpid, long genStamp, long blockId, ReplicaState state) {
      super(blockId,(int)JNIBlog.CURRENT_SNAPSHOT_ID,0l,genStamp);
      PoolData pd = null;
      synchronized(poolMap){
        pd = poolMap.get(bpid);
        if(pd==null){
      	  pd = newPoolData();
      	  poolMap.put(bpid, pd);
        }
      }
      this.blog = pd.blog;
      this.blockId = blockId;
      this.state = state;
      this.isDeleted = false;
    }
    
    public boolean isDeleted(){
    	return isDeleted;
    }
    
    public void delete(){
    	this.isDeleted = true;
    }

    public ReplicaState getState() {
      return state;
    }
    
    public void setState(ReplicaState state){
    	this.state = state;
    }

    @Override
    public long getBytesOnDisk() {
    	return getNumBytes();
    }
    public long getBytesOnDisk(long sid){
    	return getNumBytes(sid);
    }

    @Override
    public long getVisibleLength() {
    	return getNumBytes();
    }
    public long getVisibleLength(long sid){
    	return getNumBytes(sid);
    }

    public String getStorageUuid() {
      return "0";
    }
    
    public long getBytesAcked() {
    	return getNumBytes();
    }
    public long getBytesAcked(long sid){
    	return getNumBytes(sid);
    }
  	@Override
  	public long getBlockId() {
  		return this.blockId;
  	}

  	@Override
  	public long getNumBytes() {
  		return getNumBytes(JNIBlog.CURRENT_SNAPSHOT_ID);
  	}
  	
  	public long getNumBytes(long sid){
  		return blog.getNumberOfBytes(blockId,sid);
  	}
  	
  	public BlogOutputStream getOutputStream(){
  		return getOutputStream((int)getNumBytes());
  	}
  	public BlogOutputStream getOutputStream(int offset){
  		if(offset < 0)
  			return getOutputStream();
  		else
  		  return new BlogOutputStream(blog,blockId,offset);
  	}
  	public BlogInputStream getInputStream(int offset){
  		return getInputStream(offset, JNIBlog.CURRENT_SNAPSHOT_ID);
  	}
  	protected BlogInputStream getInputStream(int offset, long snapshotId){
  		return new BlogInputStream(blog,blockId,offset,snapshotId);
  	}
  }
  
  class BlogInputStream extends InputStream {
    JNIBlog blog;
    long blockId;
    int offset;
    long snapshotId;
    
    /**
     * @param bpid
     * @param blockId
     * @param offset
     * @param snapshotId
     */
    BlogInputStream(String bpid,long blockId, int offset, long snapshotId){
    	this.blog = poolMap.get(bpid).blog;
    	this.blockId = blockId;
    	this.offset = offset;
    	this.snapshotId = snapshotId;
    }
    
    BlogInputStream(JNIBlog blog,long blockId, int offset, long snapshotId){
    	this.blog = blog;
    	this.blockId = blockId;
    	this.offset = offset;
    	this.snapshotId = snapshotId;
    }
    
    /**
     * @param blockId
     * @param offset
     */
    BlogInputStream(String bpid, int blockId, int offset){
    	this(bpid, blockId,offset,JNIBlog.CURRENT_SNAPSHOT_ID);
    }
    
    public synchronized int read() throws IOException {
   		byte [] b = new byte[1];
   		read(b,0,1);
   		return b[0];
    }
    
    /* (non-Javadoc)
     * @see java.io.InputStream#read(byte[], int, int)
     */
    public synchronized int read(byte[] bytes, int off, int len) throws IOException {
    	if(offset < blog.getNumberOfBytes(blockId, snapshotId)){
    		int ret = blog.readBlock(blockId, snapshotId, offset, off, len, bytes);
    		if(ret > 0){
    			this.offset+=ret;
    			return ret;
    		}else throw new IOException("error in JNIBlog.read("+
    			blockId+","+snapshotId+","+offset+","+off+","+len+",b):"+ret);
    	}else
    		throw new IOException("no more data available");
    }
  }
  
  class BlogOutputStream extends VCOutputStream {
    JNIBlog blog;
    long blockId;
    int offset;

    BlogOutputStream(String bpid,long blockId, int offset){
    	this.blog = poolMap.get(bpid).blog;
    	this.blockId = blockId;
    	this.offset = offset;
    }
    
    BlogOutputStream(JNIBlog blog,long blockId, int offset){
    	this.blog = blog;
    	this.blockId = blockId;
    	this.offset = offset;
    }
    
    public synchronized void write(int b) throws IOException {
      throw new IOException("Blog allows write with vector clock only.");
    }

    public synchronized void write(byte[] bytes, int off, int len) throws IOException {
        throw new IOException("Blog allows write with vector clock only.");
    }

	  @Override
    public void write(VectorClock mvc, byte[] b, int off, int len)
    throws IOException {
      int ret = blog.writeBlock(mvc, blockId, offset, off, len, b);
      if(ret < 0)
        throw new IOException("error in JNIBlog.write("+mvc+","+
          blockId+","+offset+","+off+","+len+",b):"+ret);
      else
        offset += len;
	  } 
  }
  
//  MemDatasetManager(MemDatasetImpl dataset, Configuration conf) {
//    this.dataset = dataset;
  MemDatasetManager(Configuration conf){
    this.blocksize = conf.getLongBytes(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT);
    this.pagesize = conf.getInt(DFS_MEMBLOCK_PAGESIZE, DEFAULT_DFS_MEMBLOCK_PAGESIZE);
    this.capacity = conf.getLong("dfs.memory.capacity", 1024 * 1024 * 1024 * 2l);
    this.myVCRank = conf.getInt(DFS_VCPID, DEFAULT_DFS_VCPID)<<2;
    this.poolMap = new HashMap<String, PoolData>();
//    this.diskMaps = new HashMap<ExtendedBlockId, String>();
  }
  
  void shutdown() {
    for (Map.Entry<String, PoolData> entry : this.poolMap.entrySet()){
    	entry.getValue().blog.destroy();
    }
  }
  
  long getCapacity() {
    return capacity;
  }
  
  /**
   * get the metadata
 * @param bpid bpid
 * @param blockId blockId
 * @return
 */
MemBlockMeta get(String bpid, long blockId) {
    PoolData pd = poolMap.get(bpid);
    return (pd==null)?null:pd.blockMaps.get(blockId);
  }
  
  private PoolData newPoolData(){
  	PoolData pd;
  	pd = new PoolData();
    pd.blockMaps = new HashMap<Long,MemBlockMeta>();
    pd.blog = new JNIBlog();
    pd.blog.initialize(myVCRank, (int)blocksize, pagesize);
    return pd;
  }

  /**
   * create a block
   * @param bpid
   * @param blockId
   * @param genStamp
   * @param mvc message vector clock: input/output
   * @return metadata
   */
  MemBlockMeta createBlock(String bpid, long blockId, long genStamp, VectorClock mvc) {
  	PoolData pd = null;
  	synchronized(poolMap){
  		pd = poolMap.get(bpid);
      if(pd == null){
      	pd = newPoolData();
      	poolMap.put(bpid, pd);
      }
  	}
    synchronized(pd){
      pd.blog.createBlock(mvc, blockId);
      MemBlockMeta meta = new MemBlockMeta(bpid, genStamp, blockId, ReplicaState.TEMPORARY); 
      pd.blockMaps.put(blockId, meta);
      return meta;
    }
  }
  
  /**
   * delete a block:  this is used for invalidation, but keep the
   * history.
 * @param bpid
 * @param blockId
 * @param mvc message vector clock : input/output
 */
  void deleteBlock(String bpid, long blockId, VectorClock mvc) {
    PoolData pd = poolMap.get(bpid);
    synchronized(pd){
      pd.blockMaps.get(blockId).delete();
      pd.blog.deleteBlock(mvc, blockId);
    }
  }
  
  /**
   * Just REALLY remove the block for clean up reason.
 * @param bpid
 * @param blockId
 */
void removeBlock(String bpid, long blockId){
	  PoolData pd = poolMap.get(bpid);
	  pd.blockMaps.remove(blockId);
  }

  /**
   * Only return the latest block.
 * @param bpid
 * @param state
 * @return
 */
List<Block> getBlockMetas(String bpid, ReplicaState state) {
    LinkedList<Block> results = new LinkedList<Block>();
    PoolData pd = poolMap.get(bpid);
    if(pd!=null){
      synchronized(pd){
        for(Entry<Long,MemBlockMeta> entry:pd.blockMaps.entrySet()){
          MemBlockMeta mbm = entry.getValue();
          if(!mbm.isDeleted() && (state == null || mbm.getState() == state)){
            results.add(mbm);
          }
        } 
      }
    }
    return results;
  }

  /**
   * create a blog snapshot Id. 
 * @param bpid
 * @param snapshotId - rtc
 * @param eid
 * @throws IOException
 */
  void snapshot(String bpid, long snapshotId, long eid)throws IOException{
	  poolMap.get(bpid).blog.createSnapshot(snapshotId, eid);
  }
  
  VectorClock since(String bpid, int nnrank, long nneid, long rtc)
  throws IOException{
  	VectorClock vc = new VectorClock();
  	if(poolMap.get(bpid).blog.since(rtc, nnrank, nneid, vc)!=0)
  		throw new IOException("call since failed with rtc="+rtc+
  				",nnrank="+nnrank+
  				",nneid="+nneid);
  	
  	return vc;
  }
/*  
  long since(String bpid, long rtc)throws IOException{
  	VectorClock vc = new VectorClock();
  	if(poolMap.get(bpid).blog.since(rtc, vc)!=0)
  		throw new IOException("call since failed with rtc="+rtc);
  	return vc.vc.get(poolMap.get(bpid).blog.getMyRank());
  }
*/
}
