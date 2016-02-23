/**
 * 
 */
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import java.io.IOException;

import java.io.OutputStream;
import edu.cornell.cs.sa.HybridLogicalClock;;

/**
 * @author sonic
 * Vector Clock aware output stream
 */
abstract public class HLCOutputStream extends OutputStream {

	/**
	 * @param mhlc HybridLogicalClock, Input/Output argument
	 * @param b buffer
	 * @param off offset
	 * @param len length
	 * @throws IOException
	 */
	final public void write(HybridLogicalClock mhlc, byte[] b, int off, int len)
	    throws IOException{
	  this.write(mhlc,mhlc.r,b,off,len);
	}
	
	/**
	 * @param mhlc
	 * @param userTimestamp
	 * @param b
	 * @param off
	 * @param len
	 * @throws IOException
	 */
	public abstract void write(HybridLogicalClock mhlc, long userTimestamp, byte[] b, int off, int len) throws IOException;
}
