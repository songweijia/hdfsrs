/**
 * 
 */
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import java.io.IOException;
import java.io.OutputStream;
import edu.cornell.cs.sa.VectorClock;

/**
 * @author sonic
 * Vector Clock aware output stream
 */
abstract public class VCOutputStream extends OutputStream {

	/**
	 * @param mvc Message Vector Clock, Input/Output argument
	 * @param b buffer
	 * @param off offset
	 * @param len length
	 * @throws IOException
	 */
	public abstract void write(VectorClock mvc, byte[] b, int off, int len) throws IOException;
}
