/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemDatasetManager;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;

/** A class that receives a block and writes to its own disk, meanwhile
 * may copies it to another site. If a throttler is provided,
 * streaming throttling is also supported.
 **/
class MemBlockReceiver extends BlockReceiver {
  /** the replica to write */
  private final MemDatasetManager.MemBlockMeta replicaInfo;

  MemBlockReceiver(final ExtendedBlock block, final DataInputStream in,
      final String inAddr, final String myAddr,
      final BlockConstructionStage stage, 
      final long newGs, final long minBytesRcvd, final long maxBytesRcvd, 
      final String clientname, final DatanodeInfo srcDataNode,
      final DataNode datanode, DataChecksum requestedChecksum,
      long offset/*HDFSRS_RWAPI*/) throws IOException {
    
    super(block, in, inAddr, myAddr, stage,clientname, srcDataNode, datanode, requestedChecksum);
  
    //
    // Open local disk out
    //
    if (isDatanode) { //replication or move
      replicaInfo = (MemDatasetManager.MemBlockMeta)datanode.data.createTemporary(block);
    } else {
      switch (stage) {
      case PIPELINE_SETUP_CREATE:
        replicaInfo = (MemDatasetManager.MemBlockMeta)datanode.data.createRbw(block);
        datanode.notifyNamenodeReceivingBlock(
            block, replicaInfo.getStorageUuid());
        break;
      case PIPELINE_SETUP_STREAMING_RECOVERY:
        replicaInfo = (MemDatasetManager.MemBlockMeta)datanode.data.recoverRbw(
            block, newGs, minBytesRcvd, maxBytesRcvd);
        block.setGenerationStamp(newGs);
        break;
      case PIPELINE_SETUP_APPEND:
      case PIPELINE_SETUP_OVERWRITE:
        replicaInfo = (MemDatasetManager.MemBlockMeta)datanode.data.append(block, newGs, minBytesRcvd);
        if (datanode.blockScanner != null) { // remove from block scanner
          datanode.blockScanner.deleteBlock(block.getBlockPoolId(),
              block.getLocalBlock());
        }
        block.setGenerationStamp(newGs);
        datanode.notifyNamenodeReceivingBlock(
            block, replicaInfo.getStorageUuid());
        break;
      case PIPELINE_SETUP_APPEND_RECOVERY:
        replicaInfo = (MemDatasetManager.MemBlockMeta)datanode.data.recoverAppend(block, newGs, minBytesRcvd);
        if (datanode.blockScanner != null) { // remove from block scanner
          datanode.blockScanner.deleteBlock(block.getBlockPoolId(),
              block.getLocalBlock());
        }
        block.setGenerationStamp(newGs);
        datanode.notifyNamenodeReceivingBlock(
            block, replicaInfo.getStorageUuid());
        break;
      case TRANSFER_RBW:
      case TRANSFER_FINALIZED:
        // this is a transfer destination
        replicaInfo = (MemDatasetManager.MemBlockMeta)datanode.data.createTemporary(block);
        break;
      default: throw new IOException("Unsupported stage " + stage + 
            " while receiving block " + block + " from " + inAddr);
      }
    }
    
    try {
      this.out = datanode.data.getBlockOutputStream(block, offset);
    } catch (ReplicaAlreadyExistsException bae) {
      throw bae;
    } catch (ReplicaNotFoundException bne) {
      throw bne;
    } catch(IOException ioe) {
      IOUtils.closeStream(this);
      cleanupBlock();
      throw ioe;
    }
  }

  /**
   * close files.
   */
  @Override
  public void close() throws IOException {
    if (packetReceiver != null) {
      packetReceiver.close();
    }
    
    if (syncOnClose && out != null) {
      datanode.metrics.incrFsyncCount();      
    }
    long flushTotalNanos = 0;
    boolean measuredFlushTime = false;

    // close block file
    IOException ioe = null;
    try {
      if (out != null) {
        long flushStartNanos = System.nanoTime();
        out.flush();
        long flushEndNanos = System.nanoTime();
        flushTotalNanos += flushEndNanos - flushStartNanos;
        measuredFlushTime = true;
        out.close();
        out = null;
      }
    } catch (IOException e) {
      ioe = e;
    }
    finally{
      IOUtils.closeStream(out);
    }
    if (measuredFlushTime) {
      datanode.metrics.addFlushNanos(flushTotalNanos);
    }
    
    if(ioe != null) {
      throw ioe;
    }
  }

  /**
   * Flush block data and metadata files to disk.
   * @throws IOException
   */
  void flushOrSync(boolean isSync) throws IOException {
    long flushTotalNanos = 0;
    if (out != null) {
      long flushStartNanos = System.nanoTime();
      out.flush();
      long flushEndNanos = System.nanoTime();
      flushTotalNanos += flushEndNanos - flushStartNanos;
      
      datanode.metrics.addFlushNanos(flushTotalNanos);
      if (isSync) {
    	  datanode.metrics.incrFsyncCount();      
      }
    }
  }

  /** 
   * Receives and processes a packet. It can contain many chunks.
   * returns the number of data bytes that the packet has.
   */
  private int receivePacket() throws IOException {
    // read the next packet
    packetReceiver.receiveNextPacket(in);

    PacketHeader header = packetReceiver.getHeader();
    if (LOG.isDebugEnabled()){
      LOG.debug("Receiving one packet for block " + block +
                ": " + header);
    }

    // Sanity check the header
    if (header.getOffsetInBlock() > replicaInfo.getNumBytes()) {
      throw new IOException("Received an out-of-sequence packet for " + block + 
          "from " + inAddr + " at offset " + header.getOffsetInBlock() +
          ". Expecting packet starting at " + replicaInfo.getNumBytes());
    }
    if (header.getDataLen() < 0) {
      throw new IOException("Got wrong length during writeBlock(" + block + 
                            ") from " + inAddr + " at offset " + 
                            header.getOffsetInBlock() + ": " +
                            header.getDataLen()); 
    }

    long offsetInBlock = header.getOffsetInBlock();
    long seqno = header.getSeqno();
    boolean lastPacketInBlock = header.isLastPacketInBlock();
    int len = header.getDataLen();
    boolean syncBlock = header.getSyncBlock();

    // avoid double sync'ing on close
    if (syncBlock && lastPacketInBlock) {
      this.syncOnClose = false;
    }

    // update received bytes
    long firstByteInBlock = offsetInBlock;
    offsetInBlock += len;
    if (replicaInfo.getNumBytes() < offsetInBlock) {
      replicaInfo.setNumBytes(offsetInBlock);
    }
    
    // put in queue for pending acks, unless sync was requested
    if (responder != null && !syncBlock && !shouldVerifyChecksum()) {
      ((PacketResponder) responder.getRunnable()).enqueue(seqno,
          lastPacketInBlock, offsetInBlock, Status.SUCCESS);
    }
    
    ByteBuffer dataBuf = packetReceiver.getDataSlice();
    ByteBuffer checksumBuf = packetReceiver.getChecksumSlice();
    
    if (lastPacketInBlock || len == 0) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Receiving an empty packet or the end of the block " + block);
      }
      // sync block if requested
      if (syncBlock) {
        flushOrSync(true);
      }
    } else {
      int checksumLen = ((len + bytesPerChecksum - 1)/bytesPerChecksum)*
                                                            checksumSize;

      if ( checksumBuf.capacity() != checksumLen) {
        throw new IOException("Length of checksums in packet " +
            checksumBuf.capacity() + " does not match calculated checksum " +
            "length " + checksumLen);
      }

      if (shouldVerifyChecksum()) {
        try {
          verifyChunks(dataBuf, checksumBuf);
        } catch (IOException ioe) {
          // checksum error detected locally. there is no reason to continue.
          if (responder != null) {
            try {
              ((PacketResponder) responder.getRunnable()).enqueue(seqno,
                  lastPacketInBlock, offsetInBlock,
                  Status.ERROR_CHECKSUM);
              // Wait until the responder sends back the response
              // and interrupt this thread.
              Thread.sleep(3000);
            } catch (InterruptedException e) { }
          }
          throw new IOException("Terminating due to a checksum error." + ioe);
        }
      }
      
      // by this point, the data in the buffer uses the disk checksum
      
      try {
        long onDiskLen = replicaInfo.getBytesOnDisk();
        //HDFSRS_RWAPI{
        //if (onDiskLen<offsetInBlock) {
        if (onDiskLen==firstByteInBlock) {
          LOG.debug("[HDFSRS_RWAPI]BlockReceiver.receivePacket():appending packet received.");
        //}
          //finally write to the disk :
          
          if (onDiskLen % bytesPerChecksum != 0) { 
            // prepare to overwrite last checksum
            out.flush();
          }

          int startByteToDisk = (int)(onDiskLen-firstByteInBlock) 
              + dataBuf.arrayOffset() + dataBuf.position();

          int numBytesToDisk = (int)(offsetInBlock-onDiskLen);
          
          // Write data to disk.
          out.write(dataBuf.array(), startByteToDisk, numBytesToDisk);

          /// flush entire packet, sync if requested
          flushOrSync(syncBlock);
          
          replicaInfo.setNumBytes(offsetInBlock);

          datanode.metrics.incrBytesWritten(len);

        }//HDFSRS_RWAPI
        else{// for overwriting
          LOG.debug("[HDFSRS_RWAPI]BlockReceiver.receivePacket():overwriting packet received:packet(" +
              firstByteInBlock+","+offsetInBlock+")/replica("+onDiskLen+")");
          
          //dx.2 write data to disk.
          int startByteToDisk = dataBuf.arrayOffset() + dataBuf.position();
          out.write(dataBuf.array(), startByteToDisk, len);//packet len
          
          /// dx.7 flush entire packet, sync if requested
          flushOrSync(syncBlock);
          
          // dx.8 update replicaInfo as long as the last chunkChecksum changed.
          if( (onDiskLen - offsetInBlock) < bytesPerChecksum )
            replicaInfo.setNumBytes(Math.max(offsetInBlock,onDiskLen));

          datanode.metrics.incrBytesWritten(len);     
        }
        //}
      } catch (IOException iex) {
        throw iex;
      }
    }

    // if sync was requested, put in queue for pending acks here
    // (after the fsync finished)
    if (responder != null && (syncBlock || shouldVerifyChecksum())) {
      ((PacketResponder) responder.getRunnable()).enqueue(seqno,
          lastPacketInBlock, offsetInBlock, Status.SUCCESS);
    }

    if (throttler != null) { // throttle I/O
      throttler.throttle(len);
    }
    
    return lastPacketInBlock?-1:len;
  }

  void receiveBlock(
      DataOutputStream mirrOut, // output to next datanode
      DataInputStream mirrIn,   // input from next datanode
      DataOutputStream replyOut,  // output to previous datanode
      String mirrAddr, DataTransferThrottler throttlerArg,
      DatanodeInfo[] downstreams) throws IOException {

      syncOnClose = datanode.getDnConf().syncOnClose;
      boolean responderClosed = false;
      throttler = throttlerArg;

    try {
      if (isClient && !isTransfer) {
        responder = new Daemon(datanode.threadGroup, 
            new PacketResponder(replyOut, mirrIn, downstreams));
        responder.start(); // start thread to processes responses
      }

      while (receivePacket() >= 0) { /* Receive until the last packet */ }

      // wait for all outstanding packet responses. And then
      // indicate responder to gracefully shutdown.
      // Mark that responder has been closed for future processing
      if (responder != null) {
        ((PacketResponder)responder.getRunnable()).close();
        responderClosed = true;
      }

      // If this write is for a replication or transfer-RBW/Finalized,
      // then finalize block or convert temporary to RBW.
      // For client-writes, the block is finalized in the PacketResponder.
      if (isDatanode || isTransfer) {
        // close the block/crc files
        close();
        block.setNumBytes(replicaInfo.getNumBytes());

        if (stage == BlockConstructionStage.TRANSFER_RBW) {
          // for TRANSFER_RBW, convert temporary to RBW
          datanode.data.convertTemporaryToRbw(block);
        } else {
          // for isDatnode or TRANSFER_FINALIZED
          // Finalize the block.
          datanode.data.finalizeBlock(block);
        }
        datanode.metrics.incrBlocksWritten();
      }

    } catch (IOException ioe) {
      if (datanode.isRestarting()) {
        // Do not throw if shutting down for restart. Otherwise, it will cause
        // premature termination of responder.
        LOG.info("Shutting down for restart (" + block + ").");
      } else {
        LOG.info("Exception for " + block, ioe);
        throw ioe;
      }
    } finally {
      // Clear the previous interrupt state of this thread.
      Thread.interrupted();

      // If a shutdown for restart was initiated, upstream needs to be notified.
      // There is no need to do anything special if the responder was closed
      // normally.
      if (!responderClosed) { // Data transfer was not complete.
        if (responder != null) {
          // In case this datanode is shutting down for quick restart,
          // send a special ack upstream.
          if (datanode.isRestarting() && isClient && !isTransfer) {
            try {
              ((PacketResponder) responder.getRunnable()).
                  sendOOBResponse(PipelineAck.getRestartOOBStatus());
              // Even if the connection is closed after the ack packet is
              // flushed, the client can react to the connection closure 
              // first. Insert a delay to lower the chance of client 
              // missing the OOB ack.
              Thread.sleep(1000);
            } catch (InterruptedException ie) {
              // It is already going down. Ignore this.
            } catch (IOException ioe) {
              LOG.info("Error sending OOB Ack.", ioe);
            }
          }
          responder.interrupt();
        }
        IOUtils.closeStream(this);
        cleanupBlock();
      }
      if (responder != null) {
        try {
          responder.interrupt();
          // join() on the responder should timeout a bit earlier than the
          // configured deadline. Otherwise, the join() on this thread will
          // likely timeout as well.
          long joinTimeout = datanode.getDnConf().getXceiverStopTimeout();
          joinTimeout = joinTimeout > 1  ? joinTimeout*8/10 : joinTimeout;
          responder.join(joinTimeout);
          if (responder.isAlive()) {
            String msg = "Join on responder thread " + responder
                + " timed out";
            LOG.warn(msg + "\n" + StringUtils.getStackTrace(responder));
            throw new IOException(msg);
          }
        } catch (InterruptedException e) {
          responder.interrupt();
          // do not throw if shutting down for restart.
          if (!datanode.isRestarting()) {
            throw new IOException("Interrupted receiveBlock");
          }
        }
        responder = null;
      }
    }
  }
}
