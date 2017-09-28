package edu.cornell.cs.blog;

import java.nio.ByteBuffer;

public class PMURecordParser implements IRecordParser {
  private long ts;

  @Override
  public long getUserTimestamp() {
    return ts;
  }

  @Override
  public int ParseRecord(ByteBuffer bb) throws RecordParserException {
    byte[] data = new byte[14];
    int rem_size = bb.remaining();
    int rec_size;
    long s, ms;

    if (rem_size == 0)
      return -1;
    assert rem_size >= 16;

    // Get header.
    bb.get(data, 0, 14);
    rec_size = ((int) data[2])*256 + ((int) data[3]);
    s = ((long) data[6])*256*256*256 + ((long) data[7])*256*256 + ((long) data[8])*256 + ((long) data[9]);
    ms = ((long) data[11])*256*256 + ((long) data[12])*256 + ((long) data[13]);
    assert rem_size >= rec_size;
    // Assume that packet holds one or more full records. Records are not partially written.

    // Calculate timestamp and return size.
    ts = 1000*s + ms;
    return rec_size;
  }
 
  @Override
  public int ParseRecord(byte[] buf, int offset, int len) throws RecordParserException {
    return ParseRecord(ByteBuffer.wrap(buf,offset,len));
  }
}
