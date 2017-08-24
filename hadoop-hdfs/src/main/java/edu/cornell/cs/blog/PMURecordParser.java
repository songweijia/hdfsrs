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
    byte[] seconds = new byte[4];
    byte[] fracsec = new byte[3];

    bb.get(seconds, 6, 4);
    bb.get(fracsec, 12, 3);
    ts = 1000*(((long) seconds[0])*256 + ((long) seconds[1]));
    ts += fracsec[0]*256*256 + fracsec[1]*256 + fracsec[2];
    return -1;
  }
  
  @Override
  public int ParseRecord(byte[] buf, int offset, int len) throws RecordParserException {
    return ParseRecord(ByteBuffer.wrap(buf,offset,len));
  }
}
