package edu.cornell.cs.blog;

public class ts64RecordParser implements IRecordParser{
  long ts;
  @Override
  public long getUserTimestamp(){
    return ts;
  }
  
  @Override
  public int ParseRecord(byte[] buf, int offset, int len)throws RecordParserException{
    if(len != 8)
      throw new RecordParserException("ts64 record should be exactly 8 bytes instead of " + len + " bytes.");
    ts = 0l;
    for(int i=0;i<len;i++){
      ts += (long)buf[i];
      if(i+1<len)ts=ts<<8;
    }
    return len;
  }
}
