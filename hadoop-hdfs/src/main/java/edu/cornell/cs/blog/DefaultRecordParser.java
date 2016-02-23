/**
 * 
 */
package edu.cornell.cs.blog;

/**
 * @author weijia
 * The default record parser just return what we have 
 * in the buffer with an invalid timestamp.
 */
public class DefaultRecordParser implements IRecordParser {

  /* (non-Javadoc)
   * @see edu.cornell.cs.blog.IRecordParser#getUserTimestamp(byte[])
   */
  @Override
  public long getUserTimestamp() {
    return -1L;
  }

  /* (non-Javadoc)
   * @see edu.cornell.cs.blog.IRecordParser#ParseRecord(byte[], int, int)
   */
  @Override
  public int ParseRecord(byte[] buf, int offset, int len) throws RecordParserException {
    if(offset >= 0 && offset+len<buf.length) 
      return (len==0)?-1:offset+len;
    else
      throw new RecordParserException("Cannot ParseRecord");
  }
}
