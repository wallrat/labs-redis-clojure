/**
 * Copyright 2012 Preemptive Labs / Andreas Bielk (http://www.preemptive.se)
 *
 *************************************************************************************/
package labs.redis;


public class BulkReply extends Reply
{
  public static final char MARKER = '$';
  public final byte[] bytes;

  public BulkReply(byte[] bytes)
  {
    this.bytes = bytes;
  }

  @Override
  public byte[] getValue()
  {
    return bytes;
  }

  @Override
  public String toString()
  {

    return "BulkReply{" +
      "bytes=" + (bytes == null ? "null" : bytes.length) +
      '}';
  }
}
