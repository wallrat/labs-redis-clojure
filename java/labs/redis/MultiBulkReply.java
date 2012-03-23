/**
 * Copyright 2012 Preemptive Labs / Andreas Bielk (http://www.preemptive.se)
 *
 *************************************************************************************/
package labs.redis;

public class MultiBulkReply extends Reply
{
  public static final char MARKER = '*';
  public final Reply[] values;

  public MultiBulkReply(Reply[] values)
  {
    this.values = values;
  }

  @Override
  public Reply[] getValue()
  {
    return values;
  }

  @Override
  public String toString()
  {
    return "MultiBulkReply{" +
      "byteArrays=" + values.length +
      '}';
  }
}
