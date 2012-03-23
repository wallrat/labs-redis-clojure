/**
 * Copyright 2012 Preemptive Labs / Andreas Bielk (http://www.preemptive.se)
 *
 *************************************************************************************/
package labs.redis;

public class IntegerReply extends Reply
{
  public static final char MARKER = ':';
  public final long integer;

  public IntegerReply(long integer)
  {
    this.integer = integer;
  }

  @Override
  public Long getValue()
  {
    return integer;
  }

  @Override
  public String toString()
  {
    return "IntegerReply{" +
      "integer=" + integer +
      '}';
  }

}
