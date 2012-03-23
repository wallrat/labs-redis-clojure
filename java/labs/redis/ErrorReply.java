/**
 * Copyright 2012 Preemptive Labs / Andreas Bielk (http://www.preemptive.se)
 *
 *************************************************************************************/
package labs.redis;

public class ErrorReply extends Reply
{
  public static final char MARKER = '-';
  private final byte[] error;

  public ErrorReply(byte[] error) {
    this.error = error;
  }

  @Override
  public String getValue()
  {
    return new String(error);
  }

  @Override
  public String toString()
  {
    return "ErrorReply{" +
      "error='" + getValue() + '\'' +
      '}';
  }

}
