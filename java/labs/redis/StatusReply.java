/**
 * Copyright 2012 Preemptive Labs / Andreas Bielk (http://www.preemptive.se)
 *
 *************************************************************************************/
package labs.redis;

public class StatusReply extends Reply
{
  public static final char MARKER = '+';
  public static final StatusReply OK = new StatusReply("OK");
  public static final StatusReply PONG = new StatusReply("PONG");
  public static final StatusReply QUEUED = new StatusReply("QUEUED");

  private String status;
  private byte[] statusBytes;


  public StatusReply(String status)
  {
    this.status = status;
  }

  public StatusReply(byte[] statusBytes)
  {
    this.statusBytes = statusBytes;
  }

  @Override
  public String getValue()
  {
    if (status == null && statusBytes != null)
      status = new String(statusBytes);

    return status;
  }

  @Override
  public String toString()
  {
    return "StatusReply{" +
      "status='" + getValue() + '\'' +
      '}';
  }

}
