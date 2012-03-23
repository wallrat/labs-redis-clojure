/**
 * Copyright 2012 Preemptive Labs / Andreas Bielk (http://www.preemptive.se)
 *
 *************************************************************************************/

package labs.redis;

import java.io.IOException;
import clojure.lang.IDeref;

public class LinkedReplyFuture implements IDeref
{
  private final Connection connection;
  LinkedReplyFuture tail;
  protected Reply value;

  public LinkedReplyFuture(Connection connection, LinkedReplyFuture tail)
  {
    this.connection = connection;
    this.tail = tail;
  }

  // QUEUED values will result in Futures keeping it's tail
  // so we can complete them at a later time (EXEC)
  public boolean realizeValue()
    throws IOException
  {
    if (value != null) return (value != StatusReply.QUEUED); // done

    value = this.connection.receive();
    return (value != StatusReply.QUEUED);
  }

  protected synchronized boolean ensure()
    throws IOException
  {
    if (tail != null && tail.ensure())
      tail = null;

    return realizeValue();
  }

  public synchronized Reply get()
    throws IOException
  {
    ensure();
    return value;
  }

  public Object deref()
  {
    try
    {
      return get();
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }
  }
}
