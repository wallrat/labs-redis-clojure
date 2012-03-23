/**
 * Copyright 2012 Preemptive Labs / Andreas Bielk (http://www.preemptive.se)
 *
 *************************************************************************************/

package labs.redis;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class ClientPool
{
  private final Queue<Client> queue = new ArrayBlockingQueue<Client>(100, true);
  private final String host;
  private final int port;
  private final boolean testOnBorrow;

  public ClientPool(String host, int port, boolean testOnBorrow)
  {
    this.host = host;
    this.port = port;
    this.testOnBorrow = testOnBorrow;
  }

  public ClientPool(boolean testOnBorrow)
  {
    this("localhost",6379, testOnBorrow);
  }

  public synchronized int size() { return queue.size(); }

  public synchronized Client borrow()
    throws IOException
  {
    final Client client = queue.poll();

    if (client != null)
    {
      if (valid(client))
        return client;
      else
        return borrow();
    }

    return new Client(SocketFactory.newSocket(host, port));
  }

  public synchronized void release(Client client)
  {
    // validate
    if (client != null && valid(client)) queue.add(client);
  }

  private boolean valid(final Client client)
  {
    if (this.testOnBorrow)
    {
      try
      {
        return client.ping().get() == StatusReply.PONG;
      }
      catch (IOException e)
      {
        return false;
      }
    }

    return client.protocol.isConnected();
  }

  public synchronized void flush()
    throws IOException
  {
    Client c = null;
    while((c=queue.poll()) != null)
      c.close();
  }
}
