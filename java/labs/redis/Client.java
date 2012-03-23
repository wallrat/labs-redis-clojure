/**
 * Copyright 2012 Preemptive Labs / Andreas Bielk (http://www.preemptive.se)
 *
 *************************************************************************************/

package labs.redis;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class Client
{
  public static final Charset US_ASCII = Charset.forName("US-ASCII");
  public static final Charset UTF8 = Charset.forName("UTF-8");

  private static final byte[] EVALSHA_BYTES = "EVALSHA".getBytes(US_ASCII);
  private static final byte[] PING_BYTES = "PING".getBytes(US_ASCII);

  private final Map<String, byte[]> evalCache = new HashMap<String, byte[]>(16);
  public final Connection protocol;
  protected LinkedReplyFuture tail = null;

  public Client(final Socket socket)
    throws IOException
  {
    protocol = new Connection(socket);
  }

  public Client(String host, int port)
    throws IOException
  {
    protocol = new Connection(SocketFactory.newSocket(host, port));
  }

  public Client()
    throws IOException
  {
    this("localhost", 6379);
  }


  public synchronized LinkedReplyFuture pipeline(Object... o)
    throws IOException
  {
    send(o);
    this.tail = new LinkedReplyFuture(protocol, this.tail);
    return this.tail;
  }

  /**
   * Send data to Redis, should be paired with pull()
   */
  public synchronized void send(Object... o)
    throws IOException
  {
    if (protocol.pipelined.get() > 128) tail.ensure();
    protocol.send(o);
  }

  public synchronized LinkedReplyFuture pull()
  {
    this.tail = new LinkedReplyFuture(protocol, this.tail);
    return this.tail;
  }

  public synchronized void close()
    throws IOException
  {
    this.protocol.close();
  }

  // benchmark impl of PING
  public synchronized LinkedReplyFuture ping()
    throws IOException
  {
    send(new Object[]{PING_BYTES});
    this.tail = new LinkedReplyFuture(protocol, this.tail);
    return this.tail;
  }


  public synchronized LinkedReplyFuture eval(String lua, Object[] keys, Object[] args)
    throws IOException
  {
    byte[] sha1 = evalCache.get(lua);
    if (sha1 == null)
    {
      sha1 = (byte[]) pipeline("SCRIPT", "LOAD", lua).get().getValue();
      evalCache.put(lua, sha1);
    }

    Object[] args2 = new Object[keys.length + args.length + 3];
    args2[0] = EVALSHA_BYTES;
    args2[1] = sha1;
    args2[2] = keys.length;
    System.arraycopy(keys, 0, args2, 3, keys.length);
    System.arraycopy(args, 0, args2, 3 + keys.length, args.length);

    send(args2);
    this.tail = new LinkedReplyFuture(protocol, this.tail);
    return this.tail;
  }

  /**
   * EXEC and update tail with results
   */
  public synchronized MultiBulkReply execWithResults()
    throws IOException
  {
    // capture tail
    LinkedReplyFuture t = tail;

    // EXEC
    final MultiBulkReply exec = (MultiBulkReply) pipeline("EXEC").get();

    // update tail
    for (int i = exec.values.length - 1; i >= 0; i--)
    {
      // assertions
      if (t == null) throw new IllegalStateException("Missing tail");
      if (t.value != StatusReply.QUEUED)
        throw new IllegalStateException("Currupt tail, expected QUEUED, got " + t.value.getValue());

      t.value = exec.values[i];
      t = t.tail;
    }

    // assertion
    if (t != null) throw new IllegalStateException("Found longer tail than expected " + t.tail.value);

    return exec;
  }
}
