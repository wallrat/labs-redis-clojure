/**
 * Copyright 2012 Preemptive Labs / Andreas Bielk (http://www.preemptive.se)
 *
 *************************************************************************************/
package labs.redis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class SocketFactory
{
  public static final int TIMEOUT = 10000;

  public static Socket newSocket(String host, int port)
    throws IOException
  {
    return newSocket(host,port,SocketFactory.TIMEOUT);
  }

  public static Socket newSocket(String host, int port, int timeout)
    throws IOException
  {
    final Socket socket = new Socket();

    socket.setReuseAddress(true);
    socket.setKeepAlive(true);  //Will monitor the TCP connection is valid
    socket.setTcpNoDelay(true);  //Socket buffer Whetherclosed, to ensure timely delivery of data
    socket.setSoLinger(true, 0);  //Control calls close () method, the underlying socket is closed immediately

    socket.connect(new InetSocketAddress(host, port), timeout);
    //socket.setSoTimeout(timeout);

    return socket;
  }
}
