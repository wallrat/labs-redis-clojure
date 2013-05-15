/*
 * Inspiration (and some code) from https://github.com/spullara/redis-protocol Copyright 2012 Sam Pullara
 * (no original copyright notice in source, originally Apache License 2.0)
 *
 * Copyright 2012 Preemptive Labs / Andreas Bielk (http://www.preemptive.se)
 *************************************************************************************/
package labs.redis;

import java.io.*;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is not thread-safe. Sync is implemented in Client for writer-side
 * and LinkedFuture for reader-side.
 * <p/>
 * No, sync should be in Connection, because readAsync touches os.flush().
 * The bad think is we also need to synch in Client, because we use tail.
 * Can we do smthng else? Compare and swap?
 */
public class Connection
{
  public static final Charset US_ASCII = Charset.forName("US-ASCII");
  public static final Charset UTF8 = Charset.forName("UTF-8");

  public static final byte[] ARGS_PREFIX = "*".getBytes();
  public static final byte[] CRLF = "\r\n".getBytes();
  public static final byte[] OK = "OK".getBytes(US_ASCII);
  public static final byte[] PONG = "PONG".getBytes(US_ASCII);
  public static final byte[] QUEUED = "QUEUED".getBytes(US_ASCII);
  public static final byte[] BYTES_PREFIX = "$".getBytes();
  public static final byte[] EMPTY_BYTES = new byte[0];
  public static final byte[] NEG_ONE = convert(-1, false);
  public static final byte[] NEG_ONE_WITH_CRLF = convert(-1, true);
  public static final char LF = '\n';
  public static final char CR = '\r';
  private static final char ZERO = '0';

  final AtomicInteger pipelined = new AtomicInteger(0);

  public final Socket socket;
  public boolean failed = false;

  private final BufferedInputStream is;
  private final OutputStream os;

  public Connection(Socket socket)
    throws IOException
  {
    this.socket = socket;
    is = new BufferedInputStream(socket.getInputStream());
    os = new BufferedOutputStream(socket.getOutputStream());
  }

  public void close()
    throws IOException
  {
    if (!socket.isClosed())
      socket.close();
  }


  public boolean isConnected()
  {
    return socket != null
      && socket.isBound()
      && !socket.isClosed()
      && socket.isConnected()
      && !socket.isInputShutdown()
      && !socket.isOutputShutdown()
      && !failed;
  }


  ////////////////////////////////////////////// read
  public Reply receive()
    throws IOException
  {
    try
    {
//synchronized (os)
      {
        os.flush();
      }

      //synchronized (is)
      {
        Reply r = receiveReply(is);
        pipelined.decrementAndGet();
        return r;
      }
    }
    catch (IOException e)
    {
      this.failed = true;
      throw e;
    }
  }


  private Reply receiveReply(InputStream is)
    throws IOException
  {
    int code = is.read();
    switch (code)
    {
      case StatusReply.MARKER:
      {
        byte[] buf = readStatus(is);

        // optimze 'OK\r\n'
        if (buf == OK)
          return StatusReply.OK;

        // optimze 'PONG\r\n'
        if (buf == PONG)
          return StatusReply.PONG;

        // TODO: handle QUEUED
        if (Arrays.equals(QUEUED, buf))
          return StatusReply.QUEUED;

        return new StatusReply(buf);
      }
      case ErrorReply.MARKER:
      {
        return new ErrorReply(readStatus(is));
      }
      case IntegerReply.MARKER:
      {
        return new IntegerReply(readInteger(is));
      }
      case BulkReply.MARKER:
      {
        return new BulkReply(readBytes(is));
      }
      case MultiBulkReply.MARKER:
      {
        return new MultiBulkReply(readMultiBulk(is));
      }
      default:
      {
        throw new IOException("Unexpected character in stream: " + code);
      }
    }
  }

  private Reply[] readMultiBulk(InputStream is)
    throws IOException
  {
    int size = readInteger(is);
    if (size == -1)
      return null;

    Reply[] values = new Reply[size];
    for (int i = 0; i < values.length; i++)
      values[i] = receiveReply(is);
    return values;
  }

  private static byte[] readStatus(InputStream is)
    throws IOException
  {
    // DataInputStream.readLine
    byte buf[] = new byte[128];
    int room = buf.length;
    int offset = 0;
    int c;

    loop:
    while (true)
    {
      switch (c = is.read())
      {
        case -1:
        case '\n':
          break loop;

        case '\r':
          int c2 = is.read();
          if ((c2 != '\n') && (c2 != -1))
          {
            if (!(is instanceof PushbackInputStream))
              is = new PushbackInputStream(is);

            ((PushbackInputStream) is).unread(c2);
          }
          break loop;

        default:
          if (--room < 0)
          {
            byte[] newBuffer = new byte[offset + 128];
            room = buf.length - offset - 1;
            System.arraycopy(newBuffer, 0, buf, 0, offset);
            buf = newBuffer;
          }

          buf[offset++] = (byte) c;
          break;
      }
    }

    if ((c == -1) && (offset == 0))
    {
      return null;
    }

    // optimze 'OK\r\n'
    if (offset == 2 && buf[0] == 'O' && buf[1] == 'K')
      return OK;

    // optimze 'PONG\r\n'
    if (offset == 4 && buf[0] == 'P' && buf[1] == 'O' && buf[2] == 'N' && buf[3] == 'G')
      return PONG;

    //TODO: optimize 'QUEUED\r\n'

    byte[] r = new byte[offset];
    System.arraycopy(buf, 0, r, 0, offset);
    return r;
  }

  private static int readInteger(InputStream is)
    throws IOException
  {
    int size = 0;
    int sign = 1;
    int read = is.read();
    if (read == '-')
    {
      read = is.read();
      sign = -1;
    }

    do
    {
      if (read == CR)
      {
        if (is.read() == LF)
        {
          break;
        }
      }

      int value = read - ZERO;
      if (value >= 0 && value < 10)
      {
        size *= 10;
        size += value;
      }
      else
      {
        throw new IOException("Invalid character in integer");
      }

      read = is.read();
    }
    while (true);

    return size * sign;
  }

  private static byte[] readBytes(InputStream is)
    throws IOException
  {
    int size = readInteger(is);
    int read;
    if (size == -1)
      return null;

    byte[] bytes = new byte[size];
    int total = 0;
    while (total < bytes.length && (read = is.read(bytes, total, bytes.length - total)) != -1)
      total += read;

    if (total < bytes.length)
      throw new IOException("Failed to read enough bytes: " + total);

    int cr = is.read();
    int lf = is.read();
    if (cr != CR || lf != LF)
      throw new IOException("Improper line ending: " + cr + ", " + lf);

    return bytes;
  }


  ////////////////////////////////////////////// write

  public void send(Object[] objects)
    throws IOException
  {
    try
    {
//synchronized (os)
      {
        write(os, objects);
        pipelined.incrementAndGet();
      }
    }
    catch (IOException e)
    {
      failed = true;
      throw e;
    }
  }

  private static void write(OutputStream os, Object... objects)
    throws IOException
  {
    os.write(ARGS_PREFIX);
    os.write(numToBytes(objects.length, true));

    for (Object object : objects)
    {
      os.write(BYTES_PREFIX);

      byte[] b;
      if (object == null)
        b = EMPTY_BYTES;
      else if (object instanceof byte[])
        b = (byte[]) object;
      else if (object instanceof Number)
        b = numToBytes(((Number) object).longValue(), false);
      else
        b = object.toString().getBytes(UTF8);

      os.write(numToBytes(b.length, true));
      os.write(b);
      os.write(CRLF);
    }
  }


  // itoa impl from https://github.com/spullara/redis-protocol  Copyright 2012 Sam Pullara
  private static final int NUM_MAP_LENGTH = 256;
  private static final byte[][] numMap = new byte[NUM_MAP_LENGTH][];
  private static final byte[][] numMapWithCRLF = new byte[NUM_MAP_LENGTH][];

  static
  {
    for (int i = 0; i < NUM_MAP_LENGTH; i++)
    {
      numMap[i] = convert(i, false);
      numMapWithCRLF[i] = convert(i, true);
    }
  }


  // Optimized for the direct to ASCII bytes case
  // Could be even more optimized but it is already
  // about twice as fast as using Long.toString().getBytes()
  private static byte[] numToBytes(long value, boolean withCRLF)
  {
    if (value >= 0 && value < NUM_MAP_LENGTH)
    {
      int index = (int) value;
      return withCRLF ? numMapWithCRLF[index] : numMap[index];
    }
    else if (value == -1)
    {
      return withCRLF ? NEG_ONE_WITH_CRLF : NEG_ONE;
    }
    return convert(value, withCRLF);
  }

  private static byte[] convert(long value, boolean withCRLF)
  {
    boolean negative = value < 0;
    int index = negative ? 2 : 1;
    long current = negative ? -value : value;
    while ((current /= 10) > 0)
    {
      index++;
    }
    byte[] bytes = new byte[withCRLF ? index + 2 : index];
    if (withCRLF)
    {
      bytes[index + 1] = LF;
      bytes[index] = CR;
    }
    if (negative)
    {
      bytes[0] = '-';
    }
    current = negative ? -value : value;
    long tmp = current;
    while ((tmp /= 10) > 0)
    {
      bytes[--index] = (byte) ('0' + (current % 10));
      current = tmp;
    }
    bytes[--index] = (byte) ('0' + current);
    return bytes;
  }

  @Override
  public String toString()
  {
    return "Connection{" +
      "pipelined=" + pipelined +
      ", socket=" + socket +
      '}';
  }
}
