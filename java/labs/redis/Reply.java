/**
 * Copyright 2012 Preemptive Labs / Andreas Bielk (http://www.preemptive.se)
 *
 *************************************************************************************/
package labs.redis;
import clojure.lang.IDeref;

public abstract class Reply implements IDeref
{
  public abstract Object getValue();

  public Object deref()
  {
    return getValue();
  }
}
