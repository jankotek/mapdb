package org.mapdb;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import java.lang.management.*;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This memory warning system will call the listener when we
 * exceed the percentage of available memory specified.  There
 * should only be one instance of this object created, since the
 * usage threshold can only be set to one number.
 *<p/>
 * taken from
 * http://www.javaspecialists.eu/archive/Issue092.html
 * @author  Dr. Heinz M. Kabutz
 * Updated for JDBM by Jan Kotek
 */
public class MemoryLowWarningSystem {
  private static  final Collection<Runnable> listeners =
      new CopyOnWriteArrayList<Runnable>();

//  public interface Listener {
//    public void memoryUsageLow(long usedMemory, long maxMemory);
//  }

    public static final NotificationListener LISTENER = new NotificationListener() {
        public void handleNotification(Notification n, Object hb) {
            if (n.getType().equals(
                    MemoryNotificationInfo.MEMORY_THRESHOLD_EXCEEDED)) {
//          long maxMemory = tenuredGenPool.getUsage().getMax();
//          long usedMemory = tenuredGenPool.getUsage().getUsed();
                for (Runnable listener : listeners) {
                    listener.run();
                }
            }
        }
    };


  public static synchronized void addListener(Runnable listener) {
    listeners.add(listener);
     if(listeners.size()==1){
        MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
        NotificationEmitter emitter = (NotificationEmitter) mbean;
        emitter.addNotificationListener(LISTENER, null, null);
     }

  }

  public static  synchronized void  removeListener(Runnable listener) {
    listeners.remove(listener);
      if(listeners.isEmpty()){
          //unregister to save some memory
          MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
          NotificationEmitter emitter = (NotificationEmitter) mbean;
          try {
              emitter.removeNotificationListener(LISTENER);
          } catch (ListenerNotFoundException e) {

          }
      }

  }

  private static final MemoryPoolMXBean tenuredGenPool =
      findTenuredGenPool();

  private static void setPercentageUsageThreshold(double percentage) {
    if (percentage <= 0.0 || percentage > 1.0) {
      throw new IllegalArgumentException("Percentage not in range");
    }
    long maxMemory = tenuredGenPool.getUsage().getMax();
    long warningThreshold = (long) (maxMemory * percentage);
    tenuredGenPool.setUsageThreshold(warningThreshold);
  }

  /**
   * Tenured Space Pool can be determined by it being of type
   * HEAP and by it being possible to set the usage threshold.
   */
  private static MemoryPoolMXBean findTenuredGenPool() {
    for (MemoryPoolMXBean pool :
        ManagementFactory.getMemoryPoolMXBeans()) {
      // I don't know whether this approach is better, or whether
      // we should rather check for the pool name "Tenured Gen"?
      if (pool.getType() == MemoryType.HEAP &&
          pool.isUsageThresholdSupported()) {
        return pool;
      }
    }
    throw new AssertionError("Could not find tenured space");
  }


    static{
        setPercentageUsageThreshold(0.75);
    }

}
  


