/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.mapdb;


import javax.management.*;
import javax.management.NotificationListener;
import java.lang.management.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Cache created objects using hard reference.
 * It auto-clears on low memory to prevent OutOfMemoryException.
 *
 * @author Jan Kotek
 */
public class CacheHardRef extends CacheLRU {


    protected static final Object NULL = new Object();

    protected final Runnable lowMemoryListener = new Runnable() {
        @Override
        public void run() {
            cache.clear();
            //TODO clear() may have high overhead, maybe just create new map instance
        }
    };


    public CacheHardRef(Engine engine, int initialCapacity) {
        super(engine, new LongConcurrentHashMap<Object>(initialCapacity));
        addMemoryLowListener(lowMemoryListener);
    }


    @Override
    public void close() {
        removeMemoryLowListener(lowMemoryListener);
        super.close();
    }




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
    private static  final Collection<Runnable> memoryLowListeners =
            new CopyOnWriteArrayList<Runnable>();


    public static final NotificationListener LISTENER = new NotificationListener() {
        @Override
		public void handleNotification(Notification n, Object hb) {
            if (n.getType().equals(
                    MemoryNotificationInfo.MEMORY_THRESHOLD_EXCEEDED)) {
//          long maxMemory = tenuredGenPool.getUsage().getMax();
//          long usedMemory = tenuredGenPool.getUsage().getUsed();
                for (Runnable listener : memoryLowListeners) {
                    listener.run();
                }
            }
        }
    };


    public static synchronized void addMemoryLowListener(Runnable listener) {
        memoryLowListeners.add(listener);
        if(memoryLowListeners.size()==1){
            MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
            NotificationEmitter emitter = (NotificationEmitter) mbean;
            emitter.addNotificationListener(LISTENER, null, null);
        }

    }

    public static  synchronized void  removeMemoryLowListener(Runnable listener) {
        memoryLowListeners.remove(listener);
        if(memoryLowListeners.isEmpty()){
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
