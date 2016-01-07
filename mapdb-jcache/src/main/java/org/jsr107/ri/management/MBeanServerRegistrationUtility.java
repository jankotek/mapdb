/**
 *  Copyright 2011-2013 Terracotta, Inc.
 *  Copyright 2011-2013 Oracle America Incorporated
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package org.jsr107.ri.management;

import org.jsr107.ri.RICache;
import org.mapdb.jcache.MJCache;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.Set;


/**
 * A convenience class for registering CacheStatisticsMBeans with an MBeanServer.
 *
 * @author Greg Luck
 * @since 1.0
 */
public final class MBeanServerRegistrationUtility {

  //ensure everything gets put in one MBeanServer
  private static MBeanServer mBeanServer = MBeanServerFactory.createMBeanServer();

  /**
   * The type of registered Object
   */
  public enum ObjectNameType {

    /**
     * Cache Statistics
     */
    Statistics,

    /**
     * Cache Configuration
     */
    Configuration

  }


  private MBeanServerRegistrationUtility() {
    //prevent construction
  }


  /**
   * Utility method for registering CacheStatistics with the MBeanServer
   *
   * @param cache the cache to register
   */
  public static void registerCacheObject(MJCache cache,
                                    ObjectNameType objectNameType) {
    //these can change during runtime, so always look it up
    ObjectName registeredObjectName = calculateObjectName(cache, objectNameType);
    try {
      if (objectNameType.equals(ObjectNameType.Configuration)) {
        if (!isRegistered(cache, objectNameType)) {
          mBeanServer.registerMBean(cache.getCacheMXBean(), registeredObjectName);
        }
      } else if (objectNameType.equals(ObjectNameType.Statistics)) {
        if (!isRegistered(cache, objectNameType)) {
          mBeanServer.registerMBean(cache.getStatistics(), registeredObjectName);
        }
      }
    } catch (Exception e) {
      throw new CacheException("Error registering cache MXBeans for CacheManager "
          + registeredObjectName + " . Error was " + e.getMessage(), e);
    }
  }


  /**
   * Checks whether an ObjectName is already registered.
   *
   * @throws CacheException - all exceptions are wrapped in CacheException
   */
  static boolean isRegistered(MJCache cache, ObjectNameType objectNameType) {

    Set<ObjectName> registeredObjectNames = null;

    ObjectName objectName = calculateObjectName(cache, objectNameType);
    registeredObjectNames = mBeanServer.queryNames(objectName, null);

    return !registeredObjectNames.isEmpty();
  }


  /**
   * Removes registered CacheStatistics for a Cache
   *
   * @throws CacheException - all exceptions are wrapped in CacheException
   */
  public static void unregisterCacheObject(MJCache cache,
                                     ObjectNameType objectNameType) {

    Set<ObjectName> registeredObjectNames = null;

    ObjectName objectName = calculateObjectName(cache, objectNameType);
    registeredObjectNames = mBeanServer.queryNames(objectName, null);

    //should just be one
    for (ObjectName registeredObjectName : registeredObjectNames) {
      try {
        mBeanServer.unregisterMBean(registeredObjectName);
      } catch (Exception e) {
        throw new CacheException("Error unregistering object instance "
            + registeredObjectName + " . Error was " + e.getMessage(), e);
      }
    }
  }

  /**
   * Creates an object name using the scheme
   * "javax.cache:type=Cache&lt;Statistics|Configuration&gt;,CacheManager=&lt;cacheManagerName&gt;,name=&lt;cacheName&gt;"
   */
  private static ObjectName calculateObjectName(Cache cache, ObjectNameType objectNameType) {
    String cacheManagerName = mbeanSafe(cache.getCacheManager().getURI().toString());
    String cacheName = mbeanSafe(cache.getName());

    try {
      return new ObjectName("javax.cache:type=Cache" + objectNameType + ",CacheManager="
          + cacheManagerName + ",Cache=" + cacheName);
    } catch (MalformedObjectNameException e) {
      throw new CacheException("Illegal ObjectName for Management Bean. " +
          "CacheManager=[" + cacheManagerName + "], Cache=[" + cacheName + "]", e);
    }
  }


  /**
   * Filter out invalid ObjectName characters from string.
   *
   * @param string input string
   * @return A valid JMX ObjectName attribute value.
   */
  private static String mbeanSafe(String string) {
    return string == null ? "" : string.replaceAll(",|:|=|\n", ".");
  }

}

