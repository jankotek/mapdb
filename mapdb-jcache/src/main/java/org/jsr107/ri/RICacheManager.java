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

package org.jsr107.ri;

import org.jsr107.ri.spi.RICachingProvider;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The reference implementation of the {@link CacheManager}.
 *
 * @author Yannis Cosmadopoulos
 * @author Brian Oliver
 * @since 1.0
 */
public class RICacheManager implements CacheManager {

  private static final Logger LOGGER = Logger.getLogger("javax.cache");
  private final HashMap<String, RICache<?, ?>> caches = new HashMap<String, RICache<?, ?>>();

  private final RICachingProvider cachingProvider;

  private final URI uri;
  private final WeakReference<ClassLoader> classLoaderReference;
  private final Properties properties;

  private volatile boolean isClosed;

  /**
   * Constructs a new RICacheManager with the specified name.
   *
   * @param cachingProvider the CachingProvider that created the CacheManager
   * @param uri             the name of this cache manager
   * @param classLoader     the ClassLoader that should be used in converting values into Java Objects.
   * @param properties      the vendor specific Properties for the CacheManager
   * @throws NullPointerException if the URI and/or classLoader is null.
   */
  public RICacheManager(RICachingProvider cachingProvider, URI uri, ClassLoader classLoader, Properties properties) {
    this.cachingProvider = cachingProvider;

    if (uri == null) {
      throw new NullPointerException("No CacheManager URI specified");
    }
    this.uri = uri;

    if (classLoader == null) {
      throw new NullPointerException("No ClassLoader specified");
    }
    this.classLoaderReference = new WeakReference<ClassLoader>(classLoader);

    this.properties = properties == null ? new Properties() : new Properties(properties);

    isClosed = false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CachingProvider getCachingProvider() {
    return cachingProvider;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void close() {
    if (!isClosed()) {
      //first releaseCacheManager the CacheManager from the CacheProvider so that
      //future requests for this CacheManager won't return this one
      cachingProvider.releaseCacheManager(getURI(), getClassLoader());

      isClosed = true;

      ArrayList<Cache<?, ?>> cacheList;
      synchronized (caches) {
        cacheList = new ArrayList<Cache<?, ?>>(caches.values());
        caches.clear();
      }
      for (Cache<?, ?> cache : cacheList) {
        try {
          cache.close();
        } catch (Exception e) {
          getLogger().log(Level.WARNING, "Error stopping cache: " + cache, e);
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isClosed() {
    return isClosed;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public URI getURI() {
    return uri;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Properties getProperties() {
    return properties;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ClassLoader getClassLoader() {
    return classLoaderReference.get();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C configuration) {
    if (isClosed()) {
      throw new IllegalStateException();
    }

    if (cacheName == null) {
      throw new NullPointerException("cacheName must not be null");
    }

    if (configuration == null) {
      throw new NullPointerException("configuration must not be null");
    }

    synchronized (caches) {
      RICache<?, ?> cache = caches.get(cacheName);

      if (cache == null) {
        cache = new RICache(this, cacheName, getClassLoader(), configuration);
        caches.put(cache.getName(), cache);

        return (Cache<K, V>) cache;
      } else {
        throw new CacheException("A cache named " + cacheName + " already exists.");
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
    if (isClosed()) {
      throw new IllegalStateException();
    }

    if (keyType == null) {
      throw new NullPointerException("keyType can not be null");
    }

    if (valueType == null) {
      throw new NullPointerException("valueType can not be null");
    }

    synchronized (caches) {
      RICache<?, ?> cache = caches.get(cacheName);

      if (cache == null) {
        return null;
      } else {
        Configuration<?, ?> configuration = cache.getConfiguration(CompleteConfiguration.class);

        if (configuration.getKeyType() != null &&
            configuration.getKeyType().equals(keyType)) {

          if (configuration.getValueType() != null &&
              configuration.getValueType().equals(valueType)) {

            return (Cache<K, V>) cache;
          } else {
            throw new ClassCastException("Incompatible cache value types specified, expected " +
                configuration.getValueType() + " but " + valueType + " was specified");
          }
        } else {
          throw new ClassCastException("Incompatible cache key types specified, expected " +
              configuration.getKeyType() + " but " + keyType + " was specified");
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Cache getCache(String cacheName) {
    if (isClosed()) {
      throw new IllegalStateException();
    }
    synchronized (caches) {
      RICache cache = caches.get(cacheName);

      if (cache == null) {
        return null;
      } else {
        Configuration configuration = cache.getConfiguration(CompleteConfiguration.class);

        if (configuration.getKeyType().equals(Object.class) &&
            configuration.getValueType().equals(Object.class)) {
          return cache;
        } else {
          throw new IllegalArgumentException("Cache " + cacheName + " was " +
              "defined with specific types Cache<" +
              configuration.getKeyType() + ", " + configuration.getValueType() + "> " +
              "in which case CacheManager.getCache(String, Class, Class) must be used");
        }

      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<String> getCacheNames() {
    synchronized (caches) {
      HashSet<String> set = new HashSet<String>();
      for (Cache<?, ?> cache : caches.values()) {
        set.add(cache.getName());
      }
      return Collections.unmodifiableSet(set);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void destroyCache(String cacheName) {
    if (isClosed()) {
      throw new IllegalStateException();
    }
    if (cacheName == null) {
      throw new NullPointerException();
    }

    Cache<?, ?> cache;
    synchronized (caches) {
      cache = caches.get(cacheName);
    }

    if (cache != null) {
      cache.close();
    }
  }

  /**
   * Releases the Cache with the specified name from being managed by
   * this CacheManager.
   *
   * @param cacheName the name of the Cache to releaseCacheManager
   */
  void releaseCache(String cacheName) {
    if (cacheName == null) {
      throw new NullPointerException();
    }
    synchronized (caches) {
      caches.remove(cacheName);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void enableStatistics(String cacheName, boolean enabled) {
    if (isClosed()) {
      throw new IllegalStateException();
    }
    if (cacheName == null) {
      throw new NullPointerException();
    }
    ((RICache) caches.get(cacheName)).setStatisticsEnabled(enabled);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void enableManagement(String cacheName, boolean enabled) {
    if (isClosed()) {
      throw new IllegalStateException();
    }
    if (cacheName == null) {
      throw new NullPointerException();
    }
    ((RICache) caches.get(cacheName)).setManagementEnabled(enabled);
  }

  @Override
  public <T> T unwrap(java.lang.Class<T> cls) {
    if (cls.isAssignableFrom(getClass())) {
      return cls.cast(this);
    }

    throw new IllegalArgumentException("Unwapping to " + cls + " is not a supported by this implementation");
  }

  /**
   * Obtain the logger.
   *
   * @return the logger.
   */
  Logger getLogger() {
    return LOGGER;
  }
}
