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
package org.jsr107.ri.spi;

import org.jsr107.ri.RICacheManager;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Properties;
import java.util.WeakHashMap;

/**
 * The reference implementation of the {@link CachingProvider}.
 *
 * @author Brian Oliver
 */
public class RICachingProvider implements CachingProvider {

  /**
   * The CacheManagers scoped by ClassLoader and URI.
   */
  private WeakHashMap<ClassLoader, HashMap<URI, CacheManager>> cacheManagersByClassLoader;

  /**
   * Constructs an RICachingProvider.
   */
  public RICachingProvider() {
    this.cacheManagersByClassLoader = new WeakHashMap<ClassLoader, HashMap<URI, CacheManager>>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
    URI managerURI = uri == null ? getDefaultURI() : uri;
    ClassLoader managerClassLoader = classLoader == null ? getDefaultClassLoader() : classLoader;
    Properties managerProperties = properties == null ? new Properties() : properties;

    HashMap<URI, CacheManager> cacheManagersByURI = cacheManagersByClassLoader.get(managerClassLoader);

    if (cacheManagersByURI == null) {
      cacheManagersByURI = new HashMap<URI, CacheManager>();
    }

    CacheManager cacheManager = cacheManagersByURI.get(managerURI);

    if (cacheManager == null) {
      cacheManager = new RICacheManager(this, managerURI, managerClassLoader, managerProperties);

      cacheManagersByURI.put(managerURI, cacheManager);
    }

    if (!cacheManagersByClassLoader.containsKey(managerClassLoader)) {
      cacheManagersByClassLoader.put(managerClassLoader, cacheManagersByURI);
    }

    return cacheManager;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CacheManager getCacheManager(URI uri, ClassLoader classLoader) {
    return getCacheManager(uri, classLoader, getDefaultProperties());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CacheManager getCacheManager() {
    return getCacheManager(getDefaultURI(), getDefaultClassLoader(), null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ClassLoader getDefaultClassLoader() {
    return getClass().getClassLoader();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public URI getDefaultURI() {
    try {
      return new URI(this.getClass().getName());
    } catch (URISyntaxException e) {
      throw new CacheException(
          "Failed to create the default URI for the javax.cache Reference Implementation",
          e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Properties getDefaultProperties() {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void close() {
    WeakHashMap<ClassLoader, HashMap<URI, CacheManager>> managersByClassLoader = this.cacheManagersByClassLoader;
    this.cacheManagersByClassLoader = new WeakHashMap<ClassLoader, HashMap<URI, CacheManager>>();

    for (ClassLoader classLoader : managersByClassLoader.keySet()) {
      for (CacheManager cacheManager : managersByClassLoader.get(classLoader).values()) {
        cacheManager.close();
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void close(ClassLoader classLoader) {
    ClassLoader managerClassLoader = classLoader == null ? getDefaultClassLoader() : classLoader;

    HashMap<URI, CacheManager> cacheManagersByURI = cacheManagersByClassLoader.remove(managerClassLoader);

    if (cacheManagersByURI != null) {
      for (CacheManager cacheManager : cacheManagersByURI.values()) {
        cacheManager.close();
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void close(URI uri, ClassLoader classLoader) {
    URI managerURI = uri == null ? getDefaultURI() : uri;
    ClassLoader managerClassLoader = classLoader == null ? getDefaultClassLoader() : classLoader;

    HashMap<URI, CacheManager> cacheManagersByURI = cacheManagersByClassLoader.get(managerClassLoader);
    if (cacheManagersByURI != null) {
      CacheManager cacheManager = cacheManagersByURI.remove(managerURI);

      if (cacheManager != null) {
        cacheManager.close();
      }

      if (cacheManagersByURI.size() == 0) {
        cacheManagersByClassLoader.remove(managerClassLoader);
      }
    }
  }

  /**
   * Releases the CacheManager with the specified URI and ClassLoader
   * from this CachingProvider.  This does not close the CacheManager.  It
   * simply releases it from being tracked by the CachingProvider.
   * <p>
   * This method does nothing if a CacheManager matching the specified
   * parameters is not being tracked.
   * </p>
   * @param uri         the URI of the CacheManager
   * @param classLoader the ClassLoader of the CacheManager
   */
  public synchronized void releaseCacheManager(URI uri, ClassLoader classLoader) {
    URI managerURI = uri == null ? getDefaultURI() : uri;
    ClassLoader managerClassLoader = classLoader == null ? getDefaultClassLoader() : classLoader;

    HashMap<URI, CacheManager> cacheManagersByURI = cacheManagersByClassLoader.get(managerClassLoader);
    if (cacheManagersByURI != null) {
      cacheManagersByURI.remove(managerURI);

      if (cacheManagersByURI.size() == 0) {
        cacheManagersByClassLoader.remove(managerClassLoader);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isSupported(OptionalFeature optionalFeature) {
    switch (optionalFeature) {

      case STORE_BY_REFERENCE:
        return true;

      default:
        return false;
    }
  }
}
