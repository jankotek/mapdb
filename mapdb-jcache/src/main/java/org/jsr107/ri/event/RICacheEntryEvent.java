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

package org.jsr107.ri.event;

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.EventType;

/**
 * The reference implementation of the {@link CacheEntryEvent}.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of cached values
 * @author Greg Luck
 * @since 1.0
 */
public class RICacheEntryEvent<K, V> extends CacheEntryEvent<K, V> {

  private K key;
  private V value;
  private V oldValue;
  private boolean oldValueAvailable;

  /**
   * Constructs a cache entry event from a given cache as source
   * (without an old value)
   *
   * @param source the cache that originated the event
   * @param key    the key
   * @param value  the value
   */
  public RICacheEntryEvent(Cache<K, V> source, K key, V value, EventType eventType) {
    super(source, eventType);
    this.key = key;
    this.value = value;
    this.oldValue = null;
    this.oldValueAvailable = false;
  }

  /**
   * Constructs a cache entry event from a given cache as source
   * (with an old value)
   *
   * @param source   the cache that originated the event
   * @param key      the key
   * @param value    the value
   * @param oldValue the oldValue
   */
  public RICacheEntryEvent(Cache<K, V> source, K key, V value, V oldValue, EventType eventType) {
    super(source, eventType);
    this.key = key;
    this.value = value;
    this.oldValue = oldValue;
    this.oldValueAvailable = true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public K getKey() {
    return key;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V getValue() {
    return value;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V getOldValue() throws UnsupportedOperationException {
    if (isOldValueAvailable()) {
      return oldValue;
    } else {
      throw new UnsupportedOperationException("Old value is not available for key");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> T unwrap(Class<T> clazz) {
    if (clazz != null && clazz.isInstance(this)) {
      return (T) this;
    } else {
      throw new IllegalArgumentException("The class " + clazz + " is unknown to this implementation");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isOldValueAvailable() {
    return oldValueAvailable;
  }
}
