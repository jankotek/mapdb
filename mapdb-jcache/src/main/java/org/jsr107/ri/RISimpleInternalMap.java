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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A simple implementation of a {@link RIInternalMap} based on a {@link ConcurrentHashMap}.
 *
 * @param <K> the type of keys stored
 * @param <V> the type of values stored
 * @author Brian Oliver
 */
class RISimpleInternalMap<K, V> implements RIInternalMap<K, V> {

  /**
   * The map containing the entries.
   */
  private final ConcurrentHashMap<K, V> internalMap = new ConcurrentHashMap<K, V>();

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsKey(Object key) {
    //noinspection SuspiciousMethodCalls
    return internalMap.containsKey(key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(K key, V value) {
    internalMap.put(key, value);
  }

  @Override
  public V getAndPut(K key, V value) {
    return internalMap.put(key, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V remove(Object key) {
    //noinspection SuspiciousMethodCalls
    return internalMap.remove(key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int size() {
    return internalMap.size();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<Map.Entry<K, V>> iterator() {
    return internalMap.entrySet().iterator();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V get(Object key) {
    //noinspection SuspiciousMethodCalls
    return internalMap.get(key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear() {
    internalMap.clear();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    builder.append(getClass().getName());
    builder.append("{");

    boolean isFirst = true;
    for (K key : internalMap.keySet()) {
      if (isFirst) {
        isFirst = false;
      } else {
        builder.append(", ");
      }

      builder.append("<");
      builder.append(key);
      builder.append(", ");
      builder.append(internalMap.get(key));
      builder.append(">");
    }

    builder.append("}");
    return builder.toString();
  }
}
