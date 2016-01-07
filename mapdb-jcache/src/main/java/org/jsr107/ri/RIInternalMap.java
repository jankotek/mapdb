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

import java.util.Map;

/**
 * Provides internal thread-safe storage for a {@link RICache}.
 *
 * @param <K> the type of keys stored
 * @param <V> the type of values stored
 * @author Brian Oliver
 */
interface RIInternalMap<K, V> extends Iterable<Map.Entry<K, V>> {

  /**
   * Gets the value associated with the specified key.  If there is no
   * associated value, <code>null</code> is returned.
   *
   * @param key the key
   * @return the value
   */
  V get(Object key);

  /**
   * Determines if a value is associated with the specified key.
   *
   * @param key the key
   * @return true if a value is associate with the key
   */
  boolean containsKey(Object key);

  /**
   * Associates a value with the specified key.  If a value is already
   * associated with the key, the provided value replaces the previous value.
   *
   * @param key   the key
   * @param value the value
   */
  void put(K key, V value);

  /**
   * Gets the current value associated with the specified key and replaces
   * the current value with the provided value.  If a value is not associated
   * with the key, <code>null</code> is returned.
   *
   * @param key   the key
   * @param value the value
   * @return the old value
   */
  V getAndPut(K key, V value);

  /**
   * Removes the value associated with the key from this structure.
   *
   * @param key the key
   * @return the value removed or <code>null</code> if there was no value
   */
  V remove(Object key);

  /**
   * Removes all values from this structured.
   */
  void clear();

  /**
   * Gets the number of entries in this structure.
   *
   * @return the size
   */
  int size();
}
