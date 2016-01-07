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

import javax.cache.Cache;

/**
 * A cache entry implementation.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values*
 * @author Brian Oliver
 * @author Greg Luck
 */
public class RIEntry<K, V> implements Cache.Entry<K, V> {
  private final K key;
  private final V value;
  private final V oldValue;

  /**
   * Constructor
   */
  public RIEntry(K key, V value) {
    this.key = key;
    this.value = value;
    this.oldValue = null;
  }

  /**
   * Constructor
   */
  public RIEntry(K key, V value, V oldValue) {
    this.key = key;
    this.value = value;
    this.oldValue = oldValue;
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
   *
   * @return the old value, if any
   */
  public V getOldValue() {
    return oldValue;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> T unwrap(Class<T> clazz) {
    if (clazz != null && clazz.isInstance(this)) {
      return (T) this;
    } else {
      throw new IllegalArgumentException("Class " + clazz + " is unknown to this implementation");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || ((Object) this).getClass() != o.getClass()) return false;

    RIEntry<?, ?> e2 = (RIEntry<?, ?>) o;

    return this.getKey().equals(e2.getKey()) &&
        this.getValue().equals(e2.getValue()) &&
        (this.oldValue == null && e2.oldValue == null ||
            this.getOldValue().equals(e2.getOldValue()));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return getKey().hashCode();
  }
}

