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

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A adapter to {@link Iterator}s to allow filtering of {@link CacheEntryEvent}s
 *
 * @param <K> the type of keys
 * @param <V> the type of value
 * @author Brian Oliver
 */
public class RICacheEntryEventFilteringIterator<K, V> implements Iterator<CacheEntryEvent<K, V>> {

  /**
   * The underlying iterator to filter.
   */
  private Iterator<CacheEntryEvent<K, V>> iterator;

  /**
   * The filter to apply to Cache Entry Events in the {@link Iterator}.
   */
  private CacheEntryEventFilter<? super K, ? super V> filter;

  /**
   * The next available Cache Entry Event that satisfies the filter.
   * (when null we must seek to find the next event)
   */
  private CacheEntryEvent<K, V> nextEntry;

  /**
   * Constructs an {@link RICacheEntryEventFilteringIterator}.
   *
   * @param iterator the underlying iterator to filter
   * @param filter   the filter to apply to entries in the iterator
   */
  public RICacheEntryEventFilteringIterator(Iterator<CacheEntryEvent<K, V>> iterator,
                                            CacheEntryEventFilter<? super K, ? super V> filter) {
    this.iterator = iterator;
    this.filter = filter;
    this.nextEntry = null;
  }

  /**
   * Fetches the next available, entry that satisfies the filter from
   * the underlying iterator
   */
  private void fetch() {
    while (nextEntry == null && iterator.hasNext()) {
      CacheEntryEvent<K, V> entry = iterator.next();

      if (filter.evaluate(entry)) {
        nextEntry = entry;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasNext() {
    if (nextEntry == null) {
      fetch();
    }
    return nextEntry != null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CacheEntryEvent<K, V> next() {
    if (hasNext()) {
      CacheEntryEvent<K, V> entry = nextEntry;

      //reset nextEntry to force fetching the next available entry
      nextEntry = null;

      return entry;
    } else {
      throw new NoSuchElementException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void remove() {
    iterator.remove();
    nextEntry = null;
  }
}
