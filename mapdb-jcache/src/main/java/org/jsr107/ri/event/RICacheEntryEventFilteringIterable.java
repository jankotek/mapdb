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

/**
 * An adapter to provide {@link Iterable}s over Cache Entries, those of which
 * are filtered using a {@link CacheEntryEventFilter}.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author Brian Oliver
 */
public class RICacheEntryEventFilteringIterable<K, V> implements Iterable<CacheEntryEvent<K, V>> {

  /**
   * The underlying {@link Iterable} to filter.
   */
  private Iterable<CacheEntryEvent<K, V>> iterable;

  /**
   * The filter to apply to entries in the produced {@link Iterator}s.
   */
  private CacheEntryEventFilter<? super K, ? super V> filter;

  /**
   * Constructs an {@link RICacheEntryEventFilteringIterable}.
   *
   * @param iterable the underlying iterable to filter
   * @param filter   the filter to apply to entries in the iterable
   */
  public RICacheEntryEventFilteringIterable(Iterable<CacheEntryEvent<K, V>> iterable,
                                            CacheEntryEventFilter<? super K, ? super V> filter) {
    this.iterable = iterable;
    this.filter = filter;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<CacheEntryEvent<K, V>> iterator() {
    return new RICacheEntryEventFilteringIterator<K, V>(iterable.iterator(), filter);
  }
}
