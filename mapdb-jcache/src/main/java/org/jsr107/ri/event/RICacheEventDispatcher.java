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

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Collects and appropriately dispatches {@link CacheEntryEvent}s to
 * {@link CacheEntryListener}s.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author Brian Oliver
 */
public class RICacheEventDispatcher<K, V> {

  /**
   * The map of {@link CacheEntryEvent}s to deliver, keyed by the class of
   * {@link CacheEntryListener} to which they should be dispatched.
   */
  private ConcurrentHashMap<Class<? extends CacheEntryListener>,
      ArrayList<CacheEntryEvent<K, V>>> eventMap;

  /**
   * Constructs an {@link RICacheEventDispatcher}.
   */
  public RICacheEventDispatcher() {
    this.eventMap = new ConcurrentHashMap<Class<? extends CacheEntryListener>,
        ArrayList<CacheEntryEvent<K, V>>>();
  }

  /**
   * Requests that the specified event be prepared for dispatching to the
   * specified type of listeners.
   *
   * @param listenerClass the class of {@link CacheEntryListener} that should
   *                      receive the event
   * @param event         the event to be dispatched
   */
  public void addEvent(Class<? extends CacheEntryListener> listenerClass,
                       CacheEntryEvent<K, V> event) {
    if (listenerClass == null) {
      throw new NullPointerException("listenerClass can't be null");
    }

    if (event == null) {
      throw new NullPointerException("event can't be null");
    }

    if (!listenerClass.isInterface() || !CacheEntryListener.class.isAssignableFrom(listenerClass)) {
      throw new IllegalArgumentException("listenerClass must be an CacheEntryListener interface");
    }

    //for safety
    ArrayList<CacheEntryEvent<K, V>> eventList;
    synchronized (this) {
      eventList = eventMap.get(listenerClass);
      if (eventList == null) {
        eventList = new ArrayList<CacheEntryEvent<K, V>>();
        eventMap.put(listenerClass, eventList);
      }
    }

    eventList.add(event);
  }

  /**
   * Dispatches the added events to the listeners defined by the specified
   * {@link CacheEntryListenerConfiguration}s.
   *
   * @param registrations the {@link RICacheEntryListenerRegistration}s defining
   *                      {@link CacheEntryListener}s to which to dispatch events
   * @see #addEvent(Class, CacheEntryEvent)
   */
  public void dispatch(Iterable<RICacheEntryListenerRegistration<K, V>> registrations) {

    //TODO: we could really optimize this implementation

    //TODO: we need to handle exceptions here

    //TODO: we need to work out which events should be raised synchronously or asynchronously

    //TODO: we need to remove/hide old values appropriately

    Iterable<CacheEntryEvent<K, V>> events;

    try {

      //notify expiry listeners
      events = eventMap.get(CacheEntryExpiredListener.class);
      if (events != null) {
        for (RICacheEntryListenerRegistration<K, V> registration : registrations) {
          CacheEntryEventFilter<? super K, ? super V> filter = registration.getCacheEntryFilter();
          Iterable<CacheEntryEvent<K, V>> iterable =
              filter == null ? events : new RICacheEntryEventFilteringIterable<K, V>(events, filter);

          CacheEntryListener<? super K, ? super V> listener = registration.getCacheEntryListener();
          if (listener instanceof CacheEntryExpiredListener) {
            ((CacheEntryExpiredListener) listener).onExpired(iterable);
          }
        }
      }

      //notify create listeners
      events = eventMap.get(CacheEntryCreatedListener.class);
      if (events != null) {
        for (RICacheEntryListenerRegistration<K, V> registration : registrations) {
          CacheEntryEventFilter<? super K, ? super V> filter = registration.getCacheEntryFilter();
          Iterable<CacheEntryEvent<K, V>> iterable =
              filter == null ? events : new RICacheEntryEventFilteringIterable<K, V>(events, filter);

          CacheEntryListener<? super K, ? super V> listener = registration.getCacheEntryListener();
          if (listener instanceof CacheEntryCreatedListener) {
            ((CacheEntryCreatedListener) listener).onCreated(iterable);
          }
        }
      }

      //notify update listeners
      events = eventMap.get(CacheEntryUpdatedListener.class);
      if (events != null) {
        for (RICacheEntryListenerRegistration<K, V> registration : registrations) {
          CacheEntryEventFilter<? super K, ? super V> filter = registration.getCacheEntryFilter();
          Iterable<CacheEntryEvent<K, V>> iterable =
              filter == null ? events : new RICacheEntryEventFilteringIterable<K, V>(events, filter);

          CacheEntryListener<? super K, ? super V> listener = registration.getCacheEntryListener();
          if (listener instanceof CacheEntryUpdatedListener) {
            ((CacheEntryUpdatedListener) listener).onUpdated(iterable);
          }
        }
      }

      //notify remove listeners
      events = eventMap.get(CacheEntryRemovedListener.class);
      if (events != null) {
        for (RICacheEntryListenerRegistration<K, V> registration : registrations) {
          CacheEntryEventFilter<? super K, ? super V> filter = registration.getCacheEntryFilter();
          Iterable<CacheEntryEvent<K, V>> iterable =
              filter == null ? events : new RICacheEntryEventFilteringIterable<K, V>(events, filter);

          CacheEntryListener<? super K, ? super V> listener = registration.getCacheEntryListener();
          if (listener instanceof CacheEntryRemovedListener) {
            ((CacheEntryRemovedListener) listener).onRemoved(iterable);
          }
        }
      }
    } catch (Exception e) {
      if (!(e instanceof CacheEntryListenerException)) {
        throw new CacheEntryListenerException("Exception on listener execution", e);
      } else {
        throw e;
      }
    }
  }


}
