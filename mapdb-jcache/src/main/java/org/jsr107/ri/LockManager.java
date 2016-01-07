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

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A mechanism to manage locks for a collection of objects.
 *
 * @param <K> the type of the object to be locked
 * @author Yannis Cosmadopoulos
 * @author Greg Luck
 */
public final class LockManager<K> {
  private final ConcurrentHashMap<K, ReentrantLock> locks = new ConcurrentHashMap<K, ReentrantLock>();
  private final LockFactory lockFactory = new LockFactory();

  /**
   * Constructor
   */
  LockManager() {
  }

  /**
   * Lock the object
   *
   * @param key the key
   */
  void lock(K key) {
    ReentrantLock lock = lockFactory.getLock();

    while (true) {
      ReentrantLock oldLock = locks.putIfAbsent(key, lock);
      if (oldLock == null) {
        return;
      }
      // there was a lock
      oldLock.lock();
      // now we have it. Because of possibility that someone had it for remove,
      // we don't re-use directly
      lockFactory.release(oldLock);
    }
  }

  /**
   * Unlock the object
   *
   * @param key the object
   */
  void unLock(K key) {
    ReentrantLock lock = locks.remove(key);
    lockFactory.release(lock);
  }

  /**
   * A factory for {@link ReentrantLock}s.
   */
  private static final class LockFactory {
    private static final int CAPACITY = 100;
    private static final ArrayList<ReentrantLock> LOCKS = new ArrayList<ReentrantLock>(CAPACITY);

    private LockFactory() {
    }

    private ReentrantLock getLock() {
      ReentrantLock qLock = null;
      synchronized (LOCKS) {
        if (!LOCKS.isEmpty()) {
          qLock = LOCKS.remove(0);
        }
      }

      ReentrantLock lock = qLock != null ? qLock : new ReentrantLock();
      lock.lock();
      return lock;
    }

    private void release(ReentrantLock lock) {
      lock.unlock();
      synchronized (LOCKS) {
        if (LOCKS.size() <= CAPACITY) {
          LOCKS.add(lock);
        }
      }
    }
  }
}

