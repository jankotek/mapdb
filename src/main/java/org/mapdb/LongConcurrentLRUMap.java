package org.mapdb;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A LRU cache implementation based upon ConcurrentHashMap and other techniques to reduce
 * contention and synchronization overhead to utilize multiple CPU cores more effectively.
 * <p/>
 * Note that the implementation does not follow a true LRU (least-recently-used) eviction
 * strategy. Instead it strives to remove least recently used items but when the initial
 * cleanup does not remove enough items to reach the 'acceptableWaterMark' limit, it can
 * remove more items forcefully regardless of access order.
 *
 * MapDB note: reworked to implement LongMap. Original comes from:
 * https://svn.apache.org/repos/asf/lucene/dev/trunk/solr/core/src/java/org/apache/solr/util/LongConcurrentLRUMap.java
 *
 */
public class LongConcurrentLRUMap<V> extends LongMap<V> {

    protected final LongConcurrentHashMap<CacheEntry<V>> map;
    protected final int upperWaterMark, lowerWaterMark;
    protected final ReentrantLock markAndSweepLock = new ReentrantLock(true);
    protected boolean isCleaning = false;  // not volatile... piggybacked on other volatile vars

    protected final int acceptableWaterMark;
    protected long oldestEntry = 0;  // not volatile, only accessed in the cleaning method


    protected final AtomicLong accessCounter = new AtomicLong(0),
            putCounter = new AtomicLong(0),
            missCounter = new AtomicLong(),
            evictionCounter = new AtomicLong();
    protected final AtomicInteger size = new AtomicInteger();



    public LongConcurrentLRUMap(int upperWaterMark, final int lowerWaterMark, int acceptableWatermark,
                                int initialSize) {
        if (upperWaterMark < 1) throw new IllegalArgumentException("upperWaterMark must be > 0");
        if (lowerWaterMark >= upperWaterMark)
            throw new IllegalArgumentException("lowerWaterMark must be  < upperWaterMark");
        map = new LongConcurrentHashMap<CacheEntry<V>>(initialSize);
        this.upperWaterMark = upperWaterMark;
        this.lowerWaterMark = lowerWaterMark;
        this.acceptableWaterMark = acceptableWatermark;
    }

    public LongConcurrentLRUMap(int size, int lowerWatermark) {
        this(size, lowerWatermark, (int) Math.floor((lowerWatermark + size) / 2),
                (int) Math.ceil(0.75 * size));
    }

    public V get(long key) {
        CacheEntry<V> e = map.get(key);
        if (e == null) {
            missCounter.incrementAndGet();
            return null;
        }
        e.lastAccessed = accessCounter.incrementAndGet();
        return e.value;
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    public V remove(long key) {
        CacheEntry<V> cacheEntry = map.remove(key);
        if (cacheEntry != null) {
            size.decrementAndGet();
            return cacheEntry.value;
        }
        return null;
    }

    public V put(long key, V val) {
        if (val == null) return null;
        CacheEntry<V> e = new CacheEntry<V>(key, val, accessCounter.incrementAndGet());
        CacheEntry<V> oldCacheEntry = map.put(key, e);
        int currentSize;
        if (oldCacheEntry == null) {
            currentSize = size.incrementAndGet();
        } else {
            currentSize = size.get();
        }

        putCounter.incrementAndGet();

        // Check if we need to clear out old entries from the cache.
        // isCleaning variable is checked instead of markAndSweepLock.isLocked()
        // for performance because every put invokation will check until
        // the size is back to an acceptable level.
        //
        // There is a race between the check and the call to markAndSweep, but
        // it's unimportant because markAndSweep actually aquires the lock or returns if it can't.
        //
        // Thread safety note: isCleaning read is piggybacked (comes after) other volatile reads
        // in this method.
        if (currentSize > upperWaterMark && !isCleaning) {
            markAndSweep();
        }
        return oldCacheEntry == null ? null : oldCacheEntry.value;
    }

    /**
     * Removes items from the cache to bring the size down
     * to an acceptable value ('acceptableWaterMark').
     * <p/>
     * It is done in two stages. In the first stage, least recently used items are evicted.
     * If, after the first stage, the cache size is still greater than 'acceptableSize'
     * config parameter, the second stage takes over.
     * <p/>
     * The second stage is more intensive and tries to bring down the cache size
     * to the 'lowerWaterMark' config parameter.
     */
    private void markAndSweep() {
        // if we want to keep at least 1000 entries, then timestamps of
        // current through current-1000 are guaranteed not to be the oldest (but that does
        // not mean there are 1000 entries in that group... it's acutally anywhere between
        // 1 and 1000).
        // Also, if we want to remove 500 entries, then
        // oldestEntry through oldestEntry+500 are guaranteed to be
        // removed (however many there are there).

        if (!markAndSweepLock.tryLock()) return;
        try {
            long oldestEntry = this.oldestEntry;
            isCleaning = true;
            this.oldestEntry = oldestEntry;     // volatile write to make isCleaning visible

            long timeCurrent = accessCounter.get();
            int sz = size.get();

            int numRemoved = 0;
            int numKept = 0;
            long newestEntry = timeCurrent;
            long newNewestEntry = -1;
            long newOldestEntry = Long.MAX_VALUE;

            int wantToKeep = lowerWaterMark;
            int wantToRemove = sz - lowerWaterMark;

            CacheEntry[] eset = new CacheEntry[sz];
            int eSize = 0;

            // System.out.println("newestEntry="+newestEntry + " oldestEntry="+oldestEntry);
            // System.out.println("items removed:" + numRemoved + " numKept=" + numKept + " esetSz="+ eSize + " sz-numRemoved=" + (sz-numRemoved));

            for (Iterator<CacheEntry<V>> iter = map.valuesIterator(); iter.hasNext();) {
                CacheEntry<V> ce  = iter.next();
                // set lastAccessedCopy to avoid more volatile reads
                ce.lastAccessedCopy = ce.lastAccessed;
                long thisEntry = ce.lastAccessedCopy;

                // since the wantToKeep group is likely to be bigger than wantToRemove, check it first
                if (thisEntry > newestEntry - wantToKeep) {
                    // this entry is guaranteed not to be in the bottom
                    // group, so do nothing.
                    numKept++;
                    newOldestEntry = Math.min(thisEntry, newOldestEntry);
                } else if (thisEntry < oldestEntry + wantToRemove) { // entry in bottom group?
                    // this entry is guaranteed to be in the bottom group
                    // so immediately remove it from the map.
                    evictEntry(ce.key);
                    numRemoved++;
                } else {
                    // This entry *could* be in the bottom group.
                    // Collect these entries to avoid another full pass... this is wasted
                    // effort if enough entries are normally removed in this first pass.
                    // An alternate impl could make a full second pass.
                    if (eSize < eset.length-1) {
                        eset[eSize++] = ce;
                        newNewestEntry = Math.max(thisEntry, newNewestEntry);
                        newOldestEntry = Math.min(thisEntry, newOldestEntry);
                    }
                }
            }

            // System.out.println("items removed:" + numRemoved + " numKept=" + numKept + " esetSz="+ eSize + " sz-numRemoved=" + (sz-numRemoved));

            int numPasses=1; // maximum number of linear passes over the data

            // if we didn't remove enough entries, then make more passes
            // over the values we collected, with updated min and max values.
            while (sz - numRemoved > acceptableWaterMark && --numPasses>=0) {

                oldestEntry = newOldestEntry == Long.MAX_VALUE ? oldestEntry : newOldestEntry;
                newOldestEntry = Long.MAX_VALUE;
                newestEntry = newNewestEntry;
                newNewestEntry = -1;
                wantToKeep = lowerWaterMark - numKept;
                wantToRemove = sz - lowerWaterMark - numRemoved;

                // iterate backward to make it easy to remove items.
                for (int i=eSize-1; i>=0; i--) {
                    CacheEntry ce = eset[i];
                    long thisEntry = ce.lastAccessedCopy;

                    if (thisEntry > newestEntry - wantToKeep) {
                        // this entry is guaranteed not to be in the bottom
                        // group, so do nothing but remove it from the eset.
                        numKept++;
                        // remove the entry by moving the last element to it's position
                        eset[i] = eset[eSize-1];
                        eSize--;

                        newOldestEntry = Math.min(thisEntry, newOldestEntry);

                    } else if (thisEntry < oldestEntry + wantToRemove) { // entry in bottom group?

                        // this entry is guaranteed to be in the bottom group
                        // so immediately remove it from the map.
                        evictEntry(ce.key);
                        numRemoved++;

                        // remove the entry by moving the last element to it's position
                        eset[i] = eset[eSize-1];
                        eSize--;
                    } else {
                        // This entry *could* be in the bottom group, so keep it in the eset,
                        // and update the stats.
                        newNewestEntry = Math.max(thisEntry, newNewestEntry);
                        newOldestEntry = Math.min(thisEntry, newOldestEntry);
                    }
                }
                // System.out.println("items removed:" + numRemoved + " numKept=" + numKept + " esetSz="+ eSize + " sz-numRemoved=" + (sz-numRemoved));
            }



            // if we still didn't remove enough entries, then make another pass while
            // inserting into a priority queue
            if (sz - numRemoved > acceptableWaterMark) {

                oldestEntry = newOldestEntry == Long.MAX_VALUE ? oldestEntry : newOldestEntry;
                newOldestEntry = Long.MAX_VALUE;
                newestEntry = newNewestEntry;
                newNewestEntry = -1;
                wantToKeep = lowerWaterMark - numKept;
                wantToRemove = sz - lowerWaterMark - numRemoved;

                PQueue<V> queue = new PQueue<V>(wantToRemove);

                for (int i=eSize-1; i>=0; i--) {
                    CacheEntry<V> ce = eset[i];
                    long thisEntry = ce.lastAccessedCopy;

                    if (thisEntry > newestEntry - wantToKeep) {
                        // this entry is guaranteed not to be in the bottom
                        // group, so do nothing but remove it from the eset.
                        numKept++;
                        // removal not necessary on last pass.
                        // eset[i] = eset[eSize-1];
                        // eSize--;

                        newOldestEntry = Math.min(thisEntry, newOldestEntry);

                    } else if (thisEntry < oldestEntry + wantToRemove) {  // entry in bottom group?
                        // this entry is guaranteed to be in the bottom group
                        // so immediately remove it.
                        evictEntry(ce.key);
                        numRemoved++;

                        // removal not necessary on last pass.
                        // eset[i] = eset[eSize-1];
                        // eSize--;
                    } else {
                        // This entry *could* be in the bottom group.
                        // add it to the priority queue

                        // everything in the priority queue will be removed, so keep track of
                        // the lowest value that ever comes back out of the queue.

                        // first reduce the size of the priority queue to account for
                        // the number of items we have already removed while executing
                        // this loop so far.
                        queue.myMaxSize = sz - lowerWaterMark - numRemoved;
                        while (queue.size() > queue.myMaxSize && queue.size() > 0) {
                            CacheEntry otherEntry = queue.pop();
                            newOldestEntry = Math.min(otherEntry.lastAccessedCopy, newOldestEntry);
                        }
                        if (queue.myMaxSize <= 0) break;

                        Object o = queue.myInsertWithOverflow(ce);
                        if (o != null) {
                            newOldestEntry = Math.min(((CacheEntry)o).lastAccessedCopy, newOldestEntry);
                        }
                    }
                }

                // Now delete everything in the priority queue.
                // avoid using pop() since order doesn't matter anymore
                for (CacheEntry<V> ce : queue.getValues()) {
                    if (ce==null) continue;
                    evictEntry(ce.key);
                    numRemoved++;
                }

                // System.out.println("items removed:" + numRemoved + " numKept=" + numKept + " initialQueueSize="+ wantToRemove + " finalQueueSize=" + queue.size() + " sz-numRemoved=" + (sz-numRemoved));
            }

            oldestEntry = newOldestEntry == Long.MAX_VALUE ? oldestEntry : newOldestEntry;
            this.oldestEntry = oldestEntry;
        } finally {
            isCleaning = false;  // set before markAndSweep.unlock() for visibility
            markAndSweepLock.unlock();
        }
    }

    private static class PQueue<V> extends PriorityQueue<CacheEntry<V>> {
        int myMaxSize;
        final Object[] heap;

        PQueue(int maxSz) {
            super(maxSz);
            heap = getHeapArray();
            myMaxSize = maxSz;
        }


        Iterable<CacheEntry<V>> getValues() {
            return (Iterable) Collections.unmodifiableCollection(Arrays.asList(heap));
        }

        @Override
        protected boolean lessThan(CacheEntry a, CacheEntry b) {
            // reverse the parameter order so that the queue keeps the oldest items
            return b.lastAccessedCopy < a.lastAccessedCopy;
        }

        // necessary because maxSize is private in base class
        public CacheEntry<V> myInsertWithOverflow(CacheEntry<V> element) {
            if (size() < myMaxSize) {
                add(element);
                return null;
            } else if (size() > 0 && !lessThan(element, (CacheEntry<V>) heap[1])) {
                CacheEntry<V> ret = (CacheEntry<V>) heap[1];
                heap[1] = element;
                updateTop();
                return ret;
            } else {
                return element;
            }
        }
    }

    /** A PriorityQueue maintains a partial ordering of its elements such that the
     * least element can always be found in constant time.  Put()'s and pop()'s
     * require log(size) time.
     *
     * <p><b>NOTE</b>: This class will pre-allocate a full array of
     * length <code>maxSize+1</code> if instantiated via the
     * {@link #PriorityQueue(int,boolean)} constructor with
     * <code>prepopulate</code> set to <code>true</code>.
     *
     * @lucene.internal
     */
    private static abstract class PriorityQueue<T> {
        private int size;
        private final int maxSize;
        private final T[] heap;

        public PriorityQueue(int maxSize) {
            this(maxSize, true);
        }

        public PriorityQueue(int maxSize, boolean prepopulate) {
            size = 0;
            int heapSize;
            if (0 == maxSize)
                // We allocate 1 extra to avoid if statement in top()
                heapSize = 2;
            else {
                if (maxSize == Integer.MAX_VALUE) {
                    // Don't wrap heapSize to -1, in this case, which
                    // causes a confusing NegativeArraySizeException.
                    // Note that very likely this will simply then hit
                    // an OOME, but at least that's more indicative to
                    // caller that this values is too big.  We don't +1
                    // in this case, but it's very unlikely in practice
                    // one will actually insert this many objects into
                    // the PQ:
                    heapSize = Integer.MAX_VALUE;
                } else {
                    // NOTE: we add +1 because all access to heap is
                    // 1-based not 0-based.  heap[0] is unused.
                    heapSize = maxSize + 1;
                }
            }
            heap = (T[]) new Object[heapSize]; // T is unbounded type, so this unchecked cast works always
            this.maxSize = maxSize;

            if (prepopulate) {
                // If sentinel objects are supported, populate the queue with them
                T sentinel = getSentinelObject();
                if (sentinel != null) {
                    heap[1] = sentinel;
                    for (int i = 2; i < heap.length; i++) {
                        heap[i] = getSentinelObject();
                    }
                    size = maxSize;
                }
            }
        }

        /** Determines the ordering of objects in this priority queue.  Subclasses
         *  must define this one method.
         *  @return <code>true</code> iff parameter <tt>a</tt> is less than parameter <tt>b</tt>.
         */
        protected abstract boolean lessThan(T a, T b);

        /**
         * This method can be overridden by extending classes to return a sentinel
         * object which will be used by the {@link PriorityQueue#PriorityQueue(int,boolean)}
         * constructor to fill the queue, so that the code which uses that queue can always
         * assume it's full and only change the top without attempting to insert any new
         * object.<br>
         *
         * Those sentinel values should always compare worse than any non-sentinel
         * value (i.e., {@link #lessThan} should always favor the
         * non-sentinel values).<br>
         *
         * By default, this method returns false, which means the queue will not be
         * filled with sentinel values. Otherwise, the value returned will be used to
         * pre-populate the queue. Adds sentinel values to the queue.<br>
         *
         * If this method is extended to return a non-null value, then the following
         * usage pattern is recommended:
         *
         * <pre class="prettyprint">
         * // extends getSentinelObject() to return a non-null value.
         * PriorityQueue&lt;MyObject&gt; pq = new MyQueue&lt;MyObject&gt;(numHits);
         * // save the 'top' element, which is guaranteed to not be null.
         * MyObject pqTop = pq.top();
         * &lt;...&gt;
         * // now in order to add a new element, which is 'better' than top (after
         * // you've verified it is better), it is as simple as:
         * pqTop.change().
         * pqTop = pq.updateTop();
         * </pre>
         *
         * <b>NOTE:</b> if this method returns a non-null value, it will be called by
         * the {@link PriorityQueue#PriorityQueue(int,boolean)} constructor
         * {@link #size()} times, relying on a new object to be returned and will not
         * check if it's null again. Therefore you should ensure any call to this
         * method creates a new instance and behaves consistently, e.g., it cannot
         * return null if it previously returned non-null.
         *
         * @return the sentinel object to use to pre-populate the queue, or null if
         *         sentinel objects are not supported.
         */
        protected T getSentinelObject() {
            return null;
        }

        /**
         * Adds an Object to a PriorityQueue in log(size) time. If one tries to add
         * more objects than maxSize from initialize an
         * {@link ArrayIndexOutOfBoundsException} is thrown.
         *
         * @return the new 'top' element in the queue.
         */
        public final T add(T element) {
            size++;
            heap[size] = element;
            upHeap();
            return heap[1];
        }

        /**
         * Adds an Object to a PriorityQueue in log(size) time.
         * It returns the object (if any) that was
         * dropped off the heap because it was full. This can be
         * the given parameter (in case it is smaller than the
         * full heap's minimum, and couldn't be added), or another
         * object that was previously the smallest value in the
         * heap and now has been replaced by a larger one, or null
         * if the queue wasn't yet full with maxSize elements.
         */
        public T insertWithOverflow(T element) {
            if (size < maxSize) {
                add(element);
                return null;
            } else if (size > 0 && !lessThan(element, heap[1])) {
                T ret = heap[1];
                heap[1] = element;
                updateTop();
                return ret;
            } else {
                return element;
            }
        }

        /** Returns the least element of the PriorityQueue in constant time. */
        public final T top() {
            // We don't need to check size here: if maxSize is 0,
            // then heap is length 2 array with both entries null.
            // If size is 0 then heap[1] is already null.
            return heap[1];
        }

        /** Removes and returns the least element of the PriorityQueue in log(size)
         time. */
        public final T pop() {
            if (size > 0) {
                T result = heap[1];       // save first value
                heap[1] = heap[size];     // move last to first
                heap[size] = null;        // permit GC of objects
                size--;
                downHeap();               // adjust heap
                return result;
            } else
                return null;
        }

        /**
         * Should be called when the Object at top changes values. Still log(n) worst
         * case, but it's at least twice as fast to
         *
         * <pre class="prettyprint">
         * pq.top().change();
         * pq.updateTop();
         * </pre>
         *
         * instead of
         *
         * <pre class="prettyprint">
         * o = pq.pop();
         * o.change();
         * pq.push(o);
         * </pre>
         *
         * @return the new 'top' element.
         */
        public final T updateTop() {
            downHeap();
            return heap[1];
        }

        /** Returns the number of elements currently stored in the PriorityQueue. */
        public final int size() {
            return size;
        }

        /** Removes all entries from the PriorityQueue. */
        public final void clear() {
            for (int i = 0; i <= size; i++) {
                heap[i] = null;
            }
            size = 0;
        }

        private final void upHeap() {
            int i = size;
            T node = heap[i];          // save bottom node
            int j = i >>> 1;
            while (j > 0 && lessThan(node, heap[j])) {
                heap[i] = heap[j];       // shift parents down
                i = j;
                j = j >>> 1;
            }
            heap[i] = node;            // install saved node
        }

        private final void downHeap() {
            int i = 1;
            T node = heap[i];          // save top node
            int j = i << 1;            // find smaller child
            int k = j + 1;
            if (k <= size && lessThan(heap[k], heap[j])) {
                j = k;
            }
            while (j <= size && lessThan(heap[j], node)) {
                heap[i] = heap[j];       // shift up child
                i = j;
                j = i << 1;
                k = j + 1;
                if (k <= size && lessThan(heap[k], heap[j])) {
                    j = k;
                }
            }
            heap[i] = node;            // install saved node
        }

        /** This method returns the internal heap array as Object[].
         * @lucene.internal
         */
        protected final Object[] getHeapArray() {
            return heap;
        }
    }


    private void evictEntry(long key) {
        CacheEntry<V> o = map.remove(key);
        if (o == null) return;
        size.decrementAndGet();
        evictionCounter.incrementAndGet();
        evictedEntry(o.key,o.value);
    }


    public int size() {
        return size.get();
    }

    @Override
    public Iterator<V> valuesIterator() {
        final Iterator<CacheEntry<V>> iter = map.valuesIterator();
        return new Iterator<V>(){

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public V next() {
                return iter.next().value;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public LongMapIterator<V> longMapIterator() {
        final LongMapIterator<CacheEntry<V>> iter = map.longMapIterator();
        return new LongMapIterator<V>() {
            @Override
            public boolean moveToNext() {
                return iter.moveToNext();
            }

            @Override
            public long key() {
                return iter.key();
            }

            @Override
            public V value() {
                return iter.value().value;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public void clear() {
        map.clear();
    }

    public LongMap<CacheEntry<V>> getMap() {
        return map;
    }

    private static final class CacheEntry<V> implements Comparable<CacheEntry<V>> {
        final long key;
        final V value;
        volatile long lastAccessed = 0;
        long lastAccessedCopy = 0;


        public CacheEntry(long key, V value, long lastAccessed) {
            this.key = key;
            this.value = value;
            this.lastAccessed = lastAccessed;
        }

        @Override
        public int compareTo(CacheEntry<V> that) {
            if (this.lastAccessedCopy == that.lastAccessedCopy) return 0;
            return this.lastAccessedCopy < that.lastAccessedCopy ? 1 : -1;
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return value.equals(obj);
        }

        @Override
        public String toString() {
            return "key: " + key + " value: " + value + " lastAccessed:" + lastAccessed;
        }
    }






    /** override this method to get notified about evicted entries*/
    protected void evictedEntry(long key, V value){

    }
}
