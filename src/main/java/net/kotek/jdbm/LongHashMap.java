/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.kotek.jdbm;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Hash Map which uses primitive long as key.
 * Main advantage is new instanceof of Long does not have to be created for each lookup.
 * <p/>
 * This code comes from Android, which in turns comes from Apache Harmony.
 * This class was modified to use primitive longs and stripped down to consume less space.
 * <p/>
 * Author of JDBM modifications: Jan Kotek
 */
public class LongHashMap<V> extends LongMap<V> implements Serializable  {
    private static final long serialVersionUID = 362499999763181265L;

    private int elementCount;

    private Entry<V>[] elementData;

    private final float loadFactor;

    private int threshold;

    private int defaultSize = 16;

    private transient Entry<V> reuseAfterDelete = null;

    static final class Entry<V> implements  Serializable{
        private static final long serialVersionUID = 362445231113181265L;

        Entry<V> next;

        V value;

        long key;

        Entry(long theKey) {
            this.key = theKey;
            this.value = null;
        }


    }


    static class HashMapIterator<V> implements Iterator<V> {
        private int position = 0;


        boolean canRemove = false;

        Entry<V> entry;

        Entry<V> lastEntry;

        final LongHashMap<V> associatedMap;

        HashMapIterator(LongHashMap<V> hm) {
            associatedMap = hm;
        }

        public boolean hasNext() {
            if (entry != null) {
                return true;
            }

            Entry<V>[] elementData = associatedMap.elementData;
            int length = elementData.length;
            int newPosition = position;
            boolean result = false;

            while (newPosition < length) {
                if (elementData[newPosition] == null) {
                    newPosition++;
                } else {
                    result = true;
                    break;
                }
            }

            position = newPosition;
            return result;
        }

        public V next() {

            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            Entry<V> result;
            Entry<V> _entry = entry;
            if (_entry == null) {
                result = lastEntry = associatedMap.elementData[position++];
                entry = lastEntry.next;
            } else {
                if (lastEntry.next != _entry) {
                    lastEntry = lastEntry.next;
                }
                result = _entry;
                entry = _entry.next;
            }
            canRemove = true;
            return result.value;
        }

        public void remove() {
            if (!canRemove) {
                throw new IllegalStateException();
            }

            canRemove = false;

            if (lastEntry.next == entry) {
                while (associatedMap.elementData[--position] == null) {
                    // Do nothing
                }
                associatedMap.elementData[position] = associatedMap.elementData[position].next;
                entry = null;
            } else {
                lastEntry.next = entry;
            }
            if (lastEntry != null) {
                Entry<V> reuse = lastEntry;
                lastEntry = null;
                reuse.key = Long.MIN_VALUE;
                reuse.value = null;
                associatedMap.reuseAfterDelete = reuse;
            }

            associatedMap.elementCount--;
        }
    }


    @SuppressWarnings("unchecked")
    private Entry<V>[] newElementArray(int s) {
        return new Entry[s];
    }

    /**
     * Constructs a new empty {@code HashMap} instance.
     *
     * @since Android 1.0
     */
    public LongHashMap() {
        this(16);
    }

    /**
     * Constructs a new {@code HashMap} instance with the specified capacity.
     *
     * @param capacity the initial capacity of this hash map.
     * @throws IllegalArgumentException when the capacity is less than zero.
     * @since Android 1.0
     */
    public LongHashMap(int capacity) {
        defaultSize = capacity;
        if (capacity >= 0) {
            elementCount = 0;
            elementData = newElementArray(capacity == 0 ? 1 : capacity);
            loadFactor = 0.75f; // Default load factor of 0.75
            computeMaxSize();
        } else {
            throw new IllegalArgumentException();
        }
    }


    // BEGIN android-changed



    @Override
    @SuppressWarnings("unchecked")
    public void clear() {
        if (elementCount > 0) {            
            elementCount = 0;            
        }
        if(elementData.length>1024 && elementData.length>defaultSize)
            elementData = new Entry[defaultSize];
        else
            Arrays.fill(elementData, null);
        computeMaxSize();
    }
    // END android-changed


    private void computeMaxSize() {
        threshold = (int) (elementData.length * loadFactor);
    }




    @Override
    public V get(final long key) {

        final int hash = JdbmUtil.longHash(key);
        final int index = (hash & 0x7FFFFFFF) % elementData.length;

        //find non null entry
        Entry<V> m = elementData[index];
        while (m != null) {
            if (key == m.key)
                return m.value;
            m = m.next;
        }

        return null;

    }



    @Override
    public boolean isEmpty() {
        return elementCount == 0;
    }

    /**
     * @return iterator over keys
     */

//      public Iterator<K> keyIterator(){
//                 return new HashMapIterator<K, K, V>(
//                            new MapEntry.Type<K, K, V>() {
//                                public K get(Entry<K, V> entry) {
//                                    return entry.key;
//                                }
//                            }, HashMap.this);
//
//     }




    @Override
    public V put(final long key, final V value) {

        int hash = JdbmUtil.longHash(key);
        int index = (hash & 0x7FFFFFFF) % elementData.length;

        //find non null entry
        Entry<V> entry = elementData[index];
        while (entry != null && key != entry.key) {
            entry = entry.next;
        }

        if (entry == null) {
            if (++elementCount > threshold) {
                rehash();
                index = (hash & 0x7FFFFFFF) % elementData.length;
            }
            entry = createHashedEntry(key, index);
        }


        V result = entry.value;
        entry.value = value;
        return result;
    }


    Entry<V> createHashedEntry(final long key, final int index) {
        Entry<V> entry = reuseAfterDelete;
        if (entry == null) {
            entry = new Entry<V>(key);
        } else {
            reuseAfterDelete = null;
            entry.key = key;
            entry.value = null;
        }

        entry.next = elementData[index];
        elementData[index] = entry;
        return entry;
    }


    void rehash(final int capacity) {
        int length = (capacity == 0 ? 1 : capacity << 1);

        Entry<V>[] newData = newElementArray(length);
        for (Entry<V> anElementData : elementData) {
            Entry<V> entry = anElementData;
            while (entry != null) {
                int index = (JdbmUtil.longHash(entry.key) & 0x7FFFFFFF) % length;
                Entry<V> next = entry.next;
                entry.next = newData[index];
                newData[index] = entry;
                entry = next;
            }
        }
        elementData = newData;
        computeMaxSize();
    }

    void rehash() {
        rehash(elementData.length);
    }

    /**
     * Removes the mapping with the specified key from this map.
     *
     * @param key the key of the mapping to remove.
     * @return the value of the removed mapping or {@code null} if no mapping
     *         for the specified key was found.
     * @since Android 1.0
     */

    @Override
    public V remove(final long key) {
        Entry<V> entry = removeEntry(key);
        if (entry == null)
            return null;
        V ret = entry.value;
        entry.value = null;
        entry.key = Long.MIN_VALUE;
        reuseAfterDelete = entry;

        return ret;
    }

    Entry<V> removeEntry(final long key) {
        Entry<V> last = null;

        final int hash = JdbmUtil.longHash(key);
        final int index = (hash & 0x7FFFFFFF) % elementData.length;
        Entry<V> entry = elementData[index];

        while (true) {
            if (entry == null) {
                return null;
            }

            if (key == entry.key) {
                if (last == null) {
                    elementData[index] = entry.next;
                } else {
                    last.next = entry.next;
                }
                elementCount--;
                return entry;
            }

            last = entry;
            entry = entry.next;
        }
    }

    /**
     * Returns the number of elements in this map.
     *
     * @return the number of elements in this map.
     * @since Android 1.0
     */

    @Override
    public int size() {
        return elementCount;
    }

    /**
     * @return iterator over values in map
     */
    @Override
    public Iterator<V> valuesIterator() {
        return new HashMapIterator<V>(this);

    }


    static class LongMapIterator2<V> extends HashMapIterator<V> implements LongMapIterator<V>{

        LongMapIterator2(LongHashMap m) {
            super(m);
        }

        @Override public boolean moveToNext() {
            if(!hasNext())return false;
            next();
            return true;
        }

        @Override public long key() {
            return lastEntry.key;
        }

        @Override public V value() {
            return lastEntry.value;
        }
    }


    @Override
    public LongMapIterator<V> longMapIterator() {
        return new LongMapIterator2<V>(this);
    }


}



