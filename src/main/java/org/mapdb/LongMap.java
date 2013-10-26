/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.mapdb;

import java.util.Iterator;

/**
 * Same as 'java.util.Map' but uses primitive 'long' keys to minimise boxing (and GC) overhead.
 *
 * @author Jan Kotek
 */
public abstract class LongMap<V> {

    /**
     * Removes all mappings from this hash map, leaving it empty.
     *
     * @see #isEmpty
     * @see #size
     */
    public abstract void clear();

    /**
     * Returns the value of the mapping with the specified key.
     *
     * @param key the key.
     * @return the value of the mapping with the specified key, or {@code null}
     *         if no mapping for the specified key is found.
     */
    public abstract V get(long key);

    /**
     * Returns whether this map is empty.
     *
     * @return {@code true} if this map has no elements, {@code false}
     *         otherwise.
     * @see #size()
     */
    public abstract boolean isEmpty();

    /**
     * Maps the specified key to the specified value.
     *
     * @param key   the key.
     * @param value the value.
     * @return the value of any previous mapping with the specified key or
     *         {@code null} if there was no such mapping.
     */
    public abstract V put(long key, V value);


    /**
     * Removes the mapping from this map
     *
     * @param key to remove
     *  @return value contained under this key, or null if value did not exist
     */
    public abstract V remove(long key);

    /**
     * Returns the number of elements in this map.
     *
     * @return the number of elements in this map.
     */
    public abstract int size();


    /**
     * @return iterator over values in map
     */
    public abstract Iterator<V> valuesIterator();

    public abstract LongMapIterator<V> longMapIterator();


    /** Iterates over LongMap key and values without boxing long keys */
    public interface LongMapIterator<V>{
        boolean moveToNext();
        long key();
        V value();

        void remove();
    }

    @Override
	public String toString(){
        final StringBuilder b = new StringBuilder();
        b.append(getClass().getSimpleName());
        b.append('[');
        boolean first = true;
        LongMapIterator<V> iter = longMapIterator();
        while(iter.moveToNext()){
            if(first){
                first = false;
            }else{
                b.append(", ");
            }
            b.append(iter.key());
            b.append(" => ");
            b.append(iter.value());
        }
        b.append(']');
        return b.toString();
    }
}
