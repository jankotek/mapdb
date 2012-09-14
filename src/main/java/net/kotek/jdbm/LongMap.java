package net.kotek.jdbm;

import java.util.Iterator;

/**
 * @author Jan Kotek
 */
interface LongMap<V> {

    /**
     * Removes all mappings from this hash map, leaving it empty.
     *
     * @see #isEmpty
     * @see #size
     */
    void clear();

    /**
     * Returns the value of the mapping with the specified key.
     *
     * @param key the key.
     * @return the value of the mapping with the specified key, or {@code null}
     *         if no mapping for the specified key is found.
     */
    V get(long key);

    /**
     * Returns whether this map is empty.
     *
     * @return {@code true} if this map has no elements, {@code false}
     *         otherwise.
     * @see #size()
     */
    boolean isEmpty();

    /**
     * Maps the specified key to the specified value.
     *
     * @param key   the key.
     * @param value the value.
     * @return the value of any previous mapping with the specified key or
     *         {@code null} if there was no such mapping.
     */
    V put(long key, V value);


    /**
     * Removes the mapping from this map
     *
     * @param key to remove
     *  @return value contained under this key, or null if value did not exist
     */
    V remove(long key);

    /**
     * Returns the number of elements in this map.
     *
     * @return the number of elements in this map.
     */
    int size();


    /**
     * @return iterator over values in map
     */
    Iterator<V> valuesIterator();

    LongMapIterator<V> longMapIterator();


    public interface LongMapIterator<V>{
        boolean moveToNext();
        long key();
        V value();
    }
}
