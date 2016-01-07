package org.mapdb;

/**
 * Callback interface for {@link MapExtra} modification notifications.
 */
public interface MapModificationListener<K,V> {

    void modify(K key, V oldValue, V newValue, boolean triggered);

}
