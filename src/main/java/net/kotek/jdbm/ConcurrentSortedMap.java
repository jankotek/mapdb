package net.kotek.jdbm;

import java.util.concurrent.ConcurrentMap;

/**
 * Interface which mixes ConcurrentMap and SortedMap together.
 * <p/>
 * There is 'ConcurrentNavigableMap' interface, but JDBM does not use it.
 * It was introduced in Java6 but we try to stay at Java5 API.
 * Also NavigableMap requires bidirectional iterators which B-Linked-Tree
 * used in BTreeMap does not provide
 */
public interface ConcurrentSortedMap<K, V> extends ConcurrentMap<K,V>{
}
