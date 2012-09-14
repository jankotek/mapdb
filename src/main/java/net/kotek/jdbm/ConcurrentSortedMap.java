package net.kotek.jdbm;

import java.util.*;
import java.util.concurrent.*;

/**
 * Interface which mixes ConcurrentMap and SortedMap together.
 * <p/>
 * There is 'ConcurrentNavigableMap' but it requires bidirectional traverse,
 * which BTreeMap concurrent algorithm does not support.
 * ConcurrentNavigableMap was also introduced in Java6, and JDBM tries to stay at Java5 API.
 */
public interface ConcurrentSortedMap<K, V> extends ConcurrentMap<K,V>, SortedMap<K,V>{
    @Override
    ConcurrentSortedMap<K, V> subMap(K fromKey, K toKey);

    @Override
    ConcurrentSortedMap<K, V> headMap(K toKey);

    @Override
    ConcurrentSortedMap<K, V> tailMap(K fromKey);

    @Override
    SortedSet<K> keySet();

    @Override
    SortedSet<Entry<K, V>> entrySet();
}
