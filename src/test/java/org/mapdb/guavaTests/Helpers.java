package org.mapdb.guavaTests;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class Helpers {
    public static <K, V> Map.Entry<K, V> mapEntry(final K key, final V value) {
        Map<K,V> m = new HashMap<K,V>();
        m.put(key,value);
        m = Collections.unmodifiableMap(m);
        return m.entrySet().iterator().next();
    }
}


