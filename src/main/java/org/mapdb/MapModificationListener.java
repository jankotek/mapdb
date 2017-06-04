package org.mapdb;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Callback interface for {@link DBConcurrentMap} modification notifications.
 */
public interface MapModificationListener<K,V> {

    void modify(@NotNull K key, @Nullable  V oldValue, @Nullable V newValue, boolean triggered);

}
