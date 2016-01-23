package org.mapdb

import org.eclipse.collections.api.block.procedure.Procedure
import java.util.EventListener
import java.util.concurrent.ConcurrentMap
import java.util.function.BiConsumer

/**
 * Extra methods for Map interface
 */
interface MapExtra<K, V> : ConcurrentMap<K, V> {



    /** map size as long number  */
    fun sizeLong(): Long


    /**
     * Atomically associates the specified key with the given value if it is
     * not already associated with a value.
     *
     *
     * This is equivalent to:
     * `
     * if (!cache.containsKey(key)) {}
     * cache.put(key, value);
     * return true;
     * } else {
     * return false;
     * }
    ` *
     * except that the action is performed atomically.

     * @param key   key with which the specified value is to be associated
     * *
     * @param value value to be associated with the specified key
     * *
     * @return true if a value was set.
     * *
     * @throws NullPointerException  if key is null or value is null
     * *
     * @throws IllegalStateException if the cache is [.isClosed]
     * *
     * @throws ClassCastException    if the implementation is configured to perform
     * *                               runtime-type-checking, and the key or value
     * *                               types are incompatible with those that have been
     * *                               configured with different serialziers
     * *  TODO link to JCache standar
     * *  TODO credits for javadoc
     */
    fun putIfAbsentBoolean(key: K?, value: V?): Boolean


    fun isClosed(): Boolean

    fun forEachKey(procedure: (K?)->Unit);

    fun forEachValue(procedure: (V?)->Unit);

    override fun forEach(action: BiConsumer<in K?, in V?>?);



}
