package org.mapdb

import java.util.concurrent.ConcurrentMap
import java.util.concurrent.ConcurrentNavigableMap
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
     *
     * @param value value to be associated with the specified key
     *
     * @return true if a value was set.
     *
     * @throws NullPointerException  if key is null or value is null
     *
     * @throws IllegalStateException if the cache is [.isClosed]
     *
     * @throws ClassCastException    if the implementation is configured to perform
     *                                runtime-type-checking, and the key or value
     *                                types are incompatible with those that have been
     *                                configured with different serialziers
     * TODO link to JCache standard
     * TODO credits for javadoc
     */
    fun putIfAbsentBoolean(key: K?, value: V?): Boolean

    /**
     * Puts new value, but does not return old value. Might be faster since old value is not deserialized
     *
     * Old value will only be deserialized if modification listeners are installed, or values are inlined.
     */
    fun putOnly(key:K?, value:V?)

    fun isClosed(): Boolean

    fun forEachKey(procedure: (K)->Unit);

    fun forEachValue(procedure: (V)->Unit);

    override fun forEach(action: BiConsumer<in K, in V>);

    val keySerializer:Serializer<K>

    val valueSerializer:Serializer<V>

}


interface ConcurrentNavigableMapExtra<K, V> : ConcurrentNavigableMap<K, V>, MapExtra<K, V>, BTreeMapJava.ConcurrentNavigableMap2<K,V> {

    val hasValues:Boolean

    fun findHigher(key: K?, inclusive: Boolean): MutableMap.MutableEntry<K, V>?

    fun findLower(key: K?, inclusive: Boolean): MutableMap.MutableEntry<K, V>?

    fun findHigherKey(key: K?, inclusive: Boolean): K?

    fun findLowerKey(key: K?, inclusive: Boolean): K?

    fun keyIterator(): MutableIterator<K>

    fun keyIterator(lo: K?, loInclusive: Boolean, hi: K?, hiInclusive: Boolean): MutableIterator<K?>

    fun valueIterator(lo: K?, loInclusive: Boolean, hi: K?, hiInclusive: Boolean): MutableIterator<V?>

    fun entryIterator(lo: K?, loInclusive: Boolean, hi: K?, hiInclusive: Boolean): MutableIterator<MutableMap.MutableEntry<K, V?>>

    fun descendingKeyIterator(): MutableIterator<K>

    fun descendingKeyIterator(lo: K?, loInclusive: Boolean, hi: K?, hiInclusive: Boolean): MutableIterator<K>

    fun descendingValueIterator(): MutableIterator<V?>

    fun descendingValueIterator(lo: K?, loInclusive: Boolean, hi: K?, hiInclusive: Boolean): MutableIterator<V?>

    fun descendingEntryIterator(): MutableIterator<MutableMap.MutableEntry<K, V?>>

    fun descendingEntryIterator(lo: K?, loInclusive: Boolean, hi: K?, hiInclusive: Boolean): MutableIterator<MutableMap.MutableEntry<K, V?>>
}
