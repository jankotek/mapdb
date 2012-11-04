
/*
 * NOTE: some code (and javadoc) used in this class
 * comes from Apache Harmony with following copyright:
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package org.mapdb;

import java.util.AbstractMap;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Various abstract junk for BTreeMap
 */
public abstract class BTreeMapAbstract<K,V>
    extends AbstractMap<K,V>
    implements ConcurrentNavigableMap<K,V> {


    @Override
    public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
        return subMap(null, true, toKey,  inclusive);
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
        return subMap(fromKey, inclusive, null, false);
    }

    @Override
    public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
        return subMap(fromKey, true, toKey, false);
    }

    @Override
    public ConcurrentNavigableMap<K, V> headMap(K toKey) {
        return subMap(null, true, toKey, false);
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
        return subMap(fromKey, true, null, false);
    }

    @Override
    public Entry<K, V> lowerEntry(K key) {
        return notYetImplemented();
    }

    @Override
    public K lowerKey(K key) {
        return notYetImplemented();
    }

    @Override
    public Entry<K, V> floorEntry(K key) {
        return notYetImplemented();
    }

    @Override
    public K floorKey(K key) {
        return notYetImplemented();
    }

    @Override
    public Entry<K, V> ceilingEntry(K key) {
        return notYetImplemented();
    }

    @Override
    public K ceilingKey(K key) {
        return notYetImplemented();
    }

    @Override
    public Entry<K, V> higherEntry(K key) {
        return notYetImplemented();
    }

    @Override
    public K higherKey(K key) {
        return notYetImplemented();
    }

    @Override
    public Entry<K, V> firstEntry() {
        return notYetImplemented();
    }

    @Override
    public Entry<K, V> lastEntry() {
        return notYetImplemented();
    }

    @Override
    public Entry<K, V> pollFirstEntry() {
        return notYetImplemented();
    }

    @Override
    public Entry<K, V> pollLastEntry() {
        return notYetImplemented();
    }

    @Override
    public ConcurrentNavigableMap<K, V> descendingMap() {
        return notYetImplemented();
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        return notYetImplemented();
    }

    protected <E> E notYetImplemented() {
        throw new InternalError("not yet implemented");
    }

    abstract public NavigableSet<K> keySet();

}
