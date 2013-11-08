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

import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * Binding is simple yet powerful way to keep secondary collection synchronized with primary collection.
 * Primary collection provides notification on updates and secondary collection is modified accordingly.
 * This way MapDB provides secondary indexes, values and keys. It also supports less usual scenarious such
 * as histograms, inverse lookup index (on maps), group counters and so on.
 *
 * There are two things to keep on mind when using binding:
 *
 *  * Binding is not persistent, so it needs to be restored every time store is reopened.
 *    If you modify primary collection before binding is restored, secondary collection does not get updated and becomes
 *    inconsistent.
 *
 *  * If secondary collection is empty, binding will recreate its content based on primary collection.
 *    If there is even single item on secondary collection, binding assumes it is consistent and leaves it as its.
 *
 *  Any thread-safe collection can be used as secondary (not just collections provided by MapDB).
 *  This gives great flexibility for modeling
 *  and scaling your data. For example primary data can be stored in durable DB with transactions and large secondary
 *  indexes may be stored in other faster non-durable DB. Or primary collection may be stored on disk and smaller
 *  secondary index (such as category counters) can be stored in memory for faster lookups. Also you may use
 *  ordinary `java.util.*` collections (if they are thread safe) to get additional speed.
 *
 *  There are many [code examples](https://github.com/jankotek/MapDB/tree/master/src/test/java/examples)
 *  how Collection Binding can be used.
 *
 * @author Jan Kotek
 */
public final class Bind {

    private Bind(){}



    /**
     * Listener called when `Map` is modified.
     * @param <K> key type  in map
     * @param <V> value type in map
     */
    public interface MapListener<K,V>{
        /**
         * Callback method called after `Map` was modified.
         * It is called on insert, update or delete.
         *
         * MapDB collections do not support null keys or values.
         * Null parameter may be than used to indicate operation:
         *
         *
         *
         * @param key key in map
         * @param oldVal old value in map (if any, null on inserts)
         * @param newVal new value in map (if any, null on deletes)
         */
        void update(K key, V oldVal, V newVal);
    }

    /**
     * Primary Maps must provide notifications when it is modified.
     * So Primary Maps must implement this interface to allow registering callback listeners.
     *
     * @param <K> key type  in map
     * @param <V> value type in map
     */
    public interface MapWithModificationListener<K,V> extends Map<K,V> {
        /**
         * Add new modification listener notified when Map has been updated
         * @param listener callback interface notified when map changes
         */
        public void addModificationListener(MapListener<K,V> listener);

        /**
         * Remove registered notification listener
         *
         * @param listener  callback interface notified when map changes
         */
        public void removeModificationListener(MapListener<K,V> listener);


        /**
         *
         * @return size of map, but in  64bit long which does not overflow at 2e9 items.
         */
        public long sizeLong();
    }

    /**
     * Binds {@link Atomic.Long} to Primary Map so the Atomic.Long contains size of Map.
     * `Atomic.Long` is incremented on each insert and decremented on each entry removal.
     * MapDB collections usually do not keep their size, but require complete traversal to count items.
     *
     * If `Atomic.Long` has zero value, it will be updated with value from `map.size()` and than
     * bind to map.
     *
     * This binding is not persistent. You need to restore it every time store is reopened.
     *
     * NOTE: {@link BTreeMap} and {@link HTreeMap} already supports this directly as optional parameter named `counter`.
     * In that case all calls to `Map.size()` are forwarded to underlying counter. Check parameters at
     * {@link DB#createHashMap(String)} and
     * {@link DB#createTreeMap(String)}
     *
     *
     * @param map primary map whose size needs to be tracked
     * @param sizeCounter number updated when Map Entry is added or removed.
     */
    public static void size(MapWithModificationListener map, final Atomic.Long sizeCounter){
        //set initial value first if necessary
        if(sizeCounter.get() == 0)
            sizeCounter.set(map.sizeLong());

        map.addModificationListener(new MapListener() {
            @Override
            public void update(Object key, Object oldVal, Object newVal) {
                if(oldVal == null && newVal!=null){
                    sizeCounter.incrementAndGet();
                }else if(oldVal!=null && newVal == null){
                    sizeCounter.decrementAndGet();
                }

                //update does not change collection size
            }
        });
    }

    /**
     * Binds Secondary Map so that it contains Key from Primary Map and custom Value.
     * Secondary Value is updated every time Primary Map is modified.
     *
     * If Secondary Map is empty its content will be recreated from Primary Map.
     * This binding is not persistent. You need to restore it every time store is reopened.
     *
     * @param map Primary Map
     * @param secondary Secondary Map with custom
     * @param fun function which calculates secondary value from primary key and value
     * @param <K> key type in primary and Secondary Map
     * @param <V> value type in Primary Map
     * @param <V2> value type in Secondary Map.
     */
    public static <K,V, V2> void secondaryValue(MapWithModificationListener<K, V> map,
                                              final Map<K, V2> secondary,
                                              final Fun.Function2<V2, K, V> fun){
        //fill if empty
        if(secondary.isEmpty()){
            for(Map.Entry<K,V> e:map.entrySet())
                secondary.put(e.getKey(), fun.run(e.getKey(),e.getValue()));
        }
        //hook listener
        map.addModificationListener(new MapListener<K, V>() {
            @Override
            public void update(K key, V oldVal, V newVal) {
                if(newVal == null){
                    //removal
                    secondary.remove(key);
                }else{
                    secondary.put(key, fun.run(key,newVal));
                }
            }
        });
    }

    /**
     * Binds Secondary Map so that it contains Key from Primary Map and custom Value.
     * Secondary Value is updated every time Primary Map is modified.
     *
     * If Secondary Map is empty its content will be recreated from Primary Map.
     * This binding is not persistent. You need to restore it every time store is reopened.
     *
     * @param map Primary Map
     * @param secondary Secondary Map with custom
     * @param fun function which calculates secondary values from primary key and value
     * @param <K> key type in primary and Secondary Map
     * @param <V> value type in Primary Map
     * @param <V2> value type in Secondary Map.
     */
    public static <K,V, V2> void secondaryValues(MapWithModificationListener<K, V> map,
                                                final Set<Fun.Tuple2<K, V2>> secondary,
                                                final Fun.Function2<V2[], K, V> fun){
        //fill if empty
        if(secondary.isEmpty()){
            for(Map.Entry<K,V> e:map.entrySet()){
                V2[] v = fun.run(e.getKey(),e.getValue());
                if(v!=null)
                    for(V2 v2:v)
                        secondary.add(Fun.t2(e.getKey(), v2));
            }
        }
        //hook listener
        map.addModificationListener(new MapListener<K, V>() {
            @Override
            public void update(K key, V oldVal, V newVal) {
                if(newVal == null){
                    //removal
                    V2[] v = fun.run(key,oldVal);
                    if(v != null)
                        for(V2 v2 :v)
                            secondary.remove(Fun.t2(key,v2));
                }else if(oldVal==null){
                    //insert
                    V2[] v = fun.run(key,newVal);
                    if(v != null)
                        for(V2 v2 :v)
                            secondary.add(Fun.t2(key,v2));
                }else{
                    //update, must remove old key and insert new
                    V2[] oldv = fun.run(key, oldVal);
                    V2[] newv = fun.run(key, newVal);
                    if(oldv==null){
                        //insert new
                        if(newv!=null)
                            for(V2 v :newv)
                                secondary.add(Fun.t2(key,v));
                        return;
                    }
                    if(newv==null){
                        //remove old
                        for(V2 v :oldv)
                            secondary.remove(Fun.t2(key,v));
                        return;
                    }

                    Set<V2> hashes = new HashSet<V2>();
                    for(V2 v:oldv)
                        hashes.add(v);

                    //add new non existing items
                    for(V2 v:newv){
                        if(!hashes.contains(v)){
                            secondary.add(Fun.t2(key,v));
                        }
                    }
                    //remove items which are in old, but not in new
                    for(V2 v:newv){
                        hashes.remove(v);
                    }
                    for(V2 v:hashes){
                        secondary.remove(Fun.t2(key,v));
                    }
                }
            }
        });
    }


    /**
     * Binds Secondary Set so it contains Secondary Key (Index). Usefull if you need
     * to lookup Keys from Primary Map by custom criteria. Other use is for reverse lookup
     *
     * To lookup keys in Secondary Set use {@link Bind#findSecondaryKeys(java.util.NavigableSet, Object)}
     *
     *
     * If Secondary Set is empty its content will be recreated from Primary Map.
     * This binding is not persistent. You need to restore it every time store is reopened.
     *
     * @param map primary map
     * @param secondary secondary set
     * @param fun function which calculates Secondary Key from Primary Key and Value
     * @param <K> Key in Primary Map
     * @param <V> Value in Primary Map
     * @param <K2> Secondary
     */
    public static <K,V, K2> void secondaryKey(MapWithModificationListener<K, V> map,
                                                final Set<Fun.Tuple2<K2, K>> secondary,
                                                final Fun.Function2<K2, K, V> fun){
        //fill if empty
        if(secondary.isEmpty()){
            for(Map.Entry<K,V> e:map.entrySet()){
                secondary.add(Fun.t2(fun.run(e.getKey(),e.getValue()), e.getKey()));
            }
        }
        //hook listener
        map.addModificationListener(new MapListener<K, V>() {
            @Override
            public void update(K key, V oldVal, V newVal) {
                if(newVal == null){
                    //removal
                    secondary.remove(Fun.t2(fun.run(key, oldVal), key));
                }else if(oldVal==null){
                    //insert
                    secondary.add(Fun.t2(fun.run(key,newVal), key));
                }else{
                    //update, must remove old key and insert new
                    K2 oldKey = fun.run(key, oldVal);
                    K2 newKey = fun.run(key, newVal);
                    if(oldKey == newKey || oldKey.equals(newKey)) return;
                    secondary.remove(Fun.t2(oldKey, key));
                    secondary.add(Fun.t2(newKey,key));
                }
            }
        });
    }

    /**
     * Binds Secondary Set so it contains Secondary Key (Index). Useful if you need
     * to lookup Keys from Primary Map by custom criteria. Other use is for reverse lookup
     *
     * To lookup keys in Secondary Set use {@link Bind#findSecondaryKeys(java.util.NavigableSet, Object)}
     *
     *
     * If Secondary Set is empty its content will be recreated from Primary Map.
     * This binding is not persistent. You need to restore it every time store is reopened.
     *
     * @param map primary map
     * @param secondary secondary set
     * @param fun function which calculates Secondary Keys from Primary Key and Value
     * @param <K> Key in Primary Map
     * @param <V> Value in Primary Map
     * @param <K2> Secondary
     */
    public static <K,V, K2> void secondaryKeys(MapWithModificationListener<K, V> map,
                                              final Set<Fun.Tuple2<K2, K>> secondary,
                                              final Fun.Function2<K2[], K, V> fun){
        //fill if empty
        if(secondary.isEmpty()){
            for(Map.Entry<K,V> e:map.entrySet()){
                K2[] k2 = fun.run(e.getKey(), e.getValue());
                if(k2 != null)
                    for(K2 k22 :k2)
                        secondary.add(Fun.t2(k22, e.getKey()));
            }
        }
        //hook listener
        map.addModificationListener(new MapListener<K, V>() {
            @Override
            public void update(K key, V oldVal, V newVal) {
                if(newVal == null){
                    //removal
                    K2[] k2 = fun.run(key,oldVal);
                    if(k2 != null)
                        for(K2 k22 :k2)
                            secondary.remove(Fun.t2(k22, key));
                }else if(oldVal==null){
                    //insert
                    K2[] k2 = fun.run(key,newVal);
                    if(k2 != null)
                        for(K2 k22 :k2)
                            secondary.add(Fun.t2(k22, key));
                }else{
                    //update, must remove old key and insert new
                    K2[] oldk = fun.run(key, oldVal);
                    K2[] newk = fun.run(key, newVal);
                    if(oldk==null){
                        //insert new
                        if(newk!=null)
                            for(K2 k22 :newk)
                                secondary.add(Fun.t2(k22, key));
                        return;
                    }
                    if(newk==null){
                        //remove old
                        for(K2 k22 :oldk)
                            secondary.remove(Fun.t2(k22, key));
                        return;
                    }

                    Set<K2> hashes = new HashSet<K2>();
                    for(K2 k:oldk)
                        hashes.add(k);

                    //add new non existing items
                    for(K2 k2:newk){
                        if(!hashes.contains(k2)){
                            secondary.add(Fun.t2(k2, key));
                        }
                    }
                    //remove items which are in old, but not in new
                    for(K2 k2:newk){
                        hashes.remove(k2);
                    }
                    for(K2 k2:hashes){
                        secondary.remove(Fun.t2(k2, key));
                    }
                }
            }
        });
    }

    /**
     * Binds Secondary Set so it contains inverse mapping to Primary Map: Primary Value will become Secondary Key.
     * This is useful for creating bi-directional Maps.
     *
     * To lookup keys in Secondary Set use {@link Bind#findSecondaryKeys(java.util.NavigableSet, Object)}
     *
     * If Secondary Set is empty its content will be recreated from Primary Map.
     * This binding is not persistent. You need to restore it every time store is reopened.
     *
     * @param primary Primary Map for which inverse mapping will be created
     * @param inverse Secondary Set which will contain inverse mapping
     * @param <K> Key in Primary Map and Second Value in Secondary Set
     * @param <V> Value in Primary Map and Primary Value in Secondary Set
     */
    public static <K,V> void mapInverse(MapWithModificationListener<K,V> primary,
                                        Set<Fun.Tuple2<V, K>> inverse) {
        Bind.secondaryKey(primary,inverse, new Fun.Function2<V, K,V>(){
            @Override public V run(K key, V value) {
                return value;
            }
        });
    }


    /**
     * Find all Primary Keys associated with Secondary Key.
     * This is useful companion to {@link Bind#mapInverse(org.mapdb.Bind.MapWithModificationListener, java.util.Set)}
     * and {@link Bind#secondaryKey(org.mapdb.Bind.MapWithModificationListener, java.util.Set, org.mapdb.Fun.Function2)}
     * It can by also used to find values from 'MultiMap'.
     *
     * @param secondaryKeys Secondary Set or 'MultiMap' to find values in
     * @param secondaryKey key to look from
     * @param <K2> Secondary Key type
     * @param <K1> Primary Key type
     * @return all keys where primary value equals to `secondaryKey`
     * @deprecated (use {@link Bind#findVals2()}
     */
    public static <K2,K1> Iterable<K1> findSecondaryKeys(final NavigableSet<Fun.Tuple2<K2,K1>> secondaryKeys, final K2 secondaryKey) {
        return new Iterable<K1>(){
            @Override
            public Iterator<K1> iterator() {
                //use range query to get all values
                final Iterator<Fun.Tuple2<K2,K1>> iter =
                        ((NavigableSet)secondaryKeys) //cast is workaround for generics
                                .subSet(
                                        Fun.t2(secondaryKey,null), //NULL represents lower bound, everything is larger than null
                                        Fun.t2(secondaryKey,Fun.HI) // HI is upper bound everything is smaller then HI
                                ).iterator();

                return new Iterator<K1>() {
                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public K1 next() {
                        return iter.next().b;
                    }

                    @Override
                    public void remove() {
                        iter.remove();
                    }
                };
            }
        };

    }


    public static <A,B> Iterable<B> findVals2(final NavigableSet<Fun.Tuple2<A,B>> secondaryKeys, final A secondaryKey) {
        return findSecondaryKeys(secondaryKeys,secondaryKey);
    }

    public static <A,B,C> Iterable<C> findVals3(final NavigableSet<Fun.Tuple3<A,B,C>> secondaryKeys,
                                                final A a, final B b) {
        return new Iterable<C>(){
            @Override
            public Iterator<C> iterator() {
                //use range query to get all values
                final Iterator<Fun.Tuple3> iter =
                        ((NavigableSet)secondaryKeys) //cast is workaround for generics
                                .subSet(
                                        Fun.t3(a, b, null), //NULL represents lower bound, everything is larger than null
                                        Fun.t3(a,b==null?Fun.HI():b,Fun.HI()) // HI is upper bound everything is smaller then HI
                                ).iterator();

                return new Iterator<C>() {
                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public C next() {
                        return (C) iter.next().c;
                    }

                    @Override
                    public void remove() {
                        iter.remove();
                    }
                };
            }
        };

    }

    public static <A,B,C,D> Iterable<D> findVals4(final NavigableSet<Fun.Tuple4<A,B,C,D>> secondaryKeys, final A a, final B b, final C c) {
        return new Iterable<D>(){
            @Override
            public Iterator<D> iterator() {
                //use range query to get all values
                final Iterator<Fun.Tuple4> iter =
                        ((NavigableSet)secondaryKeys) //cast is workaround for generics
                                .subSet(
                                        Fun.t4(a,b,c, null), //NULL represents lower bound, everything is larger than null
                                        Fun.t4(a,b==null?Fun.HI():b,c==null?Fun.HI():c,Fun.HI()) // HI is upper bound everything is smaller then HI
                                ).iterator();

                return new Iterator<D>() {
                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public D next() {
                        return (D) iter.next().d;
                    }

                    @Override
                    public void remove() {
                        iter.remove();
                    }
                };
            }
        };

    }



    /**
     * Binds Secondary Map so it it creates [histogram](http://en.wikipedia.org/wiki/Histogram) from
     * data in Primary Map. Histogram keeps count how many items are in each category.
     * This method takes function which defines in what category each Primary Map entry is in.
     *
     *
     * If Secondary Map is empty its content will be recreated from Primary Map.
     * This binding is not persistent. You need to restore it every time store is reopened.
     *
     * @param primary Primary Map to create histrogram for
     * @param histogram Secondary Map to create histogram for, key is Category, value is number of items in category
     * @param entryToCategory returns Category in which entry from Primary Map belongs to.
     * @param <K> Key type in primary map
     * @param <V> Value type in primary map
     * @param <C> Category type
     */
    public static <K,V,C> void histogram(MapWithModificationListener<K,V> primary, final ConcurrentMap<C,Long> histogram,
                                  final Fun.Function2<C, K, V> entryToCategory){

        MapListener<K,V> listener = new MapListener<K, V>() {
            @Override public void update(K key, V oldVal, V newVal) {
                if(newVal == null){
                    //removal
                    C category = entryToCategory.run(key,oldVal);
                    incrementHistogram(category, -1);
                }else if(oldVal==null){
                    //insert
                    C category = entryToCategory.run(key,newVal);
                    incrementHistogram(category, 1);
                }else{
                    //update, must remove old key and insert new
                    C oldCat = entryToCategory.run(key, oldVal);
                    C newCat = entryToCategory.run(key, newVal);
                    if(oldCat == newCat || oldCat.equals(newCat)) return;
                    incrementHistogram(oldCat,-1);
                    incrementHistogram(oldCat,1);
                }

            }

            /** atomically update counter in histogram*/
            private void incrementHistogram(C category, long i) {
                for(;;){
                    Long oldCount = histogram.get(category);
                    if(oldCount == null){
                        //insert new count
                        if(histogram.putIfAbsent(category,i) == null)
                            return;
                    }else{
                        //increase existing count
                        Long newCount = oldCount+i;
                        if(histogram.replace(category,oldCount, newCount))
                            return;
                    }
                }
            }
        };

        primary.addModificationListener(listener);
    }




}
