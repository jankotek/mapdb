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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Binding is simple yet powerful way to keep secondary collection synchronized with primary collection.
 * Primary collection provides notification on updates and secondary collection is modified accordingly.
 * This way MapDB provides secondary indexes, values and keys. It also supports less usual scenarios such
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
 *  NOTE: Binding just installs Modification Listener on primary collection. Binding itself is not persistent
 *  and has to be restored after primary collection is loaded. Data contained in secondary collection are persistent.
 *
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
        public void modificationListenerAdd(MapListener<K, V> listener);

        /**
         * Remove registered notification listener
         *
         * @param listener  callback interface notified when map changes
         */
        public void modificationListenerRemove(MapListener<K, V> listener);


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
     * NOTE: Binding just installs Modification Listener on primary collection. Binding itself is not persistent
     * and has to be restored after primary collection is loaded. Data contained in secondary collection are persistent.
     *
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
    public static <K,V> void  size(MapWithModificationListener<K,V> map, final Atomic.Long sizeCounter){
        //set initial value first if necessary
        if(sizeCounter.get() == 0){
            long size = map.sizeLong();
            if(sizeCounter.get()!=size)
                sizeCounter.set(size);
        }

        map.modificationListenerAdd(new MapListener<K, V>() {
            @Override
            public void update(K key, V oldVal, V newVal) {
                if (oldVal == null && newVal != null) {
                    sizeCounter.incrementAndGet();
                } else if (oldVal != null && newVal == null) {
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
     *
     * NOTE: Binding just installs Modification Listener on primary collection. Binding itself is not persistent
     * and has to be restored after primary collection is loaded. Data contained in secondary collection are persistent.
     *
     * Type params:
     *
     *  * `<K>` - key type in primary and Secondary Map
     *  * `<V>` - value type in Primary Map
     *  * `<V2>` - value type in Secondary Map
     *  .
     * @param map Primary Map
     * @param secondary Secondary Map with custom
     * @param fun function which calculates secondary value from primary key and value
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
        map.modificationListenerAdd(new MapListener<K, V>() {
            @Override
            public void update(K key, V oldVal, V newVal) {
                if (newVal == null) {
                    //removal
                    secondary.remove(key);
                } else {
                    secondary.put(key, fun.run(key, newVal));
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
     *
     * NOTE: Binding just installs Modification Listener on primary collection. Binding itself is not persistent
     * and has to be restored after primary collection is loaded. Data contained in secondary collection are persistent.
     *
     * Type params:
     *
     *  * `<K>` - key type in primary and Secondary Map
     *  * `<V>` - value type in Primary Map
     *  * `<V2>` - value type in Secondary Map
     * .
     * @param map Primary Map
     * @param secondary Secondary Map with custom
     * @param fun function which calculates secondary values from primary key and value
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
        map.modificationListenerAdd(new MapListener<K, V>() {
            @Override
            public void update(K key, V oldVal, V newVal) {
                if (newVal == null) {
                    //removal
                    V2[] v = fun.run(key, oldVal);
                    if (v != null)
                        for (V2 v2 : v)
                            secondary.remove(Fun.t2(key, v2));
                } else if (oldVal == null) {
                    //insert
                    V2[] v = fun.run(key, newVal);
                    if (v != null)
                        for (V2 v2 : v)
                            secondary.add(Fun.t2(key, v2));
                } else {
                    //update, must remove old key and insert new
                    V2[] oldv = fun.run(key, oldVal);
                    V2[] newv = fun.run(key, newVal);
                    if (oldv == null) {
                        //insert new
                        if (newv != null)
                            for (V2 v : newv)
                                secondary.add(Fun.t2(key, v));
                        return;
                    }
                    if (newv == null) {
                        //remove old
                        for (V2 v : oldv)
                            secondary.remove(Fun.t2(key, v));
                        return;
                    }

                    Set<V2> hashes = new HashSet<V2>();
                    Collections.addAll(hashes, oldv);

                    //add new non existing items
                    for (V2 v : newv) {
                        if (!hashes.contains(v)) {
                            secondary.add(Fun.t2(key, v));
                        }
                    }
                    //remove items which are in old, but not in new
                    for (V2 v : newv) {
                        hashes.remove(v);
                    }
                    for (V2 v : hashes) {
                        secondary.remove(Fun.t2(key, v));
                    }
                }
            }
        });
    }


    /**
     * Binds Secondary Set so it contains Secondary Key (Index). Usefull if you need
     * to lookup Keys from Primary Map by custom criteria. Other use is for reverse lookup
     *
     * To lookup keys in Secondary Set use {@link Fun#filter(java.util.NavigableSet, Object)}
     *
     * If Secondary Set is empty its content will be recreated from Primary Map.
     * This binding is not persistent. You need to restore it every time store is reopened.
     *
     * NOTE: Binding just installs Modification Listener on primary collection. Binding itself is not persistent
     * and has to be restored after primary collection is loaded. Data contained in secondary collection are persistent.
     *
     * Type params:
     *
     *  * `<K>` - Key in Primary Map
     *  * `<V>` - Value in Primary Map
     *  * `<K2>` - Secondary
     *
     * @param map primary map
     * @param secondary secondary set
     * @param fun function which calculates Secondary Key from Primary Key and Value
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
        map.modificationListenerAdd(new MapListener<K, V>() {
            @Override
            public void update(K key, V oldVal, V newVal) {
                if (newVal == null) {
                    //removal
                    secondary.remove(Fun.t2(fun.run(key, oldVal), key));
                } else if (oldVal == null) {
                    //insert
                    secondary.add(Fun.t2(fun.run(key, newVal), key));
                } else {
                    //update, must remove old key and insert new
                    K2 oldKey = fun.run(key, oldVal);
                    K2 newKey = fun.run(key, newVal);
                    if (oldKey == newKey || oldKey.equals(newKey)) return;
                    secondary.remove(Fun.t2(oldKey, key));
                    secondary.add(Fun.t2(newKey, key));
                }
            }
        });
    }

    /**
     * Binds Secondary Set so it contains Secondary Key (Index). Usefull if you need
     * to lookup Keys from Primary Map by custom criteria. Other use is for reverse lookup
     *
     * If Secondary Set is empty its content will be recreated from Primary Map.
     * This binding is not persistent. You need to restore it every time store is reopened.
     *
     * NOTE: Binding just installs Modification Listener on primary collection. Binding itself is not persistent
     * and has to be restored after primary collection is loaded. Data contained in secondary collection are persistent.
     *
     * Type params:
     *
     *  * `<K>` - Key in Primary Map
     *  * `<V>` - Value in Primary Map
     *  * `<K2>` - Secondary
     *
     * @param map primary map
     * @param secondary secondary set
     * @param fun function which calculates Secondary Key from Primary Key and Value
     */
    public static <K,V, K2> void secondaryKey(MapWithModificationListener<K, V> map,
                                              final Map<K2, K> secondary,
                                              final Fun.Function2<K2, K, V> fun){
        //fill if empty
        if(secondary.isEmpty()){
            for(Map.Entry<K,V> e:map.entrySet()){
                secondary.put(fun.run(e.getKey(), e.getValue()), e.getKey());
            }
        }
        //hook listener
        map.modificationListenerAdd(new MapListener<K, V>() {
            @Override
            public void update(K key, V oldVal, V newVal) {
                if (newVal == null) {
                    //removal
                    secondary.remove(fun.run(key, oldVal));
                } else if (oldVal == null) {
                    //insert
                    secondary.put(fun.run(key, newVal), key);
                } else {
                    //update, must remove old key and insert new
                    K2 oldKey = fun.run(key, oldVal);
                    K2 newKey = fun.run(key, newVal);
                    if (oldKey == newKey || oldKey.equals(newKey)) return;
                    secondary.remove(oldKey);
                    secondary.put(newKey, key);
                }
            }
        });
    } 
    /**
     * Binds Secondary Set so it contains Secondary Key (Index). Useful if you need
     * to lookup Keys from Primary Map by custom criteria. Other use is for reverse lookup
     *
     * To lookup keys in Secondary Set use {@link Fun#filter(java.util.NavigableSet, Object)}}
     *
     *
     * If Secondary Set is empty its content will be recreated from Primary Map.
     *
     * NOTE: Binding just installs Modification Listener on primary collection. Binding itself is not persistent
     * and has to be restored after primary collection is loaded. Data contained in secondary collection are persistent.
     *
     *
     * Type params:
     *
     *  * `<K>` - Key in Primary Map
     *  * `<V>` - Value in Primary Map
     *  * `<K2>` - Secondary
     *
     * @param map primary map
     * @param secondary secondary set
     * @param fun function which calculates Secondary Keys from Primary Key and Value
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
        map.modificationListenerAdd(new MapListener<K, V>() {
            @Override
            public void update(K key, V oldVal, V newVal) {
                if (newVal == null) {
                    //removal
                    K2[] k2 = fun.run(key, oldVal);
                    if (k2 != null)
                        for (K2 k22 : k2)
                            secondary.remove(Fun.t2(k22, key));
                } else if (oldVal == null) {
                    //insert
                    K2[] k2 = fun.run(key, newVal);
                    if (k2 != null)
                        for (K2 k22 : k2)
                            secondary.add(Fun.t2(k22, key));
                } else {
                    //update, must remove old key and insert new
                    K2[] oldk = fun.run(key, oldVal);
                    K2[] newk = fun.run(key, newVal);
                    if (oldk == null) {
                        //insert new
                        if (newk != null)
                            for (K2 k22 : newk)
                                secondary.add(Fun.t2(k22, key));
                        return;
                    }
                    if (newk == null) {
                        //remove old
                        for (K2 k22 : oldk)
                            secondary.remove(Fun.t2(k22, key));
                        return;
                    }

                    Set<K2> hashes = new HashSet<K2>();
                    Collections.addAll(hashes, oldk);

                    //add new non existing items
                    for (K2 k2 : newk) {
                        if (!hashes.contains(k2)) {
                            secondary.add(Fun.t2(k2, key));
                        }
                    }
                    //remove items which are in old, but not in new
                    for (K2 k2 : newk) {
                        hashes.remove(k2);
                    }
                    for (K2 k2 : hashes) {
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
     * To lookup keys in Secondary Set use {@link Fun#filter(java.util.NavigableSet, Object)}
     *
     * If Secondary Set is empty its content will be recreated from Primary Map.
     *
     * NOTE: Binding just installs Modification Listener on primary collection. Binding itself is not persistent
     * and has to be restored after primary collection is loaded. Data contained in secondary collection are persistent.
     *
     * Type params:
     *
     *  * `<K>` - Key in Primary Map and Second Value in Secondary Set
     *  * `<V>` - Value in Primary Map and Primary Value in Secondary Set
     *
     * @param primary Primary Map for which inverse mapping will be created
     * @param inverse Secondary Set which will contain inverse mapping
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
     * Binds Secondary Set so it contains inverse mapping to Primary Map: Primary Value will become Secondary Key.
     * This is useful for creating bi-directional Maps.
     *
     * In this case some data may be lost, if there are duplicated primary values.
     * It is recommended to use multimap: `NavigableSet<Fun.Tuple2<V,K>>` which
     * handles value duplicities. Use @{link Bind.mapInverse(MapWithModificationListener<K,V>Set<Fun.Tuple2<V, K>>}
     *
     * If Secondary Set is empty its content will be recreated from Primary Map.
     *
     * NOTE: Binding just installs Modification Listener on primary collection. Binding itself is not persistent
     * and has to be restored after primary collection is loaded. Data contained in secondary collection are persistent.
     *
     * Type params:
     *
     *  * `<K>` - Key in Primary Map and Second Value in Secondary Set
     *  * `<V>` - Value in Primary Map and Primary Value in Secondary Set
     *
     * @param primary Primary Map for which inverse mapping will be created
     * @param inverse Secondary Set which will contain inverse mapping
     */
    public static <K,V> void mapInverse(MapWithModificationListener<K,V> primary,
                                        Map<V, K> inverse) {
        Bind.secondaryKey(primary,inverse, new Fun.Function2<V, K,V>(){
            @Override public V run(K key, V value) {
                return value;
            }
        });
    }







    /**
     * Binds Secondary Map so it it creates [histogram](http://en.wikipedia.org/wiki/Histogram) from
     * data in Primary Map. Histogram keeps count how many items are in each category.
     * This method takes function which defines in what category each Primary Map entry is in.
     *
     *
     * If Secondary Map is empty its content will be recreated from Primary Map.
     *
     * NOTE: Binding just installs Modification Listener on primary collection. Binding itself is not persistent
     * and has to be restored after primary collection is loaded. Data contained in secondary collection are persistent.
     *
     *
     * Type params:
     *
     *  * `<K>` - Key type in primary map
     *  * `<V>` - Value type in primary map
     *  * `<C>` - Category type
     *
     * @param primary Primary Map to create histogram for
     * @param histogram Secondary Map to create histogram for, key is Category, value is number of items in category
     * @param entryToCategory returns Category in which entry from Primary Map belongs to.
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

        primary.modificationListenerAdd(listener);
    }




}
