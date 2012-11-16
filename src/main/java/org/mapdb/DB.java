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

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * A database with easy access to named maps and other collections.
 *
 * @author Jan Kotek
 */
@SuppressWarnings("unchecked")
public class DB {

    protected Engine engine;
    protected Map<String, WeakReference<?>> collections = new HashMap<String, WeakReference<?>>();

    protected Map<String, Long> nameDir;

    protected Serializer defaultSerializer;

    public DB(final Engine engine){
        this.engine = engine;
        final ArrayList classInfos = engine.recordGet(engine.serializerRecid(), SerializerPojo.serializer);
        this.defaultSerializer = new SerializerPojo(classInfos){
            @Override
            protected void saveClassInfo() {
                engine.recordUpdate(engine.serializerRecid(), registered, SerializerPojo.serializer);
            }
        };

        //open name dir
        nameDir = HTreeMap.preinitNamedDir(engine);

    }

    /**
     * Opens existing or creates new Hash Tree Map.
     * This collection perform well under concurrent access.
     * Is best for large keys and large values.
     *
     * @param name of map
     * @param <K> key
     * @param <V> value
     * @return map
     */
    synchronized public <K,V> ConcurrentMap<K,V> getHashMap(String name){
        checkNotClosed();
        HTreeMap<K,V> ret = (HTreeMap<K, V>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        Long recid = nameDir.get(name);
        if(recid!=null){
            //open existing map
            ret = new HTreeMap<K,V>(engine, recid,defaultSerializer);
            if(CC.ASSERT && !ret.hasValues) throw new ClassCastException("Collection is Set, not Map");
        }else{
            //create new map
            ret = new HTreeMap<K,V>(engine,true,defaultSerializer,null, null);
            nameDir.put(name, ret.rootRecid);
        }
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }


    synchronized public <K,V> ConcurrentMap<K,V> createHashMap(
            String name, Serializer<K> keySerializer, Serializer<V> valueSerializer){
        checkNameNotExists(name);
        HTreeMap<K,V> ret = new HTreeMap<K,V>(engine, true, defaultSerializer, keySerializer, valueSerializer);
        nameDir.put(name, ret.rootRecid);
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }

    /**
     *  Opens existing or creates new Hash Tree Set.
     *
     * @param name of Set
     * @param <K> values in set
     * @return set
     */
    synchronized public <K> Set<K> getHashSet(String name){
        checkNotClosed();
        Set<K> ret = (Set<K>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        Long recid = nameDir.get(name);
        if(recid!=null){
            //open existing map
            HTreeMap<K,Object> m = new HTreeMap<K,Object>(engine, recid, defaultSerializer);
            if(CC.ASSERT && m.hasValues) throw new ClassCastException("Collection is Map, not Set");
            ret = m.keySet();
        }else{
            //create new map
            HTreeMap<K,Object> m = new HTreeMap<K,Object>(engine, false, defaultSerializer, null, null);
            ret = m.keySet();
            nameDir.put(name, m.rootRecid);
        }
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }


    synchronized public <K> Set<K> createHashSet(String name, Serializer<K> serializer){
        checkNameNotExists(name);
        HTreeMap<K,Object> ret = new HTreeMap<K,Object>(engine, true, defaultSerializer, serializer, null);
        nameDir.put(name, ret.rootRecid);
        Set<K> ret2 = ret.keySet();
        collections.put(name, new WeakReference<Object>(ret2));
        return ret2;
    }

    /**
     * Opens existing or creates new B-linked-tree Map.
     * This collection performs well under concurrent access.
     * Only trade-off are deletes, which causes tree fragmentation.
     * It is ordered and best suited for small keys and values.
     *
     * @param name of map
     * @param <K> key
     * @param <V> value
     * @return map
     */
    synchronized public <K,V> ConcurrentNavigableMap<K,V> getTreeMap(String name){
        checkNotClosed();
        BTreeMap<K,V> ret = (BTreeMap<K,V>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        Long recid = nameDir.get(name);
        if(recid!=null){
            //open existing map
            ret = new BTreeMap<K,V>(engine, recid,defaultSerializer);
            if(CC.ASSERT && !ret.hasValues) throw new ClassCastException("Collection is Set, not Map");
        }else{
            //create new map
            ret = new BTreeMap<K,V>(engine,BTreeMap.DEFAULT_MAX_NODE_SIZE, true, false, defaultSerializer, null, null, null);
            nameDir.put(name, ret.treeRecid);
        }
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }


    synchronized public <K,V> ConcurrentNavigableMap<K,V> createTreeMap(
            String name, int nodeSize, boolean valuesStoredOutsideNodes,
            Serializer<K[]> keySerializer, Serializer<V> valueSerializer, Comparator<K> comparator){
        checkNameNotExists(name);
        BTreeMap<K,V> ret = new BTreeMap<K,V>(engine, nodeSize, true,valuesStoredOutsideNodes, defaultSerializer, keySerializer, valueSerializer, comparator);
        nameDir.put(name, ret.rootRecid);
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }


    /**
     * Opens existing or creates new B-linked-tree Set.
     *
     * @param name of set
     * @param <K> values in set
     * @return set
     */
    synchronized public <K> NavigableSet<K> getTreeSet(String name){
        checkNotClosed();
        NavigableSet<K> ret = (NavigableSet<K>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        Long recid = nameDir.get(name);
        if(recid!=null){
            //open existing map
            BTreeMap<K,Object> m = new BTreeMap<K,Object>(engine,  recid, defaultSerializer);
            if(CC.ASSERT && m.hasValues) throw new ClassCastException("Collection is Map, not Set");
            ret = m.keySet();
        }else{
            //create new map
            BTreeMap<K,Object> m =  new BTreeMap<K,Object>(engine,BTreeMap.DEFAULT_MAX_NODE_SIZE,
                    false, false, defaultSerializer, null, null, null);
            nameDir.put(name, m.treeRecid);
            ret = m.keySet();
        }

        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }

    synchronized public <K> NavigableSet<K> createTreeSet(String name, int nodeSize, Serializer<K[]> serializer, Comparator<K> comparator){
        checkNameNotExists(name);
        BTreeMap<K,Object> ret = new BTreeMap<K,Object>(engine, nodeSize, true, false, defaultSerializer, serializer, null, comparator);
        nameDir.put(name, ret.rootRecid);
        NavigableSet<K> ret2 = ret.keySet();
        collections.put(name, new WeakReference<Object>(ret2));
        return ret2;
    }

    protected void checkNameNotExists(String name) {
        if(nameDir.get(name)!=null)
            throw new IllegalArgumentException("Name already used: "+name);
    }


    /**
     * Closes database.
     * All other methods will throw 'IllegalAccessError' after this method was called.
     * <p/>
     * !! it is necessary to call this method before JVM exits!!
     */
    synchronized public void close(){
        if(engine == null) return;
        engine.close();
        //dereference db to prevent memory leaks
        engine = null;
        collections = null;
        defaultSerializer = null;
    }

    /**
     * All collections are weakly referenced to prevent two instances of the same collection in memory.
     * This is mainly for locking, two instances of the same lock would not simply work.
     */
    protected Object getFromWeakCollection(String name){

        WeakReference<?> r = collections.get(name);
        if(r==null) return null;
        Object o = r.get();
        if(o==null) collections.remove(name);
        return o;
    }


    protected void checkNotClosed() {
        if(engine == null) throw new IllegalAccessError("DB was already closed");
    }

    synchronized public void commit() {
        checkNotClosed();
        engine.commit();
    }

    synchronized public void rollback() {
        checkNotClosed();
        engine.rollback();
    }

    synchronized public void defrag(){

    }

    public Serializer getDefaultSerializer() {
        return defaultSerializer;
    }

    public Engine getEngine() {
        return engine;
    }
}
