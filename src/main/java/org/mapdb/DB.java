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

import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A database with easy access to named maps and other collections.
 *
 * @author Jan Kotek
 */
@SuppressWarnings("unchecked")
public class DB {

    /** Engine which provides persistence for this DB*/
    protected Engine engine;
    /** already loaded named collections. It is important to keep collections as singletons, because of 'in-memory' locking*/
    protected Map<String, WeakReference<?>> collections = new HashMap<String, WeakReference<?>>();

    /** view over named records */
    protected Map<String, Long> nameDir;

    /** default serializer used for persistence. Handles POJO and other stuff which requires write-able access to Engine */
    protected Serializer<?> defaultSerializer;

    /**
     * Construct new DB. It is just thin layer over {@link Engine} which does the real work.
     * @param engine
     */
    public DB(final Engine engine){
        this.engine = engine;
        // load serializer
        final CopyOnWriteArrayList<SerializerPojo.ClassInfo> classInfos = engine.get(engine.serializerRecid(), SerializerPojo.serializer);
        this.defaultSerializer = new SerializerPojo(classInfos){
            @Override
            protected void saveClassInfo() {
                //hook to save classes if they are updated
                //I did not want to create direct dependency between SerialierPojo and Engine
                engine.update(engine.serializerRecid(), registered, SerializerPojo.serializer);
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
    @NotNull
    synchronized public <K,V> HTreeMap<K,V> getHashMap(String name){
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


    /**
     * Creates new HashMap with more specific arguments
     *
     * @param name of map to create
     * @param keySerializer used to convert keys into/from binary form. Use null for default value.
     * @param valueSerializer used to convert values into/from binary form. Use null for default value.
     * @param <K> key type
     * @param <V> value type
     * @throws IllegalArgumentException if name is already used
     * @return newly created map
     */
    @NotNull
    synchronized public <K,V> HTreeMap<K,V> createHashMap(
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
    @NotNull
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


    /**
     * Creates new HashSet
     * @param name of set to create
     * @param serializer used to convert keys into/from binary form. Use null for default value.
     * @param <K> item type
     * @throws IllegalArgumentException if name is already used

     */
    @NotNull
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
    @NotNull
    synchronized public <K,V> BTreeMap<K,V> getTreeMap(String name){
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

    /**
     * Creates new TreeMap
     * @param name of map to create
     * @param nodeSize maximal size of node, larger node causes overflow and creation of new BTree node. Use large number for small keys, use small number for large keys.
     * @param valuesStoredOutsideNodes if true, values are stored outside of BTree nodes. Use 'true' if your values are large.
     * @param keySerializer used to convert keys into/from binary form. Use null for default value.
     * @param valueSerializer used to convert values into/from binary form. Use null for default value.
     * @param comparator used to sort keys. Use null for default value. TODO delta packing
     * @param <K> key type
     * @param <V> value type
     * @throws IllegalArgumentException if name is already used
     * @return newly created map
     */
    @NotNull
    synchronized public <K,V> BTreeMap<K,V> createTreeMap(
            String name, int nodeSize, boolean valuesStoredOutsideNodes,
            Serializer<K[]> keySerializer, Serializer<V> valueSerializer, Comparator<K> comparator){
        checkNameNotExists(name);
        BTreeMap<K,V> ret = new BTreeMap<K,V>(engine, nodeSize, true,valuesStoredOutsideNodes, defaultSerializer, keySerializer, valueSerializer, comparator);
        nameDir.put(name, ret.treeRecid);
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }


    /**
     * Get Named directory. Key is name, value is recid under which named record is stored
     * @return
     */
    @NotNull
    public Map<String, Long> getNameDir(){
        return nameDir;
    }


    /**
     * Opens existing or creates new B-linked-tree Set.
     *
     * @param name of set
     * @param <K> values in set
     * @return set
     */
    @NotNull
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

    /**
     * Creates new TreeSet.
     * @param name of set to create
     * @param nodeSize maximal size of node, larger node causes overflow and creation of new BTree node. Use large number for small keys, use small number for large keys.
     * @param serializer used to convert keys into/from binary form. Use null for default value.
     * @param comparator used to sort keys. Use null for default value. TODO delta packing
     * @param <K>
     * @throws IllegalArgumentException if name is already used
     * @return
     */
    @NotNull
    synchronized public <K> NavigableSet<K> createTreeSet(String name, int nodeSize, Serializer<K[]> serializer, Comparator<K> comparator){
        checkNameNotExists(name);
        BTreeMap<K,Object> ret = new BTreeMap<K,Object>(engine, nodeSize, true, false, defaultSerializer, serializer, null, comparator);
        nameDir.put(name, ret.treeRecid);
        NavigableSet<K> ret2 = ret.keySet();
        collections.put(name, new WeakReference<Object>(ret2));
        return ret2;
    }

    /**
     * Checks that object with given name does not exist yet.
     * @param name to check
     * @throws IllegalArgumentException if name is already used
     */
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

    /**
     * @return true if DB is closed and can no longer be used
     */
    public synchronized  boolean isClosed(){
        return engine == null;
    }

    /**
     * Commit changes made on collections loaded by this DB
     *
     * @see org.mapdb.Engine#commit()
     */
    synchronized public void commit() {
        checkNotClosed();
        engine.commit();
    }

    /**
     * Rollback changes made on collections loaded by this DB
     *
     * @see org.mapdb.Engine#rollback()
     */
    synchronized public void rollback() {
        checkNotClosed();
        engine.rollback();
    }

    /**
     * not yet implemented
     */
    synchronized public void compact(){

    }


    /**
     * Make readonly snapshot view of DB and all of its collection
     * Collections loaded by this instance are not affected (are still mutable).
     * You have to load new collections from DB returned by this method
     *
     * @return readonly snapshot view
     */
    @NotNull
    synchronized public DB snapshot(){
        Engine snapshot = SnapshotEngine.createSnapshotFor(engine);
        return new DB (snapshot);
    }

    /**
     * @return default serializer used in this DB, it handles POJO and other stuff.
     */
    @NotNull
    public Serializer<?> getDefaultSerializer() {
        return defaultSerializer;
    }

    /**
     * @return underlying engine which takes care of persistence for this DB.
     */
    @NotNull
    public Engine getEngine() {
        return engine;
    }
}
