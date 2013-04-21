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
        reinit();
    }

    protected void reinit() {
        // load serializer
        final CopyOnWriteArrayList<SerializerPojo.ClassInfo> classInfos = engine.get(Engine.CLASS_INFO_RECID, SerializerPojo.serializer);
        this.defaultSerializer = new SerializerPojo(classInfos){
            @Override
            protected void saveClassInfo() {
                //hook to save classes if they are updated
                //I did not want to create direct dependency between SerialierPojo and Engine
                engine.update(Engine.CLASS_INFO_RECID, registered, SerializerPojo.serializer);
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
    synchronized public <K,V> HTreeMap<K,V> getHashMap(String name){
        checkNotClosed();
        HTreeMap<K,V> ret = (HTreeMap<K, V>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        Long recid = nameDir.get(name);
        if(recid!=null){
            //open existing map
            ret = new HTreeMap<K,V>(engine, recid,defaultSerializer);
            if(!ret.hasValues) throw new ClassCastException("Collection is Set, not Map");
        }else{
            //create new map
            ret = new HTreeMap<K,V>(engine,true,false,Utils.RANDOM.nextInt(), defaultSerializer,null, null);
            nameDir.put(name, ret.rootRecid);
        }
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }


    /**
     * Creates new HashMap with more specific arguments
     *
     * @param name of map to create
     * @param keepCounter if counter should be kept, without counter updates are faster, but entire collection needs to be traversed to count items.
     * @param keySerializer used to convert keys into/from binary form. Use null for default value.
     * @param valueSerializer used to convert values into/from binary form. Use null for default value.
     * @param <K> key type
     * @param <V> value type
     * @throws IllegalArgumentException if name is already used
     * @return newly created map
     */
    synchronized public <K,V> HTreeMap<K,V> createHashMap(
            String name, boolean keepCounter, Serializer<K> keySerializer, Serializer<V> valueSerializer){
        checkNameNotExists(name);
        HTreeMap<K,V> ret = new HTreeMap<K,V>(engine, true,keepCounter,Utils.RANDOM.nextInt(), defaultSerializer, keySerializer, valueSerializer);
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
            if(m.hasValues) throw new ClassCastException("Collection is Map, not Set");
            ret = m.keySet();
        }else{
            //create new map
            HTreeMap<K,Object> m = new HTreeMap<K,Object>(engine, false,false, Utils.RANDOM.nextInt(), defaultSerializer, null, null);
            ret = m.keySet();
            nameDir.put(name, m.rootRecid);
        }
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }


    /**
     * Creates new HashSet
     * @param name of set to create
     * @param keepCounter if counter should be kept, without counter updates are faster, but entire collection needs to be traversed to count items.
     * @param serializer used to convert keys into/from binary form. Use null for default value.
     * @param <K> item type
     * @throws IllegalArgumentException if name is already used
     */
    
    synchronized public <K> Set<K> createHashSet(String name, boolean keepCounter, Serializer<K> serializer){
        checkNameNotExists(name);
        HTreeMap<K,Object> ret = new HTreeMap<K,Object>(engine, false,keepCounter,Utils.RANDOM.nextInt(), defaultSerializer, serializer, null);
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
    
    synchronized public <K,V> BTreeMap<K,V> getTreeMap(String name){
        checkNotClosed();
        BTreeMap<K,V> ret = (BTreeMap<K,V>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        Long recid = nameDir.get(name);
        if(recid!=null){
            //open existing map
            ret = new BTreeMap<K,V>(engine, recid,defaultSerializer);
            if(!ret.hasValues) throw new ClassCastException("Collection is Set, not Map");
        }else{
            //create new map
            ret = new BTreeMap<K,V>(engine,BTreeMap.DEFAULT_MAX_NODE_SIZE, true, false,false, defaultSerializer, null, null, null);
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
     * @param keepCounter if counter should be kept, without counter updates are faster, but entire collection needs to be traversed to count items.
     * @param keySerializer used to convert keys into/from binary form. Use null for default value.
     * @param valueSerializer used to convert values into/from binary form. Use null for default value.
     * @param comparator used to sort keys. Use null for default value. TODO delta packing
     * @param <K> key type
     * @param <V> value type
     * @throws IllegalArgumentException if name is already used
     * @return newly created map
     */
    synchronized public <K,V> BTreeMap<K,V> createTreeMap(
            String name, int nodeSize, boolean valuesStoredOutsideNodes, boolean keepCounter,
            BTreeKeySerializer<K> keySerializer, Serializer<V> valueSerializer, Comparator<K> comparator){
        checkNameNotExists(name);
        keySerializer = fillNulls(keySerializer);
        BTreeMap<K,V> ret = new BTreeMap<K,V>(engine, nodeSize, true,valuesStoredOutsideNodes, keepCounter,defaultSerializer, keySerializer, valueSerializer, comparator);
        nameDir.put(name, ret.treeRecid);
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }

    protected <K> BTreeKeySerializer<K> fillNulls(BTreeKeySerializer<K> keySerializer) {
        if(keySerializer instanceof BTreeKeySerializer.Tuple2KeySerializer){
            BTreeKeySerializer.Tuple2KeySerializer s = (BTreeKeySerializer.Tuple2KeySerializer) keySerializer;
            return new BTreeKeySerializer.Tuple2KeySerializer(
                    s.aComparator!=null?s.aComparator:Utils.COMPARABLE_COMPARATOR,
                    s.aSerializer!=null?s.aSerializer:defaultSerializer,
                    s.bSerializer!=null?s.bSerializer:defaultSerializer
            );
        }
        if(keySerializer instanceof BTreeKeySerializer.Tuple3KeySerializer){
            BTreeKeySerializer.Tuple3KeySerializer s = (BTreeKeySerializer.Tuple3KeySerializer) keySerializer;
            return new BTreeKeySerializer.Tuple3KeySerializer(
                    s.aComparator!=null?s.aComparator:Utils.COMPARABLE_COMPARATOR,
                    s.bComparator!=null?s.bComparator:Utils.COMPARABLE_COMPARATOR,
                    s.aSerializer!=null?s.aSerializer:defaultSerializer,
                    s.bSerializer!=null?s.bSerializer:defaultSerializer,
                    s.cSerializer!=null?s.cSerializer:defaultSerializer
            );
        }
        if(keySerializer instanceof BTreeKeySerializer.Tuple4KeySerializer){
            BTreeKeySerializer.Tuple4KeySerializer s = (BTreeKeySerializer.Tuple4KeySerializer) keySerializer;
            return new BTreeKeySerializer.Tuple4KeySerializer(
                    s.aComparator!=null?s.aComparator:Utils.COMPARABLE_COMPARATOR,
                    s.bComparator!=null?s.bComparator:Utils.COMPARABLE_COMPARATOR,
                    s.cComparator!=null?s.cComparator:Utils.COMPARABLE_COMPARATOR,
                    s.aSerializer!=null?s.aSerializer:defaultSerializer,
                    s.bSerializer!=null?s.bSerializer:defaultSerializer,
                    s.cSerializer!=null?s.cSerializer:defaultSerializer,
                    s.dSerializer!=null?s.dSerializer:defaultSerializer
            );
        }

        return keySerializer;
    }


    /**
     * Get Named directory. Key is name, value is recid under which named record is stored
     * @return
     */
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
    synchronized public <K> NavigableSet<K> getTreeSet(String name){
        checkNotClosed();
        NavigableSet<K> ret = (NavigableSet<K>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        Long recid = nameDir.get(name);
        if(recid!=null){
            //open existing map
            BTreeMap<K,Object> m = new BTreeMap<K,Object>(engine,  recid, defaultSerializer);
            if(m.hasValues) throw new ClassCastException("Collection is Map, not Set");
            ret = m.keySet();
        }else{
            //create new map
            BTreeMap<K,Object> m =  new BTreeMap<K,Object>(engine,BTreeMap.DEFAULT_MAX_NODE_SIZE,
                    false, false,false, defaultSerializer, null, null, null);
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
     * @param keepCounter if counter should be kept, without counter updates are faster, but entire collection needs to be traversed to count items.
     * @param serializer used to convert keys into/from binary form. Use null for default value.
     * @param comparator used to sort keys. Use null for default value. TODO delta packing
     * @param <K>
     * @throws IllegalArgumentException if name is already used
     * @return
     */
    synchronized public <K> NavigableSet<K> createTreeSet(String name,int nodeSize, boolean keepCounter, BTreeKeySerializer<K> serializer, Comparator<K> comparator){
        checkNameNotExists(name);
        serializer = fillNulls(serializer);
        BTreeMap<K,Object> ret = new BTreeMap<K,Object>(engine, nodeSize, false, false, keepCounter, defaultSerializer, serializer, null, comparator);
        nameDir.put(name, ret.treeRecid);
        NavigableSet<K> ret2 = ret.keySet();
        collections.put(name, new WeakReference<Object>(ret2));
        return ret2;
    }

//    synchronized public <E> Queue<E> getQueue(String name){
//        Long recid = nameDir.get(name);
//        if(recid==null){
//
//        }else{
//            return new Queues.Lifo<E>(engine, getDefaultSerializer(),  recid, true);
//        }
//    }


    synchronized public <E> Queue<E> getQueue(String name) {
        Long recid = nameDir.get(name);
        if(recid == null){
            recid = Queues.createQueue(engine, getDefaultSerializer(), getDefaultSerializer());
            nameDir.put(name,recid);
        }
        Queue<E> ret = Queues.getQueue(engine,getDefaultSerializer(),recid);
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }

    synchronized public <E> Queue<E> getStack(String name) {
        Long recid = nameDir.get(name);
        if(recid == null){
            recid = Queues.createStack(engine, getDefaultSerializer(), getDefaultSerializer(), true);
            nameDir.put(name,recid);
        }
        Queue<E> ret = Queues.getStack(engine, getDefaultSerializer(),recid);
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }

    synchronized public <E> Queue<E> getCircularQueue(String name) {
        Long recid = nameDir.get(name);
        if(recid == null){
            recid = Queues.createCircularQueue(engine, getDefaultSerializer(), getDefaultSerializer(), 1000000);
            nameDir.put(name,recid);
        }
        Queues.CircularQueue<E> ret = Queues.getCircularQueue(engine,getDefaultSerializer(),recid);
        collections.put(name, new WeakReference<Object>(ret));
        return ret;

    }

    synchronized public <E> Queue<E> createQueue(String name, Serializer<E> serializer) {
        checkNameNotExists(name);
        if(serializer==null) serializer=getDefaultSerializer();
        Long recid = Queues.createQueue(engine, getDefaultSerializer(), serializer);
        nameDir.put(name,recid);
        return getQueue(name);
    }

    synchronized public <E> Queue<E> createStack(String name, Serializer<E> serializer, boolean useLocks) {
        checkNameNotExists(name);
        if(serializer==null) serializer=getDefaultSerializer();
        Long recid = Queues.createStack(engine, getDefaultSerializer(), serializer, useLocks);
        nameDir.put(name,recid);
        return getStack(name);
    }

    synchronized public <E> Queue<E> createCircularQueue(String name, Serializer<E> serializer, long size) {
        checkNameNotExists(name);
        if(serializer==null) serializer=getDefaultSerializer();
        Long recid = Queues.createCircularQueue(engine, getDefaultSerializer(), serializer, size);
        nameDir.put(name,recid);
        return getCircularQueue(name);
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
     * Perform storage maintenance.
     * Typically compact underlying storage and reclaim unused space.
     * <p/>
     * NOTE: MapDB does not have smart defragmentation algorithms. So compaction usually recreates entire
     * store from scratch. This may require additional disk space.
     */
    synchronized public void compact(){
        engine.compact();
    }


    /**
     * Make readonly snapshot view of DB and all of its collection
     * Collections loaded by this instance are not affected (are still mutable).
     * You have to load new collections from DB returned by this method
     *
     * @return readonly snapshot view
     */
    synchronized public DB snapshot(){
        Engine snapshot = SnapshotEngine.createSnapshotFor(engine);
        return new DB (snapshot);
    }

    /**
     * @return default serializer used in this DB, it handles POJO and other stuff.
     */
    public  Serializer getDefaultSerializer() {
        return defaultSerializer;
    }

    /**
     * @return underlying engine which takes care of persistence for this DB.
     */
    public Engine getEngine() {
        return engine;
    }



}
