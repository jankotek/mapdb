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

import java.io.Closeable;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A database with easy access to named maps and other collections.
 *
 * @author Jan Kotek
 */
//TODO DB uses global lock, replace it with ReadWrite lock or fine grained locking.
@SuppressWarnings("unchecked")
public class DB implements Closeable {

    protected final boolean strictDBGet;
    protected final boolean deleteFilesAfterClose;

    /** Engine which provides persistence for this DB*/
    protected Engine engine;
    /** already loaded named collections. It is important to keep collections as singletons, because of 'in-memory' locking*/
    protected Map<String, WeakReference<?>> namesInstanciated = new HashMap<String, WeakReference<?>>();

    protected Map<IdentityWrapper, String> namesLookup =
            Collections.synchronizedMap( //TODO remove synchronized map, after DB locking is resolved
            new HashMap<IdentityWrapper, String>());

    /** view over named records */
    protected SortedMap<String, Object> catalog;

    protected final Fun.ThreadFactory threadFactory = Fun.ThreadFactory.BASIC;
    protected SerializerPojo serializerPojo;

    protected final Set<String> unknownClasses = new ConcurrentSkipListSet<String>();

    protected static class IdentityWrapper{

        final Object o;

        public IdentityWrapper(Object o) {
            this.o = o;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(o);
        }

        @Override
        public boolean equals(Object v) {
            return ((IdentityWrapper)v).o==o;
        }
    }

    /**
     * Construct new DB. It is just thin layer over {@link Engine} which does the real work.
     * @param engine
     */
    public DB(final Engine engine){
        this(engine,false,false);
    }

    public DB(Engine engine, boolean strictDBGet, boolean deleteFilesAfterClose) {
        //TODO investigate dereference and how non-final field affect performance. Perhaps abandon dereference completely
//        if(!(engine instanceof EngineWrapper)){
//            //access to Store should be prevented after `close()` was called.
//            //So for this we have to wrap raw Store into EngineWrapper
//            engine = new EngineWrapper(engine);
//        }
        this.engine = engine;
        this.strictDBGet = strictDBGet;
        this.deleteFilesAfterClose = deleteFilesAfterClose;

        serializerPojo = new SerializerPojo(
                //get name for given object
                new Fun.Function1<String, Object>() {
                    @Override public String run(Object o) {
                        return getNameForObject(o);
                    }
                },
                //get object with given name
                new Fun.Function1<Object, String>() {
                    @Override public Object run(String name) {
                        return get(name);
                    }
                },
                //load class catalog
                new Fun.Function0<SerializerPojo.ClassInfo[]>() {
                    @Override public SerializerPojo.ClassInfo[] run() {
                        SerializerPojo.ClassInfo[] ret =  getEngine().get(Engine.RECID_CLASS_CATALOG, SerializerPojo.CLASS_CATALOG_SERIALIZER);
                        if(ret==null)
                            ret = new SerializerPojo.ClassInfo[0];
                        return ret;
                    }
                },
                //notify DB than given class is missing in catalog and should be added on next commit.
                new Fun.Function1<Void, String>() {
                    @Override public Void run(String className) {
                        unknownClasses.add(className);
                        return null;
                    }
                },
                engine);
        reinit();
    }

    protected void reinit() {
        //open name dir
        //$DELAY$
        catalog = BTreeMap.preinitCatalog(this);
    }

    public <A> A catGet(String name, A init){
        if(CC.PARANOID && ! (Thread.holdsLock(DB.this)))
            throw new AssertionError();
        A ret = (A) catalog.get(name);
        return ret!=null? ret : init;
    }


    public <A> A catGet(String name){
        if(CC.PARANOID && ! (Thread.holdsLock(DB.this)))
            throw new AssertionError();
        //$DELAY$
        return (A) catalog.get(name);
    }

    public <A> A catPut(String name, A value){
        if(CC.PARANOID && ! (Thread.holdsLock(DB.this)))
            throw new AssertionError();
        //$DELAY$
        catalog.put(name, value);
        return value;
    }

    public <A> A catPut(String name, A value, A retValueIfNull){
        if(CC.PARANOID && ! (Thread.holdsLock(DB.this)))
            throw new AssertionError();
        if(value==null) return retValueIfNull;
        //$DELAY$
        catalog.put(name, value);
        return value;
    }

    /** returns name for this object, if it has name and was instanciated by this DB*/
    public synchronized  String getNameForObject(Object obj) {
        return namesLookup.get(new IdentityWrapper(obj));
    }


    public class HTreeMapMaker{
        protected final String name;

        public HTreeMapMaker(String name) {
            this.name = name;
        }


        protected boolean counter = false;
        protected Serializer<?> keySerializer = null;
        protected Serializer<?> valueSerializer = null;
        protected long expireMaxSize = 0L;
        protected long expire = 0L;
        protected long expireAccess = 0L;
        protected long expireStoreSize;

        protected Fun.Function1<?,?> valueCreator = null;

        protected Iterator pumpSource;
        protected Fun.Function1 pumpKeyExtractor;
        protected Fun.Function1 pumpValueExtractor;
        protected int pumpPresortBatchSize = (int) 1e7;
        protected boolean pumpIgnoreDuplicates = false;



        /** by default collection does not have counter, without counter updates are faster, but entire collection needs to be traversed to count items.*/
        public HTreeMapMaker counterEnable(){
            this.counter = true;
            return this;
        }



        /** keySerializer used to convert keys into/from binary form. */
        public HTreeMapMaker keySerializer(Serializer<?> keySerializer){
            this.keySerializer = keySerializer;
            return this;
        }

        /** valueSerializer used to convert values into/from binary form. */
        public HTreeMapMaker valueSerializer(Serializer<?> valueSerializer){
            this.valueSerializer = valueSerializer;
            return this;
        }

        /** maximal number of entries in this map. Less used entries will be expired and removed to make collection smaller  */
        public HTreeMapMaker expireMaxSize(long maxSize){
            this.expireMaxSize = maxSize;
            this.counter = true;
            return this;
        }

        /** Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, or the most recent replacement of its value.  */
        public HTreeMapMaker expireAfterWrite(long interval, TimeUnit timeUnit){
            this.expire = timeUnit.toMillis(interval);
            return this;
        }

        /** Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, or the most recent replacement of its value.  */
        public HTreeMapMaker expireAfterWrite(long interval){
            this.expire = interval;
            return this;
        }

        /** Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, the most recent replacement of its value, or its last access. Access time is reset by all map read and write operations  */
        public HTreeMapMaker expireAfterAccess(long interval, TimeUnit timeUnit){
            this.expireAccess = timeUnit.toMillis(interval);
            return this;
        }

        /** Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, the most recent replacement of its value, or its last access. Access time is reset by all map read and write operations  */
        public HTreeMapMaker expireAfterAccess(long interval){
            this.expireAccess = interval;
            return this;
        }

        public HTreeMapMaker expireStoreSize(double maxStoreSize) {
            this.expireStoreSize = (long) (maxStoreSize*1024L*1024L*1024L);
            return this;
        }

        /** If value is not found, HTreeMap can fetch and insert default value. `valueCreator` is used to return new value.
         * This way `HTreeMap.get()` never returns null */
        public HTreeMapMaker valueCreator(Fun.Function1<?,?> valueCreator){
            this.valueCreator = valueCreator;
            return this;
        }

        public <K,V> HTreeMapMaker pumpSource(Iterator<K> keysSource,  Fun.Function1<V,K> valueExtractor){
            this.pumpSource = keysSource;
            this.pumpKeyExtractor = Fun.extractNoTransform();
            this.pumpValueExtractor = valueExtractor;
            return this;
        }


        public <K,V> HTreeMapMaker pumpSource(Iterator<Fun.Pair<K,V>> entriesSource){
            this.pumpSource = entriesSource;
            this.pumpKeyExtractor = Fun.extractKey();
            this.pumpValueExtractor = Fun.extractValue();
            return this;
        }

        public HTreeMapMaker pumpPresort(int batchSize){
            this.pumpPresortBatchSize = batchSize;
            return this;
        }



        /**
         * If source iteretor contains an duplicate key, exception is thrown.
         * This options will only use firts key and ignore any consequentive duplicates.
         */
        public <K> HTreeMapMaker pumpIgnoreDuplicates(){
            this.pumpIgnoreDuplicates = true;
            return this;
        }



        public <K,V> HTreeMap<K,V> make(){
            if(expireMaxSize!=0) counter =true;
            return DB.this.createHashMap(HTreeMapMaker.this);
        }

        public <K,V> HTreeMap<K,V> makeOrGet(){
            //$DELAY$
            synchronized (DB.this){
                //TODO add parameter check
                //$DELAY$
                return (HTreeMap<K, V>) (catGet(name+".type")==null?
                                    make():getHashMap(name));
            }
        }


    }

    public class HTreeSetMaker{
        protected final String name;

        public HTreeSetMaker(String name) {
            this.name = name;
        }

        protected boolean counter = false;
        protected Serializer<?> serializer = null;
        protected long expireMaxSize = 0L;
        protected long expireStoreSize = 0L;
        protected long expire = 0L;
        protected long expireAccess = 0L;

        protected Iterator<?> pumpSource;
        protected int pumpPresortBatchSize = (int) 1e7;
        protected boolean pumpIgnoreDuplicates = false;


        /** by default collection does not have counter, without counter updates are faster, but entire collection needs to be traversed to count items.*/
        public HTreeSetMaker counterEnable(){
            this.counter = true;
            return this;
        }


        /** keySerializer used to convert keys into/from binary form. */
        public HTreeSetMaker serializer(Serializer<?> serializer){
            this.serializer = serializer;
            return this;
        }


        /** maximal number of entries in this map. Less used entries will be expired and removed to make collection smaller  */
        public HTreeSetMaker expireMaxSize(long maxSize){
            this.expireMaxSize = maxSize;
            this.counter = true;
            return this;
        }

        /** maximal size of store in GB, if store is larger entries will start expiring */
        public HTreeSetMaker expireStoreSize(double maxStoreSize){
            this.expireStoreSize = (long) (maxStoreSize * 1024L * 1024L * 1024L);
            return this;
        }

        /** Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, or the most recent replacement of its value.  */
        public HTreeSetMaker expireAfterWrite(long interval, TimeUnit timeUnit){
            this.expire = timeUnit.toMillis(interval);
            return this;
        }

        /** Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, or the most recent replacement of its value.  */
        public HTreeSetMaker expireAfterWrite(long interval){
            this.expire = interval;
            return this;
        }

        /** Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, the most recent replacement of its value, or its last access. Access time is reset by all map read and write operations  */
        public HTreeSetMaker expireAfterAccess(long interval, TimeUnit timeUnit){
            this.expireAccess = timeUnit.toMillis(interval);
            return this;
        }

        /** Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, the most recent replacement of its value, or its last access. Access time is reset by all map read and write operations  */
        public HTreeSetMaker expireAfterAccess(long interval){
            this.expireAccess = interval;
            return this;
        }


        public HTreeSetMaker pumpSource(Iterator<?> source){
            this.pumpSource = source;
            return this;
        }

        /**
         * If source iteretor contains an duplicate key, exception is thrown.
         * This options will only use firts key and ignore any consequentive duplicates.
         */
        public HTreeSetMaker pumpIgnoreDuplicates(){
            this.pumpIgnoreDuplicates = true;
            return this;
        }

        public HTreeSetMaker pumpPresort(int batchSize){
            this.pumpPresortBatchSize = batchSize;
            return this;
        }



        public <K> Set<K> make(){
            if(expireMaxSize!=0) counter =true;
            return DB.this.createHashSet(HTreeSetMaker.this);
        }

        public <K> Set<K> makeOrGet(){
            synchronized (DB.this){
                //$DELAY$
                //TODO add parameter check
                return (Set<K>) (catGet(name+".type")==null?
                        make():getHashSet(name));
            }
        }

    }



    /**
     * Opens existing or creates new Hash Tree Map.
     * This collection perform well under concurrent access.
     * Is best for large keys and large values.
     *
     * @param name of the map
     * @return map
     */
    synchronized public <K,V> HTreeMap<K,V> getHashMap(String name){
        return getHashMap(name, null);
    }

    /**
     * Opens existing or creates new Hash Tree Map.
     * This collection perform well under concurrent access.
     * Is best for large keys and large values.
     *
     * @param name of map
     * @param valueCreator if value is not found, new is created and placed into map.
     * @return map
     */
    synchronized public <K,V> HTreeMap<K,V> getHashMap(String name, Fun.Function1<V,K> valueCreator){
        checkNotClosed();
        HTreeMap<K,V> ret = (HTreeMap<K, V>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        //$DELAY$
        if(type==null){
            //$DELAY$
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0);
                //$DELAY$
                new DB(e).getHashMap("a");
                return namedPut(name,
                        new DB(new Engine.ReadOnly(e)).getHashMap("a"));
            }
            if(valueCreator!=null)
                return createHashMap(name).valueCreator(valueCreator).make();
            return createHashMap(name).make();
        }


        //check type
        checkType(type, "HashMap");
        //open existing map
        //$DELAY$
        ret = new HTreeMap<K,V>(engine,
                (Long)catGet(name+".counterRecid"),
                (Integer)catGet(name+".hashSalt"),
                (long[])catGet(name+".segmentRecids"),
                catGet(name+".keySerializer",getDefaultSerializer()),
                catGet(name+".valueSerializer",getDefaultSerializer()),
                catGet(name+".expireTimeStart",0L),
                catGet(name+".expire",0L),
                catGet(name+".expireAccess",0L),
                catGet(name+".expireMaxSize",0L),
                catGet(name+".expireStoreSize",0L),
                (long[])catGet(name+".expireHeads",null),
                (long[])catGet(name+".expireTails",null),
                valueCreator,
                threadFactory);

        //$DELAY$
        namedPut(name, ret);
        //$DELAY$
        return ret;
    }

    public  <V> V namedPut(String name, Object ret) {
        //$DELAY$
        namesInstanciated.put(name, new WeakReference<Object>(ret));
        //$DELAY$
        namesLookup.put(new IdentityWrapper(ret), name);
        return (V) ret;
    }


    /**
     * Returns new builder for HashMap with given name
     *
     * @param name of map to create
     * @throws IllegalArgumentException if name is already used
     * @return maker, call `.make()` to create map
     */
    public HTreeMapMaker createHashMap(String name){
        return new HTreeMapMaker(name);
    }



    /**
     * Creates new HashMap with more specific arguments
     *
     * @throws IllegalArgumentException if name is already used
     * @return newly created map
     */
    synchronized protected <K,V> HTreeMap<K,V> createHashMap(HTreeMapMaker m){
        String name = m.name;
        checkNameNotExists(name);
        //$DELAY$
        long expireTimeStart=0, expire=0, expireAccess=0, expireMaxSize = 0, expireStoreSize=0;
        long[] expireHeads=null, expireTails=null;

        if(m.expire!=0 || m.expireAccess!=0 || m.expireMaxSize !=0 || m.expireStoreSize!=0){
            expireTimeStart = catPut(name+".expireTimeStart",System.currentTimeMillis());
            expire = catPut(name+".expire",m.expire);
            expireAccess = catPut(name+".expireAccess",m.expireAccess);
            expireMaxSize = catPut(name+".expireMaxSize",m.expireMaxSize);
            expireStoreSize = catPut(name+".expireStoreSize",m.expireStoreSize);
            //$DELAY$
            expireHeads = new long[16];
            expireTails = new long[16];
            for(int i=0;i<16;i++){
                expireHeads[i] = engine.put(0L,Serializer.LONG);
                expireTails[i] = engine.put(0L,Serializer.LONG);
            }
            catPut(name+".expireHeads",expireHeads);
            catPut(name+".expireTails",expireTails);
        }
        //$DELAY$


        HTreeMap<K,V> ret = new HTreeMap<K,V>(engine,
                catPut(name+".counterRecid",!m.counter ?0L:engine.put(0L, Serializer.LONG)),
                catPut(name+".hashSalt",new Random().nextInt()),
                catPut(name+".segmentRecids",HTreeMap.preallocateSegments(engine)),
                catPut(name+".keySerializer",m.keySerializer,getDefaultSerializer()),
                catPut(name+".valueSerializer",m.valueSerializer,getDefaultSerializer()),
                expireTimeStart,expire,expireAccess,expireMaxSize, expireStoreSize, expireHeads ,expireTails,
                (Fun.Function1<V, K>) m.valueCreator,
                threadFactory

        );
        //$DELAY$
        catalog.put(name + ".type", "HashMap");
        namedPut(name, ret);


        //pump data if specified2
        if(m.pumpSource!=null) {
            Pump.fillHTreeMap(ret, m.pumpSource,
                    m.pumpKeyExtractor,m.pumpValueExtractor,
                    m.pumpPresortBatchSize, m.pumpIgnoreDuplicates,
                    getDefaultSerializer());
        }

        return ret;
    }

    /**
     *  Opens existing or creates new Hash Tree Set.
     *
     * @param name of the Set
     * @return set
     */
    synchronized public <K> Set<K> getHashSet(String name){
        checkNotClosed();
        Set<K> ret = (Set<K>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        //$DELAY$
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0);
                //$DELAY$
                new DB(e).getHashSet("a");
                return namedPut(name,
                        new DB(new Engine.ReadOnly(e)).getHashSet("a"));
            }
            return createHashSet(name).makeOrGet();
            //$DELAY$
        }


        //check type
        checkType(type, "HashSet");
        //open existing map
        ret = new HTreeMap<K, Object>(engine,
                (Long)catGet(name+".counterRecid"),
                (Integer)catGet(name+".hashSalt"),
                (long[])catGet(name+".segmentRecids"),
                catGet(name+".serializer",getDefaultSerializer()),
                null,
                catGet(name+".expireTimeStart",0L),
                catGet(name+".expire",0L),
                catGet(name+".expireAccess",0L),
                catGet(name+".expireMaxSize",0L),
                catGet(name+".expireStoreSize",0L),
                (long[])catGet(name+".expireHeads",null),
                (long[])catGet(name+".expireTails",null),
                null,
                threadFactory
         ).keySet();

        //$DELAY$
        namedPut(name, ret);
        //$DELAY$
        return ret;
    }

    /**
     * Creates new HashSet
     *
     * @param name of set to create
     */
    synchronized public HTreeSetMaker createHashSet(String name){
        return new HTreeSetMaker(name);
    }


    synchronized protected <K> Set<K> createHashSet(HTreeSetMaker m){
        String name = m.name;
        checkNameNotExists(name);

        long expireTimeStart=0, expire=0, expireAccess=0, expireMaxSize = 0, expireStoreSize = 0;
        long[] expireHeads=null, expireTails=null;

        if(m.expire!=0 || m.expireAccess!=0 || m.expireMaxSize !=0){
            expireTimeStart = catPut(name+".expireTimeStart",System.currentTimeMillis());
            expire = catPut(name+".expire",m.expire);
            expireAccess = catPut(name+".expireAccess",m.expireAccess);
            expireMaxSize = catPut(name+".expireMaxSize",m.expireMaxSize);
            expireStoreSize = catPut(name+".expireStoreSize",m.expireStoreSize);
            expireHeads = new long[16];
            //$DELAY$
            expireTails = new long[16];
            for(int i=0;i<16;i++){
                expireHeads[i] = engine.put(0L,Serializer.LONG);
                expireTails[i] = engine.put(0L,Serializer.LONG);
            }
            catPut(name+".expireHeads",expireHeads);
            catPut(name+".expireTails",expireTails);
        }

        //$DELAY$
        HTreeMap<K,Object> ret = new HTreeMap<K,Object>(engine,
                catPut(name+".counterRecid",!m.counter ?0L:engine.put(0L, Serializer.LONG)),
                catPut(name+".hashSalt",new Random().nextInt()),
                catPut(name+".segmentRecids",HTreeMap.preallocateSegments(engine)),
                catPut(name+".serializer",m.serializer,getDefaultSerializer()),
                null,
                expireTimeStart,expire,expireAccess,expireMaxSize, expireStoreSize, expireHeads ,expireTails,
                null,
                threadFactory
        );
        Set<K> ret2 = ret.keySet();
        //$DELAY$
        catalog.put(name + ".type", "HashSet");
        namedPut(name, ret2);
        //$DELAY$


        //pump data if specified2
        if(m.pumpSource!=null) {
            Pump.fillHTreeMap(ret, m.pumpSource,
                    (Fun.Function1)Fun.extractNoTransform(),null,
                    m.pumpPresortBatchSize, m.pumpIgnoreDuplicates,
                    getDefaultSerializer());
        }

        return ret2;
    }



    public class BTreeMapMaker{
        protected final String name;

        public BTreeMapMaker(String name) {
            this.name = name;
        }

        protected int nodeSize = 32;
        protected boolean valuesOutsideNodes = false;
        protected boolean counter = false;
        protected BTreeKeySerializer keySerializer;
        protected Serializer valueSerializer;
        protected Comparator comparator;

        protected Iterator pumpSource;
        protected Fun.Function1 pumpKeyExtractor;
        protected Fun.Function1 pumpValueExtractor;
        protected int pumpPresortBatchSize = -1;
        protected boolean pumpIgnoreDuplicates = false;


        /** nodeSize maximal size of node, larger node causes overflow and creation of new BTree node. Use large number for small keys, use small number for large keys.*/
        public BTreeMapMaker nodeSize(int nodeSize){
            if(nodeSize>=BTreeMap.NodeSerializer.SIZE_MASK)
                throw new IllegalArgumentException("Too large max node size");
            this.nodeSize = nodeSize;
            return this;
        }

        /** by default values are stored inside BTree Nodes. Large values should be stored outside of BTreeNodes*/
        public BTreeMapMaker valuesOutsideNodesEnable(){
            this.valuesOutsideNodes = true;
            return this;
        }

        /** by default collection does not have counter, without counter updates are faster, but entire collection needs to be traversed to count items.*/
        public BTreeMapMaker counterEnable(){
            this.counter = true;
            return this;
        }

        /** keySerializer used to convert keys into/from binary form. */
        public BTreeMapMaker keySerializer(BTreeKeySerializer keySerializer){
            this.keySerializer = keySerializer;
            return this;
        }
        /** keySerializer used to convert keys into/from binary form.
         * This wraps ordinary serializer, with no delta packing used*/
        public BTreeMapMaker keySerializer(Serializer serializer){
            this.keySerializer = new BTreeKeySerializer.BasicKeySerializer(serializer, comparator);
            return this;
        }

        /** valueSerializer used to convert values into/from binary form. */
        public BTreeMapMaker valueSerializer(Serializer<?> valueSerializer){
            this.valueSerializer = valueSerializer;
            return this;
        }

        /** comparator used to sort keys.  */
        public BTreeMapMaker comparator(Comparator<?> comparator){
            this.comparator = comparator;
            return this;
        }

        public <K,V> BTreeMapMaker pumpSource(Iterator<K> keysSource,  Fun.Function1<V,K> valueExtractor){
            this.pumpSource = keysSource;
            this.pumpKeyExtractor = Fun.extractNoTransform();
            this.pumpValueExtractor = valueExtractor;
            return this;
        }


        public <K,V> BTreeMapMaker pumpSource(Iterator<Fun.Pair<K,V>> entriesSource){
            this.pumpSource = entriesSource;
            this.pumpKeyExtractor = Fun.extractKey();
            this.pumpValueExtractor = Fun.extractValue();
            return this;
        }

        public BTreeMapMaker pumpPresort(int batchSize){
            this.pumpPresortBatchSize = batchSize;
            return this;
        }


        /**
         * If source iteretor contains an duplicate key, exception is thrown.
         * This options will only use firts key and ignore any consequentive duplicates.
         */
        public <K> BTreeMapMaker pumpIgnoreDuplicates(){
            this.pumpIgnoreDuplicates = true;
            return this;
        }

        public <K,V> BTreeMap<K,V> make(){
            return DB.this.createTreeMap(BTreeMapMaker.this);
        }

        public <K,V> BTreeMap<K,V> makeOrGet(){
            synchronized(DB.this){
                //TODO add parameter check
                return (BTreeMap<K, V>) (catGet(name+".type")==null?
                        make():getTreeMap(name));
            }
        }


        /** creates map optimized for using `String` keys */
        public <V> BTreeMap<String, V> makeStringMap() {
            keySerializer = BTreeKeySerializer.STRING;
            return make();
        }

        /** creates map optimized for using zero or positive `Long` keys */
        public <V> BTreeMap<Long, V> makeLongMap() {
            keySerializer = BTreeKeySerializer.ZERO_OR_POSITIVE_LONG;
            return make();
        }

    }

    public class BTreeSetMaker{
        protected final String name;

        public BTreeSetMaker(String name) {
            this.name = name;
        }

        protected int nodeSize = 32;
        protected boolean counter = false;
        protected BTreeKeySerializer serializer;
        protected Comparator<?> comparator;

        protected Iterator<?> pumpSource;
        protected int pumpPresortBatchSize = -1;
        protected boolean pumpIgnoreDuplicates = false;

        /** nodeSize maximal size of node, larger node causes overflow and creation of new BTree node. Use large number for small keys, use small number for large keys.*/
        public BTreeSetMaker nodeSize(int nodeSize){
            this.nodeSize = nodeSize;
            return this;
        }


        /** by default collection does not have counter, without counter updates are faster, but entire collection needs to be traversed to count items.*/
        public BTreeSetMaker counterEnable(){
            this.counter = true;
            return this;
        }

        /** keySerializer used to convert keys into/from binary form. */
        public BTreeSetMaker serializer(BTreeKeySerializer serializer){
            this.serializer = serializer;
            return this;
        }

        /** comparator used to sort keys.  */
        public BTreeSetMaker comparator(Comparator<?> comparator){
            this.comparator = comparator;
            return this;
        }

        public BTreeSetMaker pumpSource(Iterator<?> source){
            this.pumpSource = source;
            return this;
        }

        /**
         * If source iteretor contains an duplicate key, exception is thrown.
         * This options will only use firts key and ignore any consequentive duplicates.
         */
        public <K> BTreeSetMaker pumpIgnoreDuplicates(){
            this.pumpIgnoreDuplicates = true;
            return this;
        }

        public BTreeSetMaker pumpPresort(int batchSize){
            this.pumpPresortBatchSize = batchSize;
            return this;
        }


        public <K> NavigableSet<K> make(){
            return DB.this.createTreeSet(BTreeSetMaker.this);
        }

        public <K> NavigableSet<K> makeOrGet(){
            synchronized (DB.this){
                //TODO add parameter check
                return (NavigableSet<K>) (catGet(name+".type")==null?
                    make():getTreeSet(name));
            }
        }




        /** creates set optimized for using `String` */
        public NavigableSet<String> makeStringSet() {
            serializer = BTreeKeySerializer.STRING;
            return make();
        }

        /** creates set optimized for using zero or positive `Long` */
        public NavigableSet<Long> makeLongSet() {
            serializer = BTreeKeySerializer.ZERO_OR_POSITIVE_LONG;
            return make();
        }

    }


    /**
     * Opens existing or creates new B-linked-tree Map.
     * This collection performs well under concurrent access.
     * Only trade-off are deletes, which causes tree fragmentation.
     * It is ordered and best suited for small keys and values.
     *
     * @param name of map
     * @return map
     */
    synchronized public <K,V> BTreeMap<K,V> getTreeMap(String name){
        checkNotClosed();
        BTreeMap<K,V> ret = (BTreeMap<K,V>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        //$DELAY$
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0);
                new DB(e).getTreeMap("a");
                //$DELAY$
                return namedPut(name,
                        new DB(new Engine.ReadOnly(e)).getTreeMap("a"));
            }
            return createTreeMap(name).make();

        }
        checkType(type, "TreeMap");

        ret = new BTreeMap<K, V>(engine,
                (Long) catGet(name + ".rootRecidRef"),
                catGet(name+".maxNodeSize",32),
                catGet(name+".valuesOutsideNodes",false),
                catGet(name+".counterRecid",0L),
                catGet(name+".keySerializer",new BTreeKeySerializer.BasicKeySerializer(getDefaultSerializer(),Fun.COMPARATOR)),
                catGet(name+".valueSerializer",getDefaultSerializer()),
                catGet(name+".numberOfNodeMetas",0)
                );
        //$DELAY$
        namedPut(name, ret);
        return ret;
    }

    /**
     * Returns new builder for TreeMap with given name
     *
     * @param name of map to create
     * @throws IllegalArgumentException if name is already used
     * @return maker, call `.make()` to create map
     */
    public BTreeMapMaker createTreeMap(String name){
        return new BTreeMapMaker(name);
    }


    synchronized protected <K,V> BTreeMap<K,V> createTreeMap(final BTreeMapMaker m){
        String name = m.name;
        checkNameNotExists(name);
        //$DELAY$
        if(m.comparator==null){
            m.comparator = Fun.COMPARATOR;
        }

        m.keySerializer = fillNulls(m.keySerializer);
        m.keySerializer = catPut(name+".keySerializer",m.keySerializer,new BTreeKeySerializer.BasicKeySerializer(getDefaultSerializer(),m.comparator));
        m.valueSerializer = catPut(name+".valueSerializer",m.valueSerializer,getDefaultSerializer());

        if(m.pumpPresortBatchSize!=-1 && m.pumpSource!=null){
            Comparator presortComp =  new Comparator() {

                @Override
                public int compare(Object o1, Object o2) {
                    return - m.comparator.compare(m.pumpKeyExtractor.run(o1), m.pumpKeyExtractor.run(o2));
                }
            };

            m.pumpSource = Pump.sort(m.pumpSource,m.pumpIgnoreDuplicates, m.pumpPresortBatchSize,
                    presortComp,getDefaultSerializer());
        }
        //$DELAY$
        long counterRecid = !m.counter ?0L:engine.put(0L, Serializer.LONG);

        long rootRecidRef;
        if(m.pumpSource==null){
            rootRecidRef = BTreeMap.createRootRef(engine,m.keySerializer,m.valueSerializer,0);
        }else{
            rootRecidRef = Pump.buildTreeMap(
                    (Iterator<K>)m.pumpSource,
                    engine,
                    (Fun.Function1<K,K>)m.pumpKeyExtractor,
                    (Fun.Function1<V,K>)m.pumpValueExtractor,
                    m.pumpIgnoreDuplicates,m.nodeSize,
                    m.valuesOutsideNodes,
                    counterRecid,
                    m.keySerializer,
                    (Serializer<V>)m.valueSerializer);
        }
        //$DELAY$
        BTreeMap<K,V> ret = new BTreeMap<K,V>(engine,
                catPut(name+".rootRecidRef", rootRecidRef),
                catPut(name+".maxNodeSize",m.nodeSize),
                catPut(name+".valuesOutsideNodes",m.valuesOutsideNodes),
                catPut(name+".counterRecid",counterRecid),
                m.keySerializer,
                (Serializer<V>)m.valueSerializer,
                catPut(m.name+".numberOfNodeMetas",0)
        );
        //$DELAY$
        catalog.put(name + ".type", "TreeMap");
        namedPut(name, ret);
        return ret;
    }

    /**
     * Replace nulls in tuple serializers with default (Comparable) values
     *
     * @param keySerializer with nulls
     * @return keySerializers which does not contain any nulls
     */
    protected BTreeKeySerializer fillNulls(BTreeKeySerializer keySerializer) {
        if(keySerializer==null)
            return null;
        if(keySerializer instanceof BTreeKeySerializer.ArrayKeySerializer) {
            BTreeKeySerializer.ArrayKeySerializer k = (BTreeKeySerializer.ArrayKeySerializer) keySerializer;

            Serializer[] serializers = new Serializer[k.tsize];
            Comparator[] comparators = new Comparator[k.tsize];
            //$DELAY$
            for (int i = 0; i < k.tsize; i++) {
                serializers[i] = k.serializers[i] != null && k.serializers[i]!=Serializer.BASIC ? k.serializers[i] : getDefaultSerializer();
                comparators[i] = k.comparators[i] != null ? k.comparators[i] : Fun.COMPARATOR;
            }
            //$DELAY$
            return new BTreeKeySerializer.ArrayKeySerializer(comparators, serializers);
        }
        //$DELAY$
        return keySerializer;
    }


    /**
     * Get Name Catalog.
     * It is metatable which contains information about named collections and records.
     * Each collection constructor takes number of parameters, this map contains those parameters.
     *
     * _Note:_ Do not modify this map, unless you know what you are doing!
     *
     * @return Name Catalog
     */
    public SortedMap<String, Object> getCatalog(){
        return catalog;
    }


    /**
     * Opens existing or creates new B-linked-tree Set.
     *
     * @param name of set
     * @return set
     */
    synchronized public <K> NavigableSet<K> getTreeSet(String name){
        checkNotClosed();
        NavigableSet<K> ret = (NavigableSet<K>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0);
                new DB(e).getTreeSet("a");
                return namedPut(name,
                        new DB(new Engine.ReadOnly(e)).getTreeSet("a"));
            }
            //$DELAY$
            return createTreeSet(name).make();

        }
        checkType(type, "TreeSet");
        //$DELAY$
        ret = new BTreeMap<K, Object>(engine,
                (Long) catGet(name+".rootRecidRef"),
                catGet(name+".maxNodeSize",32),
                false,
                catGet(name+".counterRecid",0L),
                catGet(name+".keySerializer",new BTreeKeySerializer.BasicKeySerializer(getDefaultSerializer(),Fun.COMPARATOR)),
                null,
                catGet(name+".numberOfNodeMetas",0)
        ).keySet();
        //$DELAY$
        namedPut(name, ret);
        return ret;

    }

    /**
     * Creates new TreeSet.
     * @param name of set to create
     * @throws IllegalArgumentException if name is already used
     * @return maker used to construct set
     */
    synchronized public BTreeSetMaker createTreeSet(String name){
         return new BTreeSetMaker(name);
    }

    synchronized public <K> NavigableSet<K> createTreeSet(BTreeSetMaker m){
        checkNameNotExists(m.name);
        if(m.comparator==null){
            m.comparator = Fun.COMPARATOR;
        }
        //$DELAY$
        m.serializer = fillNulls(m.serializer);
        m.serializer = catPut(m.name+".keySerializer",m.serializer,new BTreeKeySerializer.BasicKeySerializer(getDefaultSerializer(),m.comparator));

        if(m.pumpPresortBatchSize!=-1){
            m.pumpSource = Pump.sort(m.pumpSource,m.pumpIgnoreDuplicates, m.pumpPresortBatchSize,Collections.reverseOrder(m.comparator),getDefaultSerializer());
        }

        long counterRecid = !m.counter ?0L:engine.put(0L, Serializer.LONG);
        long rootRecidRef;
        //$DELAY$
        if(m.pumpSource==null){
            rootRecidRef = BTreeMap.createRootRef(engine,m.serializer,null,0);
        }else{
            rootRecidRef = Pump.buildTreeMap(
                    (Iterator<Object>)m.pumpSource,
                    engine,
                    Fun.extractNoTransform(),
                    null,
                    m.pumpIgnoreDuplicates,
                    m.nodeSize,
                    false,
                    counterRecid,
                    m.serializer,
                    null);
        }
        //$DELAY$
        NavigableSet<K> ret = new BTreeMap<K,Object>(
                engine,
                catPut(m.name+".rootRecidRef", rootRecidRef),
                catPut(m.name+".maxNodeSize",m.nodeSize),
                false,
                catPut(m.name+".counterRecid",counterRecid),
                m.serializer,
                null,
                catPut(m.name+".numberOfNodeMetas",0)
        ).keySet();
        //$DELAY$
        catalog.put(m.name + ".type", "TreeSet");
        namedPut(m.name, ret);
        return ret;
    }

    synchronized public <E> BlockingQueue<E> getQueue(String name) {
        checkNotClosed();
        Queues.Queue<E> ret = (Queues.Queue<E>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        //$DELAY$
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0);
                new DB(e).getQueue("a");
                return namedPut(name,
                        new DB(new Engine.ReadOnly(e)).getQueue("a"));
            }
            //$DELAY$
            return createQueue(name,null,true);
        }
        checkType(type, "Queue");
        //$DELAY$
        ret = new Queues.Queue<E>(engine,
                (Serializer<E>) catGet(name+".serializer",getDefaultSerializer()),
                (Long) catGet(name+".headRecid"),
                (Long)catGet(name+".tailRecid"),
                (Boolean)catGet(name+".useLocks")
                );
        //$DELAY$
        namedPut(name, ret);
        return ret;
    }

    synchronized public <E> BlockingQueue<E> createQueue(String name, Serializer<E> serializer, boolean useLocks) {
        checkNameNotExists(name);

        long node = engine.preallocate(); //serializer is new Queues.SimpleQueue.NodeSerializer(serializer)
        long headRecid = engine.put(node, Serializer.LONG);
        long tailRecid = engine.put(node, Serializer.LONG);
        //$DELAY$
        Queues.Queue<E> ret = new Queues.Queue<E>(engine,
                catPut(name+".serializer",serializer,getDefaultSerializer()),
                catPut(name +".headRecid",headRecid),
                catPut(name+".tailRecid",tailRecid),
                catPut(name+".useLocks",useLocks)
                );
        catalog.put(name + ".type", "Queue");
        //$DELAY$
        namedPut(name, ret);
        return ret;

    }


    synchronized public <E> BlockingQueue<E> getStack(String name) {
        checkNotClosed();
        Queues.Stack<E> ret = (Queues.Stack<E>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        //$DELAY$
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0);
                //$DELAY$
                new DB(e).getStack("a");
                return namedPut(name,
                        new DB(new Engine.ReadOnly(e)).getStack("a"));
            }
            return createStack(name,null,true);
        }
        //$DELAY$
        checkType(type, "Stack");

        ret = new Queues.Stack<E>(engine,
                (Serializer<E>) catGet(name+".serializer",getDefaultSerializer()),
                (Long)catGet(name+".headRecid")
        );
        //$DELAY$
        namedPut(name, ret);
        //$DELAY$
        return ret;
    }



    synchronized public <E> BlockingQueue<E> createStack(String name, Serializer<E> serializer, boolean useLocks) {
        checkNameNotExists(name);

        long node = engine.preallocate();
        long headRecid = engine.put(node, Serializer.LONG);
        //$DELAY$
        Queues.Stack<E> ret = new Queues.Stack<E>(engine,
                catPut(name+".serializer",serializer,getDefaultSerializer()),
                catPut(name+".headRecid",headRecid)
        );
        //$DELAY$
        catalog.put(name + ".type", "Stack");
        namedPut(name, ret);
        return ret;
    }


    synchronized public <E> BlockingQueue<E> getCircularQueue(String name) {
        checkNotClosed();
        BlockingQueue<E> ret = (BlockingQueue<E>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        //$DELAY$
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()) {
                Engine e = new StoreHeap(true,1,0);
                new DB(e).getCircularQueue("a");
                //$DELAY$
                return namedPut(name,
                        new DB(new Engine.ReadOnly(e)).getCircularQueue("a"));
            }
            return createCircularQueue(name,null, 1024);
        }

        checkType(type, "CircularQueue");

        ret = new Queues.CircularQueue<E>(engine,
                (Serializer<E>) catGet(name+".serializer",getDefaultSerializer()),
                (Long)catGet(name+".headRecid"),
                (Long)catGet(name+".headInsertRecid"),
                (Long)catGet(name+".size")
        );
        //$DELAY$

        namedPut(name, ret);
        return ret;
    }



    synchronized public <E> BlockingQueue<E> createCircularQueue(String name, Serializer<E> serializer, long size) {
        checkNameNotExists(name);
        if(serializer==null) serializer = getDefaultSerializer();

//        long headerRecid = engine.put(0L, Serializer.LONG);
        //insert N Nodes empty nodes into a circle
        long prevRecid = 0;
        long firstRecid = 0;
        //$DELAY$
        Serializer<Queues.SimpleQueue.Node<E>> nodeSer = new Queues.SimpleQueue.NodeSerializer<E>(serializer);
        for(long i=0;i<size;i++){
            Queues.SimpleQueue.Node<E> n = new Queues.SimpleQueue.Node<E>(prevRecid, null);
            prevRecid = engine.put(n, nodeSer);
            if(firstRecid==0) firstRecid = prevRecid;
        }
        //update first node to point to last recid
        engine.update(firstRecid, new Queues.SimpleQueue.Node<E>(prevRecid, null), nodeSer );

        long headRecid = engine.put(prevRecid, Serializer.LONG);
        long headInsertRecid = engine.put(prevRecid, Serializer.LONG);
        //$DELAY$


        Queues.CircularQueue<E> ret = new Queues.CircularQueue<E>(engine,
                catPut(name+".serializer",serializer),
                catPut(name+".headRecid",headRecid),
                catPut(name+".headInsertRecid",headInsertRecid),
                catPut(name+".size",size)
        );
        //$DELAY$
        catalog.put(name + ".type", "CircularQueue");
        namedPut(name, ret);
        return ret;
    }

    synchronized public Atomic.Long createAtomicLong(String name, long initValue){
        checkNameNotExists(name);
        long recid = engine.put(initValue,Serializer.LONG);
        Atomic.Long ret = new Atomic.Long(engine,
                catPut(name+".recid",recid)
        );
        //$DELAY$
        catalog.put(name + ".type", "AtomicLong");
        namedPut(name, ret);
        return ret;

    }


    synchronized public Atomic.Long getAtomicLong(String name){
        checkNotClosed();
        Atomic.Long ret = (Atomic.Long) getFromWeakCollection(name);
        if(ret!=null) return ret;
        //$DELAY$
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            if (engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0);
                new DB(e).getAtomicLong("a");
                //$DELAY$
                return namedPut(name,
                        new DB(new Engine.ReadOnly(e)).getAtomicLong("a"));
            }
            return createAtomicLong(name,0L);
        }
        checkType(type, "AtomicLong");
        //$DELAY$
        ret = new Atomic.Long(engine, (Long) catGet(name+".recid"));
        namedPut(name, ret);
        return ret;
    }



    synchronized public Atomic.Integer createAtomicInteger(String name, int initValue){
        checkNameNotExists(name);
        long recid = engine.put(initValue,Serializer.INTEGER);
        Atomic.Integer ret = new Atomic.Integer(engine,
                catPut(name+".recid",recid)
        );
        //$DELAY$
        catalog.put(name + ".type", "AtomicInteger");
        namedPut(name, ret);
        return ret;

    }


    synchronized public Atomic.Integer getAtomicInteger(String name){
        checkNotClosed();
        Atomic.Integer ret = (Atomic.Integer) getFromWeakCollection(name);
        if(ret!=null) return ret;
        //$DELAY$
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0);
                new DB(e).getAtomicInteger("a");
                //$DELAY$
                return namedPut(name,
                        new DB(new Engine.ReadOnly(e)).getAtomicInteger("a"));
            }
            return createAtomicInteger(name, 0);
        }
        checkType(type, "AtomicInteger");

        ret = new Atomic.Integer(engine, (Long) catGet(name+".recid"));
        namedPut(name, ret);
        return ret;
    }



    synchronized public Atomic.Boolean createAtomicBoolean(String name, boolean initValue){
        checkNameNotExists(name);
        long recid = engine.put(initValue,Serializer.BOOLEAN);
        //$DELAY$
        Atomic.Boolean ret = new Atomic.Boolean(engine,
                catPut(name+".recid",recid)
        );
        catalog.put(name + ".type", "AtomicBoolean");
        //$DELAY$
        namedPut(name, ret);
        return ret;

    }


    synchronized public Atomic.Boolean getAtomicBoolean(String name){
        checkNotClosed();
        Atomic.Boolean ret = (Atomic.Boolean) getFromWeakCollection(name);
        if(ret!=null) return ret;
        //$DELAY$
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0);
                new DB(e).getAtomicBoolean("a");
                return namedPut(name,
                        new DB(new Engine.ReadOnly(e)).getAtomicBoolean("a"));
            }
            //$DELAY$
            return createAtomicBoolean(name, false);
        }
        checkType(type, "AtomicBoolean");
        //$DELAY$
        ret = new Atomic.Boolean(engine, (Long) catGet(name+".recid"));
        namedPut(name, ret);
        return ret;
    }

    public void checkShouldCreate(String name) {
        if(strictDBGet) throw new NoSuchElementException("No record with this name was found: "+name);
    }


    synchronized public Atomic.String createAtomicString(String name, String initValue){
        checkNameNotExists(name);
        if(initValue==null) throw new IllegalArgumentException("initValue may not be null");
        long recid = engine.put(initValue,Serializer.STRING_NOSIZE);
        //$DELAY$
        Atomic.String ret = new Atomic.String(engine,
                catPut(name+".recid",recid)
        );
        //$DELAY$
        catalog.put(name + ".type", "AtomicString");
        namedPut(name, ret);
        return ret;

    }


    synchronized public Atomic.String getAtomicString(String name){
        checkNotClosed();
        Atomic.String ret = (Atomic.String) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        //$DELAY$
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0);
                new DB(e).getAtomicString("a");
                //$DELAY$
                return namedPut(name,
                        new DB(new Engine.ReadOnly(e)).getAtomicString("a"));
            }
            return createAtomicString(name, "");
        }
        checkType(type, "AtomicString");

        ret = new Atomic.String(engine, (Long) catGet(name+".recid"));
        namedPut(name, ret);
        return ret;
    }

    synchronized public <E> Atomic.Var<E> createAtomicVar(String name, E initValue, Serializer<E> serializer){
        checkNameNotExists(name);
        if(serializer==null) serializer=getDefaultSerializer();
        long recid = engine.put(initValue,serializer);
        //$DELAY$
        Atomic.Var ret = new Atomic.Var(engine,
                catPut(name+".recid",recid),
                catPut(name+".serializer",serializer)
        );
        //$DELAY$
        catalog.put(name + ".type", "AtomicVar");
        namedPut(name, ret);
        return ret;

    }


    synchronized public <E> Atomic.Var<E> getAtomicVar(String name){
        checkNotClosed();

        Atomic.Var ret = (Atomic.Var) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0);
                new DB(e).getAtomicVar("a");
                return namedPut(name,
                        new DB(new Engine.ReadOnly(e)).getAtomicVar("a"));
            }
            //$DELAY$
            return createAtomicVar(name, null, getDefaultSerializer());
        }
        checkType(type, "AtomicVar");

        ret = new Atomic.Var(engine, (Long) catGet(name+".recid"), (Serializer) catGet(name+".serializer"));
        namedPut(name, ret);
        return ret;
    }

    /** return record with given name or null if name does not exist*/
    synchronized public <E> E get(String name){
        //$DELAY$
        String type = catGet(name+".type");
        if(type==null) return null;
        if("HashMap".equals(type)) return (E) getHashMap(name);
        if("HashSet".equals(type)) return (E) getHashSet(name);
        if("TreeMap".equals(type)) return (E) getTreeMap(name);
        if("TreeSet".equals(type)) return (E) getTreeSet(name);
        if("AtomicBoolean".equals(type)) return (E) getAtomicBoolean(name);
        if("AtomicInteger".equals(type)) return (E) getAtomicInteger(name);
        if("AtomicLong".equals(type)) return (E) getAtomicLong(name);
        if("AtomicString".equals(type)) return (E) getAtomicString(name);
        if("AtomicVar".equals(type)) return (E) getAtomicVar(name);
        if("Queue".equals(type)) return (E) getQueue(name);
        if("Stack".equals(type)) return (E) getStack(name);
        if("CircularQueue".equals(type)) return (E) getCircularQueue(name);
        throw new AssertionError("Unknown type: "+name);
    }

    synchronized public boolean exists(String name){
        return catGet(name+".type")!=null;
    }

    /** delete record/collection with given name*/
    synchronized public void delete(String name){
        //$DELAY$
        Object r = get(name);
        if(r instanceof Atomic.Boolean){
            engine.delete(((Atomic.Boolean)r).recid, Serializer.BOOLEAN);
        }else if(r instanceof Atomic.Integer){
            engine.delete(((Atomic.Integer)r).recid, Serializer.INTEGER);
        }else if(r instanceof Atomic.Long){
            engine.delete(((Atomic.Long)r).recid, Serializer.LONG);
        }else if(r instanceof Atomic.String){
            engine.delete(((Atomic.String)r).recid, Serializer.STRING_NOSIZE);
        }else if(r instanceof Atomic.Var){
            engine.delete(((Atomic.Var)r).recid, ((Atomic.Var)r).serializer);
        }else if(r instanceof Queue){
            //drain queue
            Queue q = (Queue) r;
            while(q.poll()!=null){
                //do nothing
            }
        }else if(r instanceof HTreeMap || r instanceof HTreeMap.KeySet){
            HTreeMap m = (r instanceof HTreeMap)? (HTreeMap) r : ((HTreeMap.KeySet)r).parent();
            m.clear();
            //$DELAY$
            //delete segments
            for(long segmentRecid:m.segmentRecids){
                engine.delete(segmentRecid, HTreeMap.DIR_SERIALIZER);
            }
        }else if(r instanceof BTreeMap || r instanceof BTreeMap.KeySet){
            BTreeMap m = (r instanceof BTreeMap)? (BTreeMap) r : (BTreeMap) ((BTreeMap.KeySet) r).m;
            //$DELAY$
            //TODO on BTreeMap recursively delete all nodes
            m.clear();

            if(m.counter!=null)
                engine.delete(m.counter.recid,Serializer.LONG);
        }

        for(String n:catalog.keySet()){
            if(!n.startsWith(name))
                continue;
            String suffix = n.substring(name.length());
            if(suffix.charAt(0)=='.' && suffix.length()>1 && !suffix.substring(1).contains("."))
                catalog.remove(n);
        }
        namesInstanciated.remove(name);
        namesLookup.remove(new IdentityWrapper(r));
    }


    /**
     * return map of all named collections/records
     */
    synchronized public Map<String,Object> getAll(){
        TreeMap<String,Object> ret= new TreeMap<String, Object>();
        //$DELAY$
        for(String name:catalog.keySet()){
            if(!name.endsWith(".type")) continue;
            //$DELAY$
            name = name.substring(0,name.length()-5);
            ret.put(name,get(name));
        }

        return Collections.unmodifiableMap(ret);
    }


    /** rename named record into newName
     *
     * @param oldName current name of record/collection
     * @param newName new name of record/collection
     * @throws NoSuchElementException if oldName does not exist
     */
    synchronized public void rename(String oldName, String newName){
        if(oldName.equals(newName)) return;
        //$DELAY$
        Map<String, Object> sub = catalog.tailMap(oldName);
        List<String> toRemove = new ArrayList<String>();
        //$DELAY$
        for(String param:sub.keySet()){
            if(!param.startsWith(oldName)) break;

            String suffix = param.substring(oldName.length());
            catalog.put(newName+suffix, catalog.get(param));
            toRemove.add(param);
        }
        if(toRemove.isEmpty()) throw new NoSuchElementException("Could not rename, name does not exist: "+oldName);
        //$DELAY$
        WeakReference old = namesInstanciated.remove(oldName);
        if(old!=null){
            Object old2 = old.get();
            if(old2!=null){
                namesLookup.remove(new IdentityWrapper(old2));
                namedPut(newName,old2);
            }
        }
        for(String param:toRemove) catalog.remove(param);
    }


    /**
     * Checks that object with given name does not exist yet.
     * @param name to check
     * @throws IllegalArgumentException if name is already used
     */
    public void checkNameNotExists(String name) {
        if(catalog.get(name+".type")!=null)
            throw new IllegalArgumentException("Name already used: "+name);
    }


    /**
     * Closes database.
     * All other methods will throw 'IllegalAccessError' after this method was called.
     * <p>
     * !! it is necessary to call this method before JVM exits!!
     */
    synchronized public void close(){
        if(engine == null) return;
        for(WeakReference r:namesInstanciated.values()){
            Object rr = r.get();
            if(rr !=null && rr instanceof Closeable)
                try {
                    ((Closeable)rr).close();
                } catch (IOException e) {
                    throw new IOError(e);
                }
        }
        String fileName = deleteFilesAfterClose?Store.forEngine(engine).fileName:null;
        engine.close();
        //dereference db to prevent memory leaks
        engine = CLOSED_ENGINE;
        namesInstanciated = Collections.unmodifiableMap(new HashMap());
        namesLookup = Collections.unmodifiableMap(new HashMap());

        if(deleteFilesAfterClose && fileName!=null){
            File f = new File(fileName);
            if (f.exists() && !f.delete()) {
                //TODO file was not deleted, log warning
            }
            //TODO delete WAL files and append-only files
        }
    }

    /**
     * All collections are weakly referenced to prevent two instances of the same collection in memory.
     * This is mainly for locking, two instances of the same lock would not simply work.
     */
    synchronized public Object getFromWeakCollection(String name){
        WeakReference<?> r =  namesInstanciated.get(name);
        //$DELAY$
        if(r==null) return null;
        //$DELAY$
        Object o = r.get();
        if(o==null) namesInstanciated.remove(name);
        return o;
    }



    public void checkNotClosed() {
        if(engine == null) throw new IllegalAccessError("DB was already closed");
    }

    /**
     * @return true if DB is closed and can no longer be used
     */
    public synchronized  boolean isClosed(){
        return engine == null || engine.isClosed();
    }

    /**
     * Commit changes made on collections loaded by this DB
     *
     * @see org.mapdb.Engine#commit()
     */
    synchronized public void commit() {
        checkNotClosed();
        //update Class Catalog with missing classes as part of this transaction
        String[] toBeAdded = unknownClasses.isEmpty()?null:unknownClasses.toArray(new String[0]);

        if(toBeAdded!=null) {

            SerializerPojo.ClassInfo[] classes =  serializerPojo.getClassInfos.run();
            SerializerPojo.ClassInfo[] classes2 = classes.length==0?null:classes;

            for(String className:toBeAdded){
                int pos = serializerPojo.classToId(classes,className);
                if(pos!=-1) {
                    continue;
                }
                SerializerPojo.ClassInfo classInfo = serializerPojo.makeClassInfo(className);
                classes = Arrays.copyOf(classes,classes.length+1);
                classes[classes.length-1]=classInfo;
            }
            engine.compareAndSwap(Engine.RECID_CLASS_CATALOG,classes2,classes,SerializerPojo.CLASS_CATALOG_SERIALIZER);
        }




        engine.commit();

        if(toBeAdded!=null) {
            for (String className : toBeAdded) {
                unknownClasses.remove(className);
            }
        }
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
     * <p>
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
        Engine snapshot = TxEngine.createSnapshotFor(engine);
        return new DB (snapshot);
    }

    /**
     * @return default serializer used in this DB, it handles POJO and other stuff.
     */
    public  Serializer getDefaultSerializer() {
        return serializerPojo;
    }

    /**
     * @return underlying engine which takes care of persistence for this DB.
     */
    public Engine getEngine() {
        return engine;
    }

    public void checkType(String type, String expected) {
        //$DELAY$
        if(!expected.equals(type)) throw new IllegalArgumentException("Wrong type: "+type);
    }


    /** throws `IllegalArgumentError("already closed)` on all access */
    protected static final Engine CLOSED_ENGINE = new Engine(){


        @Override
        public long preallocate() {
            throw new IllegalAccessError("already closed");
        }


        @Override
        public <A> long put(A value, Serializer<A> serializer) {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public <A> void delete(long recid, Serializer<A> serializer) {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public void close() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public boolean isClosed() {
            return true;
        }

        @Override
        public void commit() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public void rollback() throws UnsupportedOperationException {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public boolean isReadOnly() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public boolean canRollback() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public boolean canSnapshot() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public Engine snapshot() throws UnsupportedOperationException {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public Engine getWrappedEngine() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public void clearCache() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public void compact() {
            throw new IllegalAccessError("already closed");
        }


    };


}
