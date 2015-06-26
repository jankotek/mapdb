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
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A database with easy access to named maps and other collections.
 *
 * @author Jan Kotek
 */
//TODO DB uses global lock, replace it with ReadWrite lock or fine grained locking.
@SuppressWarnings("unchecked")
public class DB implements Closeable {

    protected static final Logger LOG = Logger.getLogger(DB.class.getName());
    public static final String METRICS_DATA_WRITE = "data.write";
    public static final String METRICS_RECORD_WRITE = "record.write";
    public static final String METRICS_DATA_READ = "data.read";
    public static final String METRICS_RECORD_READ = "record.read";
    public static final String METRICS_CACHE_HIT = "cache.hit";
    public static final String METRICS_CACHE_MISS = "cache.miss";


    protected final boolean strictDBGet;
    protected final boolean deleteFilesAfterClose;

    /** Engine which provides persistence for this DB*/
    protected Engine engine;
    /** already loaded named collections. It is important to keep collections as singletons, because of 'in-memory' locking*/
    protected Map<String, WeakReference<?>> namesInstanciated = new HashMap<String, WeakReference<?>>();

    protected Map<IdentityWrapper, String> namesLookup =
            new ConcurrentHashMap<IdentityWrapper, String>();

    /** view over named records */
    protected SortedMap<String, Object> catalog;

    protected ScheduledExecutorService executor = null;

    protected SerializerPojo serializerPojo;

    protected ScheduledExecutorService metricsExecutor;
    protected ScheduledExecutorService storeExecutor;
    protected ScheduledExecutorService cacheExecutor;

    protected final Set<String> unknownClasses = new ConcurrentSkipListSet<String>();

    //TODO collection get/create should be under consistencyLock.readLock()
    protected final ReadWriteLock consistencyLock;

    /** changes object hash and equals method to use identity */
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
        this(engine,false,false, null, false, null, 0, null, null);
    }

    public DB(
            final Engine engine,
            boolean strictDBGet,
            boolean deleteFilesAfterClose,
            ScheduledExecutorService executor,
            boolean lockDisable,
            ScheduledExecutorService metricsExecutor,
            long metricsLogInterval,
            ScheduledExecutorService storeExecutor,
            ScheduledExecutorService cacheExecutor
            ) {
        //TODO investigate dereference and how non-final field affect performance. Perhaps abandon dereference completely
//        if(!(engine instanceof EngineWrapper)){
//            //access to Store should be prevented after `close()` was called.
//            //So for this we have to wrap raw Store into EngineWrapper
//            engine = new EngineWrapper(engine);
//        }
        this.engine = engine;
        this.strictDBGet = strictDBGet;
        this.deleteFilesAfterClose = deleteFilesAfterClose;
        this.executor = executor;
        this.consistencyLock = lockDisable ?
                new Store.ReadWriteSingleLock(Store.NOLOCK) :
                new ReentrantReadWriteLock();

        this.metricsExecutor = metricsExecutor==null ? executor : metricsExecutor;
        this.storeExecutor = storeExecutor;
        this.cacheExecutor = cacheExecutor;

        serializerPojo = new SerializerPojo(
                //get name for given object
                new Fun.Function1<String, Object>() {
                    @Override
                    public String run(Object o) {
                        if(o==DB.this)
                            return "$$DB_OBJECT_Q!#!@#!#@9009a09sd";
                        return getNameForObject(o);
                    }
                },
                //get object with given name
                new Fun.Function1<Object, String>() {
                    @Override
                    public Object run(String name) {
                        Object ret = get(name);
                        if(ret == null && "$$DB_OBJECT_Q!#!@#!#@9009a09sd".equals(name))
                            return DB.this;
                        return ret;
                    }
                },
                //load class catalog
                new Fun.Function1Int<SerializerPojo.ClassInfo>() {
                    @Override
                    public SerializerPojo.ClassInfo run(int index) {
                        long[] classInfoRecids = DB.this.engine.get(Engine.RECID_CLASS_CATALOG, Serializer.RECID_ARRAY);
                        if(classInfoRecids==null || index<0 || index>=classInfoRecids.length)
                            return null;
                        return getEngine().get(classInfoRecids[index], SerializerPojo.CLASS_INFO_SERIALIZER);
                    }
                },
                new Fun.Function0<SerializerPojo.ClassInfo[]>() {
                    @Override
                    public SerializerPojo.ClassInfo[] run() {
                        long[] classInfoRecids = engine.get(Engine.RECID_CLASS_CATALOG, Serializer.RECID_ARRAY);
                        SerializerPojo.ClassInfo[] ret = new SerializerPojo.ClassInfo[classInfoRecids==null?0:classInfoRecids.length];
                        for(int i=0;i<ret.length;i++){
                            ret[i] = engine.get(classInfoRecids[i],SerializerPojo.CLASS_INFO_SERIALIZER);
                        }
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

        if(metricsExecutor!=null && metricsLogInterval!=0){

            if(!CC.METRICS_CACHE){
                LOG.warning("MapDB was compiled without cache metrics. No cache hit/miss will be reported");
            }

            metricsExecutor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    metricsLog();
                }
            }, metricsLogInterval, metricsLogInterval, TimeUnit.MILLISECONDS);
        }
    }

    public void metricsLog() {
        Map metrics = DB.this.metricsGet();
        String s = metrics.toString();
        LOG.info("Metrics: "+s);
    }

    public Map<String,Long> metricsGet() {
        Map ret = new TreeMap();
        Store s = Store.forEngine(engine);
        s.metricsCollect(ret);
        return Collections.unmodifiableMap(ret);
    }

    protected void reinit() {
        //open name dir
        //$DELAY$
        catalog = BTreeMap.preinitCatalog(this);
    }

    public <A> A catGet(String name, A init){
        if(CC.ASSERT && ! (Thread.holdsLock(DB.this)))
            throw new AssertionError();
        A ret = (A) catalog.get(name);
        return ret!=null? ret : init;
    }


    public <A> A catGet(String name){
        if(CC.ASSERT && ! (Thread.holdsLock(DB.this)))
            throw new AssertionError();
        //$DELAY$
        return (A) catalog.get(name);
    }

    public <A> A catPut(String name, A value){
        if(CC.ASSERT && ! (Thread.holdsLock(DB.this)))
            throw new AssertionError();
        //$DELAY$
        catalog.put(name, value);
        return value;
    }

    public <A> A catPut(String name, A value, A retValueIfNull){
        if(CC.ASSERT && ! (Thread.holdsLock(DB.this)))
            throw new AssertionError();
        if(value==null) return retValueIfNull;
        //$DELAY$
        catalog.put(name, value);
        return value;
    }

    /** returns name for this object, if it has name and was instanciated by this DB*/
    public String getNameForObject(Object obj) {
        return namesLookup.get(new IdentityWrapper(obj));
    }


    static public class HTreeMapMaker{

        protected final DB db;
        protected final String name;
        protected final Engine[] engines;

        public HTreeMapMaker(DB db, String name, Engine[] engines) {
            this.db = db;
            this.name = name;
            this.engines = engines;
            this.executor = db.executor;
        }


        protected boolean counter = false;
        protected Serializer<?> keySerializer = null;
        protected Serializer<?> valueSerializer = null;
        protected long expireMaxSize = 0L;
        protected long expire = 0L;
        protected long expireAccess = 0L;
        protected long expireStoreSize;
        protected Bind.MapWithModificationListener ondisk;
        protected boolean ondiskOverwrite;


        protected Fun.Function1<?,?> valueCreator = null;

        protected Iterator pumpSource;
        protected Fun.Function1 pumpKeyExtractor;
        protected Fun.Function1 pumpValueExtractor;
        protected int pumpPresortBatchSize = (int) 1e7;
        protected boolean pumpIgnoreDuplicates = false;
        protected boolean closeEngine = false;

        protected ScheduledExecutorService executor;
        protected long executorPeriod = CC.DEFAULT_HTREEMAP_EXECUTOR_PERIOD;


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


        /**
         * After expiration (or deletion), put entries into given map
         *
         * @param ondisk Map populated with data after expiration
         * @param overwrite if true any data in onDisk will be overwritten.
         *                           If false only non-existing keys will be inserted
         *                             ({@code put() versus putIfAbsent()};
         *
         * @return this builder
         */
        public HTreeMapMaker expireOverflow(Bind.MapWithModificationListener ondisk, boolean overwrite){
            this.ondisk = ondisk;
            this.ondiskOverwrite = overwrite;
            return this;
        }

        /** If value is not found, HTreeMap can fetch and insert default value. {@code valueCreator} is used to return new value.
         * This way {@code HTreeMap.get()} never returns null */
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


        public HTreeMapMaker executorEnable(){
            return executorEnable(Executors.newSingleThreadScheduledExecutor());
        }

        public HTreeMapMaker executorEnable(ScheduledExecutorService executor) {
            this.executor = executor;
            return this;
        }

        public HTreeMapMaker executorPeriod(long period){
            this.executorPeriod = period;
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


        protected HTreeMapMaker closeEngine() {
            closeEngine = true;
            return this;
        }


        public <K,V> HTreeMap<K,V> make(){
            if(expireMaxSize!=0) counter =true;
            return db.hashMapCreate(HTreeMapMaker.this);
        }

        public <K,V> HTreeMap<K,V> makeOrGet(){
            //$DELAY$
            synchronized (db){
                //TODO add parameter check
                //$DELAY$
                return (HTreeMap<K, V>) (db.catGet(name+".type")==null?
                        make():
                        db.hashMap(name,keySerializer,valueSerializer,(Fun.Function1)valueCreator));
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

        protected Iterator pumpSource;
        protected int pumpPresortBatchSize = (int) 1e7;
        protected boolean pumpIgnoreDuplicates = false;
        protected boolean closeEngine = false;

        protected ScheduledExecutorService executor = DB.this.executor;
        protected long executorPeriod = CC.DEFAULT_HTREEMAP_EXECUTOR_PERIOD;

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


        public HTreeSetMaker executorEnable(){
            return executorEnable(Executors.newSingleThreadScheduledExecutor());
        }

        public HTreeSetMaker executorEnable(ScheduledExecutorService executor) {
            this.executor = executor;
            return this;
        }

        public HTreeSetMaker executorPeriod(long period){
            this.executorPeriod = period;
            return this;
        }


        protected HTreeSetMaker closeEngine() {
            this.closeEngine = true;
            return this;
        }



        public <K> Set<K> make(){
            if(expireMaxSize!=0) counter =true;
            return DB.this.hashSetCreate(HTreeSetMaker.this);
        }

        public <K> Set<K> makeOrGet(){
            synchronized (DB.this){
                //$DELAY$
                //TODO add parameter check
                return (Set<K>) (catGet(name+".type")==null?
                        make(): hashSet(name,serializer));
            }
        }

    }


    /**
     * @deprecated method renamed, use {@link DB#hashMap(String)}
     */
    synchronized public <K,V> HTreeMap<K,V> getHashMap(String name){
        return hashMap(name);
    }
    /**
     * Opens existing or creates new Hash Tree Map.
     * This collection perform well under concurrent access.
     * Is best for large keys and large values.
     *
     * @param name of the map
     * @return map
     */
    synchronized public <K,V> HTreeMap<K,V> hashMap(String name){
        return hashMap(name, null, null, null);
    }

    /**
     * @deprecated method renamed, use {@link DB#hashMap(String,Serializer, Serializer, org.mapdb.Fun.Function1)}
     */
    synchronized public <K,V> HTreeMap<K,V> getHashMap(String name, Fun.Function1<V,K> valueCreator){
        return hashMap(name, null, null, valueCreator);
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
    synchronized public <K,V> HTreeMap<K,V> hashMap(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            Fun.Function1<V, K> valueCreator){
        checkNotClosed();
        HTreeMap<K,V> ret = (HTreeMap<K, V>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        //$DELAY$
        if(type==null){
            //$DELAY$
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0,false);
                //$DELAY$
                new DB(e).hashMap("a");
                return namedPut(name,
                        new DB(new Engine.ReadOnlyWrapper(e)).hashMap("a"));
            }
            if(valueCreator!=null)
                return hashMapCreate(name).valueCreator(valueCreator).make();
            return hashMapCreate(name).make();
        }


        //check type
        checkType(type, "HashMap");

        Object keySer2 = catGet(name+".keySerializer");
        if(keySerializer!=null){
            if(keySer2!=Fun.PLACEHOLDER && keySer2!=keySerializer){
                LOG.warning("Map '"+name+"' has keySerializer defined in Name Catalog, but other serializer was passed as constructor argument. Using one from constructor argument.");
            }
            keySer2 = keySerializer;
        }
        if(keySer2==Fun.PLACEHOLDER){
            throw new DBException.UnknownSerializer("Map '"+name+"' has no keySerializer defined in Name Catalog nor constructor argument.");
        }

        Object valSer2 = catGet(name+".valueSerializer");
        if(valueSerializer!=null){
            if(valSer2!=Fun.PLACEHOLDER && valSer2!=valueSerializer){
                LOG.warning("Map '"+name+"' has valueSerializer defined in name catalog, but other serializer was passed as constructor argument. Using one from constructor argument.");
            }
            valSer2 = valueSerializer;
        }
        if(valSer2==Fun.PLACEHOLDER) {
            throw new DBException.UnknownSerializer("Map '" + name + "' has no valueSerializer defined in Name Catalog nor constructor argument.");
        }

        //open existing map
        //$DELAY$
        ret = new HTreeMap<K,V>(
                HTreeMap.fillEngineArray(engine),
                false,
                (long[])catGet(name+".counterRecids"),
                (Integer)catGet(name+".hashSalt"),
                (long[])catGet(name+".segmentRecids"),
                (Serializer<K>)keySer2,
                (Serializer<V>)valSer2,
                catGet(name+".expireTimeStart",0L),
                catGet(name+".expire",0L),
                catGet(name+".expireAccess",0L),
                catGet(name+".expireMaxSize",0L),
                catGet(name+".expireStoreSize",0L),
                (long[])catGet(name+".expireHeads",null),
                (long[])catGet(name+".expireTails",null),
                valueCreator,
                executor,
                CC.DEFAULT_HTREEMAP_EXECUTOR_PERIOD,
                false,
                consistencyLock.readLock()
        );

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
     * @deprecated method renamed, use {@link DB#hashMapCreate(String)}
     */
    public HTreeMapMaker createHashMap(String name){
        return hashMapCreate(name);
    }

    /**
     * Returns new builder for HashMap with given name
     *
     * @param name of map to create
     * @throws IllegalArgumentException if name is already used
     * @return maker, call {@code .make()} to create map
     */
    public HTreeMapMaker hashMapCreate(String name){
        return new HTreeMapMaker(DB.this, name, HTreeMap.fillEngineArray(engine));
    }



    /**
     * Creates new HashMap with more specific arguments
     *
     * @throws IllegalArgumentException if name is already used
     * @return newly created map
     */
    synchronized protected <K,V> HTreeMap<K,V> hashMapCreate(HTreeMapMaker m){
        String name = m.name;
        checkNameNotExists(name);
        //$DELAY$
        long expireTimeStart=0, expire=0, expireAccess=0, expireMaxSize = 0, expireStoreSize=0;
        long[] expireHeads=null, expireTails=null;


        if(m.ondisk!=null) {
            if (m.valueCreator != null) {
                throw new IllegalArgumentException("ValueCreator can not be used together with ExpireOverflow.");
            }
            final Map ondisk = m.ondisk;
            m.valueCreator = new Fun.Function1<Object, Object>() {
                @Override
                public Object run(Object key) {
                    return ondisk.get(key);
                }
            };
        }

        if(m.expire!=0 || m.expireAccess!=0 || m.expireMaxSize !=0 || m.expireStoreSize!=0){
            expireTimeStart = catPut(name+".expireTimeStart",System.currentTimeMillis());
            expire = catPut(name+".expire",m.expire);
            expireAccess = catPut(name+".expireAccess",m.expireAccess);
            expireMaxSize = catPut(name+".expireMaxSize",m.expireMaxSize);
            expireStoreSize = catPut(name+".expireStoreSize",m.expireStoreSize);
            //$DELAY$
            expireHeads = new long[HTreeMap.SEG];
            expireTails = new long[HTreeMap.SEG];
            for(int i=0;i<HTreeMap.SEG;i++){
                expireHeads[i] = m.engines[i].put(0L,Serializer.LONG);
                expireTails[i] = m.engines[i].put(0L, Serializer.LONG);
            }
            catPut(name+".expireHeads",expireHeads);
            catPut(name+".expireTails",expireTails);
        }
        //$DELAY$

        long[] counterRecids = null;
        if(m.counter){
            counterRecids = new long[HTreeMap.SEG];
            for(int i=0;i<HTreeMap.SEG;i++){
                counterRecids[i] = m.engines[i].put(0L,Serializer.LONG);
            }
        }

        if(m.keySerializer==null) {
            m.keySerializer = getDefaultSerializer();
        }
        catPut(name+".keySerializer",serializableOrPlaceHolder(m.keySerializer));
        if(m.valueSerializer==null) {
            m.valueSerializer = getDefaultSerializer();
        }
        catPut(name+".valueSerializer",serializableOrPlaceHolder(m.valueSerializer));



        HTreeMap<K,V> ret = new HTreeMap<K,V>(
                m.engines,
                m.closeEngine,
                counterRecids==null? null : catPut(name + ".counterRecids", counterRecids),
                catPut(name+".hashSalt",new SecureRandom().nextInt()),
                catPut(name+".segmentRecids",HTreeMap.preallocateSegments(m.engines)),
                (Serializer<K>)m.keySerializer,
                (Serializer<V>)m.valueSerializer,
                expireTimeStart,expire,expireAccess,expireMaxSize, expireStoreSize, expireHeads ,expireTails,
                (Fun.Function1<V, K>) m.valueCreator,
                m.executor,
                m.executorPeriod,
                m.executor!=executor,
                consistencyLock.readLock());
        //$DELAY$
        catalog.put(name + ".type", "HashMap");
        namedPut(name, ret);


        //pump data if specified2
        if(m.pumpSource!=null) {
            Pump.fillHTreeMap(
                    ret,
                    m.pumpSource,
                    m.pumpKeyExtractor,
                    m.pumpValueExtractor,
                    m.pumpPresortBatchSize,
                    m.pumpIgnoreDuplicates,
                    getDefaultSerializer(),
                    m.executor);
        }

        if(m.ondisk!=null){
            Bind.mapPutAfterDelete(ret,m.ondisk, m.ondiskOverwrite);
        }

        return ret;
    }

    protected Object serializableOrPlaceHolder(Object o) {
        SerializerBase b = (SerializerBase)getDefaultSerializer();
        if(o == null || b.isSerializable(o)){
            if(!(o instanceof BTreeKeySerializer.BasicKeySerializer))
                return o;

            BTreeKeySerializer.BasicKeySerializer oo = (BTreeKeySerializer.BasicKeySerializer) o;
            if(b.isSerializable(oo.serializer) && b.isSerializable(oo.comparator))
                return o;
        }

        return Fun.PLACEHOLDER;
    }

    /**
     * @deprecated method renamed, use {@link DB#hashSet(String)}
     */
    synchronized public <K> Set<K> getHashSet(String name){
        return hashSet(name);
    }

    /**
     *  Opens existing or creates new Hash Tree Set.
     *
     * @param name of the Set
     * @return set
     */
    synchronized public <K> Set<K> hashSet(String name){
        return hashSet(name,null);
    }

    synchronized public <K> Set<K> hashSet(String name, Serializer<K> serializer){
        checkNotClosed();
        Set<K> ret = (Set<K>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        //$DELAY$
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0,false);
                //$DELAY$
                new DB(e).hashSet("a");
                return namedPut(name,
                        new DB(new Engine.ReadOnlyWrapper(e)).hashSet("a"));
            }
            return hashSetCreate(name).makeOrGet();
            //$DELAY$
        }


        //check type
        checkType(type, "HashSet");

        Object keySer2 = catGet(name+".serializer");
        if(serializer!=null){
            if(keySer2!=Fun.PLACEHOLDER && keySer2!=serializer){
                LOG.warning("Set '"+name+"' has serializer defined in Name Catalog, but other serializer was passed as constructor argument. Using one from constructor argument.");
            }
            keySer2 = serializer;
        }
        if(keySer2==Fun.PLACEHOLDER){
            throw new DBException.UnknownSerializer("Set '"+name+"' has no serializer defined in Name Catalog nor constructor argument.");
        }


        //open existing map
        ret = new HTreeMap<K, Object>(
                HTreeMap.fillEngineArray(engine),
                false,
                (long[])catGet(name+".counterRecids"),
                (Integer)catGet(name+".hashSalt"),
                (long[])catGet(name+".segmentRecids"),
                (Serializer)keySer2,
                null,
                catGet(name+".expireTimeStart",0L),
                catGet(name+".expire",0L),
                catGet(name+".expireAccess",0L),
                catGet(name+".expireMaxSize",0L),
                catGet(name+".expireStoreSize",0L),
                (long[])catGet(name+".expireHeads",null),
                (long[])catGet(name+".expireTails",null),
                null,
                executor,
                CC.DEFAULT_HTREEMAP_EXECUTOR_PERIOD,
                false,
                consistencyLock.readLock()
        ).keySet();

        //$DELAY$
        namedPut(name, ret);
        //$DELAY$
        return ret;
    }

    /**
     * @deprecated method renamed, use {@link DB#hashSetCreate(String)}
     */
    synchronized public HTreeSetMaker createHashSet(String name){
        return hashSetCreate(name);
    }
    /**
     * Creates new HashSet
     *
     * @param name of set to create
     */
    synchronized public HTreeSetMaker hashSetCreate(String name){
        return new HTreeSetMaker(name);
    }


    synchronized protected <K> Set<K> hashSetCreate(HTreeSetMaker m){
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
            expireHeads = new long[HTreeMap.SEG];
            //$DELAY$
            expireTails = new long[HTreeMap.SEG];
            for(int i=0;i<HTreeMap.SEG;i++){
                expireHeads[i] = engine.put(0L,Serializer.LONG);
                expireTails[i] = engine.put(0L,Serializer.LONG);
            }
            catPut(name+".expireHeads",expireHeads);
            catPut(name+".expireTails",expireTails);
        }
        //$DELAY$
        Engine[] engines = HTreeMap.fillEngineArray(engine);

        long[] counterRecids = null;
        if(m.counter){
            counterRecids = new long[HTreeMap.SEG];
            for(int i=0;i<HTreeMap.SEG;i++){
                counterRecids[i] = engines[i].put(0L,Serializer.LONG);
            }
        }
        if(m.serializer==null) {
            m.serializer = getDefaultSerializer();
        }
        catPut(name+".serializer",serializableOrPlaceHolder(m.serializer));


        HTreeMap<K,Object> ret = new HTreeMap<K,Object>(
                engines,
                m.closeEngine,
                counterRecids == null ? null : catPut(name + ".counterRecids", counterRecids),
                catPut(name+".hashSalt", new SecureRandom().nextInt()), //TODO investigate if hashSalt actually prevents collision attack
                catPut(name+".segmentRecids",HTreeMap.preallocateSegments(engines)),
                (Serializer)m.serializer,
                null,
                expireTimeStart,expire,expireAccess,expireMaxSize, expireStoreSize, expireHeads ,expireTails,
                null,
                m.executor,
                m.executorPeriod,
                m.executor!=executor,
                consistencyLock.readLock()
                );
        Set<K> ret2 = ret.keySet();
        //$DELAY$
        catalog.put(name + ".type", "HashSet");
        namedPut(name, ret2);
        //$DELAY$


        //pump data if specified2
        if(m.pumpSource!=null) {
            Pump.fillHTreeMap(
                    ret,
                    m.pumpSource,
                    (Fun.Function1)Fun.extractNoTransform(),
                    null,
                    m.pumpPresortBatchSize,
                    m.pumpIgnoreDuplicates,
                    getDefaultSerializer(),
                    m.executor);
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
        private BTreeKeySerializer _keySerializer;
        private Serializer _keySerializer2;
        private Comparator _comparator;

        protected Serializer<?> valueSerializer;

        protected Iterator pumpSource;
        protected Fun.Function1 pumpKeyExtractor;
        protected Fun.Function1 pumpValueExtractor;
        protected int pumpPresortBatchSize = -1;
        protected boolean pumpIgnoreDuplicates = false;
        protected boolean closeEngine = false;

        protected Executor executor = DB.this.executor;


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
        public BTreeMapMaker keySerializer(BTreeKeySerializer<?,?> keySerializer){
            this._keySerializer = keySerializer;
            return this;
        }
        /**
         * keySerializer used to convert keys into/from binary form.
         */
        public BTreeMapMaker keySerializer(Serializer<?> serializer){
            this._keySerializer2 = serializer;
            return this;
        }

        /**
         * keySerializer used to convert keys into/from binary form.
         */
        public BTreeMapMaker keySerializer(Serializer<?> serializer, Comparator<?> comparator){
            this._keySerializer2 = serializer;
            this._comparator = comparator;
            return this;
        }

        /**
         * @deprecated compatibility with 1.0
         */
        public BTreeMapMaker keySerializerWrap(Serializer<?> serializer){
            return keySerializer(serializer);
        }


        /** valueSerializer used to convert values into/from binary form. */
        public BTreeMapMaker valueSerializer(Serializer<?> valueSerializer){
            this.valueSerializer = valueSerializer;
            return this;
        }

        /** comparator used to sort keys.  */
        public BTreeMapMaker comparator(Comparator<?> comparator){
            this._comparator = comparator;
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
         * If source iterator contains an duplicate key, exception is thrown.
         * This options will only use firts key and ignore any consequentive duplicates.
         */
        public <K> BTreeMapMaker pumpIgnoreDuplicates(){
            this.pumpIgnoreDuplicates = true;
            return this;
        }

        public <K,V> BTreeMap<K,V> make(){
            return DB.this.treeMapCreate(BTreeMapMaker.this);
        }

        public <K,V> BTreeMap<K,V> makeOrGet(){
            synchronized(DB.this){
                //TODO add parameter check
                return (BTreeMap<K, V>) (catGet(name+".type")==null?
                        make() :
                        treeMap(name,getKeySerializer(),valueSerializer));
            }
        }

        protected BTreeKeySerializer getKeySerializer() {
            if(_keySerializer==null) {
                if (_keySerializer2 == null && _comparator!=null)
                    _keySerializer2 = getDefaultSerializer();
                if(_keySerializer2!=null)
                    _keySerializer = _keySerializer2.getBTreeKeySerializer(_comparator);
            }
            return _keySerializer;
        }

        /**
         * creates map optimized for using {@code String} keys
         * @deprecated MapDB 1.0 compat, will be removed in 2.1
         */
        public <V> BTreeMap<String, V> makeStringMap() {
            keySerializer(Serializer.STRING);
            return make();
        }

        /**
         * creates map optimized for using zero or positive {@code Long} keys
         * @deprecated MapDB 1.0 compat, will be removed in 2.1
         */
        public <V> BTreeMap<Long, V> makeLongMap() {
            keySerializer(Serializer.LONG);
            return make();
        }

        protected BTreeMapMaker closeEngine() {
            closeEngine = true;
            return this;
        }
    }

    public class BTreeSetMaker{
        protected final String name;


        public BTreeSetMaker(String name) {
            this.name = name;
        }

        protected int nodeSize = 32;
        protected boolean counter = false;

        private BTreeKeySerializer _serializer;
        private Serializer _serializer2;
        private Comparator _comparator;

        protected Iterator<?> pumpSource;
        protected int pumpPresortBatchSize = -1;
        protected boolean pumpIgnoreDuplicates = false;
        protected boolean standalone = false;

        protected Executor executor = DB.this.executor;


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

        /** serializer used to convert keys into/from binary form. */
        public BTreeSetMaker serializer(BTreeKeySerializer serializer){
            this._serializer = serializer;
            return this;
        }


        /** serializer used to convert keys into/from binary form. */
        public BTreeSetMaker serializer(Serializer serializer){
            this._serializer2 = serializer;
            return this;
        }

        /** serializer used to convert keys into/from binary form. */
        public BTreeSetMaker serializer(Serializer serializer, Comparator comparator){
            this._serializer2 = serializer;
            this._comparator = comparator;
            return this;
        }
        /** comparator used to sort keys.  */
        public BTreeSetMaker comparator(Comparator<?> comparator){
            this._comparator = comparator;
            return this;
        }

        protected BTreeKeySerializer getSerializer() {
            if(_serializer==null) {
                if (_serializer2 == null && _comparator!=null)
                    _serializer2 = getDefaultSerializer();
                if(_serializer2!=null)
                    _serializer = _serializer2.getBTreeKeySerializer(_comparator);
            }
            return _serializer;
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

        protected BTreeSetMaker standalone() {
            this.standalone = true;
            return this;
        }


        public <K> NavigableSet<K> make(){
            return DB.this.treeSetCreate(BTreeSetMaker.this);
        }

        public <K> NavigableSet<K> makeOrGet(){
            synchronized (DB.this){
                //TODO add parameter check
                return (NavigableSet<K>) (catGet(name+".type")==null?
                        make():
                        treeSet(name,getSerializer()));
            }
        }




        /** creates set optimized for using {@code String}
         * @deprecated MapDB 1.0 compat, will be removed in 2.1
         */
        public NavigableSet<String> makeStringSet() {
            serializer(BTreeKeySerializer.STRING);
            return make();
        }

        /** creates set optimized for using zero or positive {@code Long}
         * @deprecated MapDB 1.0 compat, will be removed in 2.1
         */
        public NavigableSet<Long> makeLongSet() {
            serializer(BTreeKeySerializer.LONG);
            return make();
        }

    }


    /**
     * @deprecated method renamed, use {@link DB#treeMap(String)}
     */
    synchronized public <K,V> BTreeMap<K,V> getTreeMap(String name){
        return treeMap(name);
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
    synchronized public <K,V> BTreeMap<K,V> treeMap(String name) {
        return treeMap(name,(BTreeKeySerializer)null,null);
    }

    synchronized public <K,V> BTreeMap<K,V> treeMap(String name, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        if(keySerializer==null)
            keySerializer = getDefaultSerializer();
        return treeMap(name,keySerializer.getBTreeKeySerializer(null),valueSerializer);
    }

    synchronized public <K,V> BTreeMap<K,V> treeMap(String name, BTreeKeySerializer keySerializer, Serializer<V> valueSerializer){
        checkNotClosed();
        BTreeMap<K,V> ret = (BTreeMap<K,V>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        //$DELAY$
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0,false);
                new DB(e).treeMap("a");
                //$DELAY$
                return namedPut(name,
                        new DB(new Engine.ReadOnlyWrapper(e)).treeMap("a"));
            }
            return treeMapCreate(name).make();

        }
        checkType(type, "TreeMap");


        Object keySer2 = catGet(name+".keySerializer");
        if(keySerializer!=null){
            if(keySer2!=Fun.PLACEHOLDER && keySer2!=keySerializer){
                LOG.warning("Map '"+name+"' has keySerializer defined in Name Catalog, but other serializer was passed as constructor argument. Using one from constructor argument.");
            }
            keySer2 = keySerializer;
        }
        if(keySer2==Fun.PLACEHOLDER){
            throw new DBException.UnknownSerializer("Map '"+name+"' has no keySerializer defined in Name Catalog nor constructor argument.");
        }

        Object valSer2 = catGet(name+".valueSerializer");
        if(valueSerializer!=null){
            if(valSer2!=Fun.PLACEHOLDER && valSer2!=valueSerializer){
                LOG.warning("Map '"+name+"' has valueSerializer defined in name catalog, but other serializer was passed as constructor argument. Using one from constructor argument.");
            }
            valSer2 = valueSerializer;
        }
        if(valSer2==Fun.PLACEHOLDER) {
            throw new DBException.UnknownSerializer("Map '" + name + "' has no valueSerializer defined in Name Catalog nor constructor argument.");
        }

        ret = new BTreeMap<K, V>(engine,
                false,
                (Long) catGet(name + ".rootRecidRef"),
                catGet(name+".maxNodeSize",32),
                catGet(name+".valuesOutsideNodes",false),
                catGet(name+".counterRecids",0L),
                (BTreeKeySerializer)keySer2,
                (Serializer<V>)valSer2,
                catGet(name+".numberOfNodeMetas",0)
                );
        //$DELAY$
        namedPut(name, ret);
        return ret;
    }

    /**
     * @deprecated method renamed, use {@link DB#treeMapCreate(String)}
     */
    public BTreeMapMaker createTreeMap(String name){
        return treeMapCreate(name);
    }

    /**
     * Returns new builder for TreeMap with given name
     *
     * @param name of map to create
     * @throws IllegalArgumentException if name is already used
     * @return maker, call {@code .make()} to create map
     */
    public BTreeMapMaker treeMapCreate(String name){
        return new BTreeMapMaker(name);
    }

    synchronized protected <K,V> BTreeMap<K,V> treeMapCreate(final BTreeMapMaker m){
        String name = m.name;
        checkNameNotExists(name);
        //$DELAY$

        BTreeKeySerializer keySerializer = fillNulls(m.getKeySerializer());
        catPut(name+".keySerializer",serializableOrPlaceHolder(keySerializer));
        if(m.valueSerializer==null)
            m.valueSerializer = getDefaultSerializer();
        catPut(name+".valueSerializer",serializableOrPlaceHolder(m.valueSerializer));

        if(m.pumpPresortBatchSize!=-1 && m.pumpSource!=null){
            final Comparator comp = keySerializer.comparator();
            final Fun.Function1 extr = m.pumpKeyExtractor;

            Comparator presortComp =  new Comparator() {
                @Override
                public int compare(Object o1, Object o2) {
                    return - comp.compare(extr.run(o1), extr.run(o2));
                }
            };

            m.pumpSource = Pump.sort(
                    m.pumpSource,
                    m.pumpIgnoreDuplicates,
                    m.pumpPresortBatchSize,
                    presortComp,
                    getDefaultSerializer(),
                    m.executor);
        }
        //$DELAY$
        long counterRecid = !m.counter ?0L:engine.put(0L, Serializer.LONG);

        long rootRecidRef;
        if(m.pumpSource==null || !m.pumpSource.hasNext()){
            rootRecidRef = BTreeMap.createRootRef(engine,keySerializer,m.valueSerializer,0);
        }else{
            rootRecidRef = Pump.buildTreeMap(
                    (Iterator<K>)m.pumpSource,
                    engine,
                    (Fun.Function1<K,K>)m.pumpKeyExtractor,
                    (Fun.Function1<V,K>)m.pumpValueExtractor,
                    m.pumpIgnoreDuplicates,m.nodeSize,
                    m.valuesOutsideNodes,
                    counterRecid,
                    keySerializer,
                    (Serializer<V>)m.valueSerializer,
                    m.executor
            );

        }
        //$DELAY$
        BTreeMap<K,V> ret = new BTreeMap<K,V>(
                engine,
                m.closeEngine,
                catPut(name+".rootRecidRef", rootRecidRef),
                catPut(name+".maxNodeSize",m.nodeSize),
                catPut(name+".valuesOutsideNodes",m.valuesOutsideNodes),
                catPut(name+".counterRecids",counterRecid),
                keySerializer,
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
    protected BTreeKeySerializer<?,?> fillNulls(BTreeKeySerializer<?,?> keySerializer) {
        if(keySerializer==null)
            return new BTreeKeySerializer.BasicKeySerializer(getDefaultSerializer(),Fun.COMPARATOR);
        if(keySerializer instanceof BTreeKeySerializer.ArrayKeySerializer) {
            BTreeKeySerializer.ArrayKeySerializer k = (BTreeKeySerializer.ArrayKeySerializer) keySerializer;

            Serializer<?>[] serializers = new Serializer[k.tsize];
            Comparator<?>[] comparators = new Comparator[k.tsize];
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
     * @deprecated method renamed, use {@link DB#treeSet(String)}
     */
    synchronized public <K> NavigableSet<K> getTreeSet(String name){
        return treeSet(name);
    }
    /**
     * Opens existing or creates new B-linked-tree Set.
     *
     * @param name of set
     * @return set
     */
    synchronized public <K> NavigableSet<K> treeSet(String name) {
        return treeSet(name, null);
    }
    synchronized public <K> NavigableSet<K> treeSet(String name,BTreeKeySerializer serializer){
        checkNotClosed();
        NavigableSet<K> ret = (NavigableSet<K>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0,false);
                new DB(e).treeSet("a");
                return namedPut(name,
                        new DB(new Engine.ReadOnlyWrapper(e)).treeSet("a"));
            }
            //$DELAY$
            return treeSetCreate(name).make();

        }
        checkType(type, "TreeSet");

        Object keySer2 = catGet(name+".serializer");
        if(serializer!=null){
            if(keySer2!=Fun.PLACEHOLDER && keySer2!=serializer){
                LOG.warning("Set '"+name+"' has serializer defined in Name Catalog, but other serializer was passed as constructor argument. Using one from constructor argument.");
            }
            keySer2 = serializer;
        }
        if(keySer2==Fun.PLACEHOLDER){
            throw new DBException.UnknownSerializer("Set '"+name+"' has no serializer defined in Name Catalog nor constructor argument.");
        }


        //$DELAY$
        ret = new BTreeMap<K, Object>(
                engine,
                false,
                (Long) catGet(name+".rootRecidRef"),
                catGet(name+".maxNodeSize",32),
                false,
                catGet(name+".counterRecids",0L),
                (BTreeKeySerializer)keySer2,
                null,
                catGet(name+".numberOfNodeMetas",0)
        ).keySet();
        //$DELAY$
        namedPut(name, ret);
        return ret;

    }

    /**
     * @deprecated method renamed, use {@link DB#treeSetCreate(String)}
     */
    synchronized public BTreeSetMaker createTreeSet(String name){
        return treeSetCreate(name);
    }

    /**
     * Creates new TreeSet.
     * @param name of set to create
     * @throws IllegalArgumentException if name is already used
     * @return maker used to construct set
     */
    synchronized public BTreeSetMaker treeSetCreate(String name){
         return new BTreeSetMaker(name);
    }

    synchronized public <K> NavigableSet<K> treeSetCreate(BTreeSetMaker m){
        checkNameNotExists(m.name);
        //$DELAY$

        BTreeKeySerializer serializer = fillNulls(m.getSerializer());
        catPut(m.name+".serializer",serializableOrPlaceHolder(serializer));

        if(m.pumpPresortBatchSize!=-1){
            m.pumpSource = Pump.sort(
                    m.pumpSource,
                    m.pumpIgnoreDuplicates,
                    m.pumpPresortBatchSize,
                    Collections.reverseOrder(serializer.comparator()),
                    getDefaultSerializer(),
                    m.executor);
        }

        long counterRecid = !m.counter ?0L:engine.put(0L, Serializer.LONG);
        long rootRecidRef;
        //$DELAY$
        if(m.pumpSource==null || !m.pumpSource.hasNext()){
            rootRecidRef = BTreeMap.createRootRef(engine,serializer,null,0);
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
                    serializer,
                    null,
                    m.executor);
        }
        //$DELAY$
        NavigableSet<K> ret = new BTreeMap<K,Object>(
                engine,
                m.standalone,
                catPut(m.name+".rootRecidRef", rootRecidRef),
                catPut(m.name+".maxNodeSize",m.nodeSize),
                false,
                catPut(m.name+".counterRecids",counterRecid),
                serializer,
                null,
                catPut(m.name+".numberOfNodeMetas",0)
        ).keySet();
        //$DELAY$
        catalog.put(m.name + ".type", "TreeSet");
        namedPut(m.name, ret);
        return ret;
    }

    /** @deprecated queues API is going to be reworked */
    synchronized public <E> BlockingQueue<E> getQueue(String name) {
        checkNotClosed();
        Queues.Queue<E> ret = (Queues.Queue<E>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        //$DELAY$
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0,false);
                new DB(e).getQueue("a");
                return namedPut(name,
                        new DB(new Engine.ReadOnlyWrapper(e)).getQueue("a"));
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


    /** @deprecated queues API is going to be reworked */
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


    /** @deprecated queues API is going to be reworked */
    synchronized public <E> BlockingQueue<E> getStack(String name) {
        checkNotClosed();
        Queues.Stack<E> ret = (Queues.Stack<E>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        //$DELAY$
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0,false);
                //$DELAY$
                new DB(e).getStack("a");
                return namedPut(name,
                        new DB(new Engine.ReadOnlyWrapper(e)).getStack("a"));
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



    /** @deprecated queues API is going to be reworked */
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


    /** @deprecated queues API is going to be reworked */
    synchronized public <E> BlockingQueue<E> getCircularQueue(String name) {
        checkNotClosed();
        BlockingQueue<E> ret = (BlockingQueue<E>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        //$DELAY$
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()) {
                Engine e = new StoreHeap(true,1,0,false);
                new DB(e).getCircularQueue("a");
                //$DELAY$
                return namedPut(name,
                        new DB(new Engine.ReadOnlyWrapper(e)).getCircularQueue("a"));
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



    /** @deprecated queues API is going to be reworked */
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

    /**
     * @deprecated method renamed, use {@link DB#atomicLongCreate(String, long)}
     */
    synchronized public Atomic.Long createAtomicLong(String name, long initValue){
        return atomicLongCreate(name, initValue);
    }

    synchronized public Atomic.Long atomicLongCreate(String name, long initValue){
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

    /**
     * @deprecated method renamed, use {@link DB#atomicLong(String)}
     */
    synchronized public Atomic.Long getAtomicLong(String name){
        return atomicLong(name);
    }

    synchronized public Atomic.Long atomicLong(String name){
        checkNotClosed();
        Atomic.Long ret = (Atomic.Long) getFromWeakCollection(name);
        if(ret!=null) return ret;
        //$DELAY$
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            if (engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0,false);
                new DB(e).atomicLong("a");
                //$DELAY$
                return namedPut(name,
                        new DB(new Engine.ReadOnlyWrapper(e)).atomicLong("a"));
            }
            return atomicLongCreate(name, 0L);
        }
        checkType(type, "AtomicLong");
        //$DELAY$
        ret = new Atomic.Long(engine, (Long) catGet(name+".recid"));
        namedPut(name, ret);
        return ret;
    }




    /**
     * @deprecated method renamed, use {@link DB#atomicIntegerCreate(String, int)}
     */
    synchronized public Atomic.Integer createAtomicInteger(String name, int initValue){
        return atomicIntegerCreate(name,initValue);
    }

    synchronized public Atomic.Integer atomicIntegerCreate(String name, int initValue){
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

    /**
     * @deprecated method renamed, use {@link DB#atomicInteger(String)}
     */
    synchronized public Atomic.Integer getAtomicInteger(String name){
        return atomicInteger(name);
    }

    synchronized public Atomic.Integer atomicInteger(String name){
        checkNotClosed();
        Atomic.Integer ret = (Atomic.Integer) getFromWeakCollection(name);
        if(ret!=null) return ret;
        //$DELAY$
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0,false);
                new DB(e).atomicInteger("a");
                //$DELAY$
                return namedPut(name,
                        new DB(new Engine.ReadOnlyWrapper(e)).atomicInteger("a"));
            }
            return atomicIntegerCreate(name, 0);
        }
        checkType(type, "AtomicInteger");

        ret = new Atomic.Integer(engine, (Long) catGet(name+".recid"));
        namedPut(name, ret);
        return ret;
    }


    /**
     * @deprecated method renamed, use {@link DB#atomicBooleanCreate(String, boolean)}
     */
    synchronized public Atomic.Boolean createAtomicBoolean(String name, boolean initValue){
        return atomicBooleanCreate(name, initValue);
    }

    synchronized public Atomic.Boolean atomicBooleanCreate(String name, boolean initValue){
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

    /**
     * @deprecated method renamed, use {@link DB#atomicBoolean(String)}
     */
    synchronized public Atomic.Boolean getAtomicBoolean(String name){
        return atomicBoolean(name);
    }

    synchronized public Atomic.Boolean atomicBoolean(String name){
        checkNotClosed();
        Atomic.Boolean ret = (Atomic.Boolean) getFromWeakCollection(name);
        if(ret!=null) return ret;
        //$DELAY$
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0,false);
                new DB(e).atomicBoolean("a");
                return namedPut(name,
                        new DB(new Engine.ReadOnlyWrapper(e)).atomicBoolean("a"));
            }
            //$DELAY$
            return atomicBooleanCreate(name, false);
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

    /**
     * @deprecated method renamed, use {@link DB#atomicStringCreate(String, String)}
     */
    synchronized public Atomic.String createAtomicString(String name, String initValue){
        return atomicStringCreate(name,initValue);
    }

    synchronized public Atomic.String atomicStringCreate(String name, String initValue){
        checkNameNotExists(name);
        if(initValue==null) throw new IllegalArgumentException("initValue may not be null");
        long recid = engine.put(initValue, Serializer.STRING_NOSIZE);
        //$DELAY$
        Atomic.String ret = new Atomic.String(engine,
                catPut(name+".recid",recid)
        );
        //$DELAY$
        catalog.put(name + ".type", "AtomicString");
        namedPut(name, ret);
        return ret;

    }

    /**
     * @deprecated method renamed, use {@link DB#atomicString(String)}
     */
    synchronized public Atomic.String getAtomicString(String name) {
        return atomicString(name);
    }

    synchronized public Atomic.String atomicString(String name){
        checkNotClosed();
        Atomic.String ret = (Atomic.String) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        //$DELAY$
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0,false);
                new DB(e).atomicString("a");
                //$DELAY$
                return namedPut(name,
                        new DB(new Engine.ReadOnlyWrapper(e)).atomicString("a"));
            }
            return atomicStringCreate(name, "");
        }
        checkType(type, "AtomicString");

        ret = new Atomic.String(engine, (Long) catGet(name+".recid"));
        namedPut(name, ret);
        return ret;
    }

    /**
     * @deprecated method renamed, use {@link DB#atomicVarCreate(String, Object, Serializer)}
     */
    synchronized public <E> Atomic.Var<E> createAtomicVar(String name, E initValue, Serializer<E> serializer){
        return atomicVarCreate(name,initValue,serializer);
    }

    synchronized public <E> Atomic.Var<E> atomicVarCreate(String name, E initValue, Serializer<E> serializer){
        if(catGet(name+".type")!=null){
            return atomicVar(name,serializer);
        }

        if(serializer==null)
            serializer=getDefaultSerializer();

        catPut(name+".serializer",serializableOrPlaceHolder(serializer));

        long recid = engine.put(initValue, serializer);
        //$DELAY$
        Atomic.Var ret = new Atomic.Var(engine,
                catPut(name+".recid",recid),
                serializer
        );
        //$DELAY$
        catalog.put(name + ".type", "AtomicVar");
        namedPut(name, ret);
        return ret;

    }

    /**
     * @deprecated method renamed, use {@link DB#atomicVar(String)}
     */
    synchronized public <E> Atomic.Var<E> getAtomicVar(String name){
        return atomicVar(name);
    }

    synchronized public <E> Atomic.Var<E> atomicVar(String name){
        return atomicVar(name,null);
    }

    synchronized public <E> Atomic.Var<E> atomicVar(String name,Serializer<E> serializer){
        checkNotClosed();

        Atomic.Var ret = (Atomic.Var) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0,false);
                new DB(e).atomicVar("a");
                return namedPut(name,
                        new DB(new Engine.ReadOnlyWrapper(e)).atomicVar("a"));
            }
            //$DELAY$
            return atomicVarCreate(name, null, getDefaultSerializer());
        }
        checkType(type, "AtomicVar");
        Object serializer2;
        if(serializer==null)
            serializer2 = catGet(name+".serializer");
        else
            serializer2 = serializer;

        if(serializer2==null)
            serializer2 = getDefaultSerializer();

        if(serializer2==Fun.PLACEHOLDER){
            throw new DBException.UnknownSerializer("Atomic.Var '"+name+"' has no serializer defined in Name Catalog nor constructor argument.");
        }

        ret = new Atomic.Var(engine, (Long) catGet(name+".recid"), (Serializer) serializer2);
        namedPut(name, ret);
        return ret;
    }

    /** return record with given name or null if name does not exist*/
    synchronized public <E> E get(String name){
        //$DELAY$
        String type = catGet(name+".type");
        if(type==null) return null;
        if("HashMap".equals(type)) return (E) hashMap(name);
        if("HashSet".equals(type)) return (E) hashSet(name);
        if("TreeMap".equals(type)) return (E) treeMap(name);
        if("TreeSet".equals(type)) return (E) treeSet(name);
        if("AtomicBoolean".equals(type)) return (E) atomicBoolean(name);
        if("AtomicInteger".equals(type)) return (E) atomicInteger(name);
        if("AtomicLong".equals(type)) return (E) atomicLong(name);
        if("AtomicString".equals(type)) return (E) atomicString(name);
        if("AtomicVar".equals(type)) return (E) atomicVar(name);
        if("Queue".equals(type)) return (E) getQueue(name);
        if("Stack".equals(type)) return (E) getStack(name);
        if("CircularQueue".equals(type)) return (E) getCircularQueue(name);
        throw new DBException.DataCorruption("Unknown type: "+name);
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
     * <p>
     * Closes database.
     * All other methods will throw 'IllegalAccessError' after this method was called.
     * </p><p>
     * !! it is necessary to call this method before JVM exits!!
     * </p>
     */
    synchronized public void close(){
        if(engine == null)
            return;

        consistencyLock.writeLock().lock();
        try {

            if(metricsExecutor!=null && metricsExecutor!=executor && !metricsExecutor.isShutdown()){
                metricsExecutor.shutdown();
                metricsExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                metricsExecutor = null;
            }

            if(cacheExecutor!=null && cacheExecutor!=executor && !cacheExecutor.isShutdown()){
                cacheExecutor.shutdown();
                cacheExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                cacheExecutor = null;
            }

            if(storeExecutor!=null && storeExecutor!=executor && !storeExecutor.isShutdown()){
                storeExecutor.shutdown();
                storeExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                storeExecutor = null;
            }


            if (executor != null && !executor.isTerminated()) {
                executor.shutdown();
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                executor = null;
            }

            for (WeakReference r : namesInstanciated.values()) {
                Object rr = r.get();
                if (rr != null && rr instanceof Closeable)
                    ((Closeable) rr).close();
            }

            String fileName = deleteFilesAfterClose ? Store.forEngine(engine).fileName : null;
            engine.close();
            //dereference db to prevent memory leaks
            engine = Engine.CLOSED_ENGINE;
            namesInstanciated = Collections.unmodifiableMap(new HashMap());
            namesLookup = Collections.unmodifiableMap(new HashMap());

            if (deleteFilesAfterClose && fileName != null) {
                File f = new File(fileName);
                if (f.exists() && !f.delete()) {
                    //TODO file was not deleted, log warning
                }
                //TODO delete WAL files and append-only files
            }
        } catch (IOException e) {
            throw new IOError(e);
        } catch (InterruptedException e) {
            throw new DBException.Interrupted(e);
        }finally {
            consistencyLock.writeLock().unlock();
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

        consistencyLock.writeLock().lock();
        try {
            //update Class Catalog with missing classes as part of this transaction
            String[] toBeAdded = unknownClasses.isEmpty() ? null : unknownClasses.toArray(new String[0]);

            //TODO if toBeAdded is modified as part of serialization, and `executor` is not null (background threads are enabled),
            // schedule this operation with 1ms delay, so it has higher chances of becoming part of the same transaction
            if (toBeAdded != null) {
                long[] classInfoRecids = engine.get(Engine.RECID_CLASS_CATALOG, Serializer.RECID_ARRAY);
                long[] classInfoRecidsOrig = classInfoRecids;
                if(classInfoRecids==null)
                    classInfoRecids = new long[0];

                int pos = classInfoRecids.length;
                classInfoRecids = Arrays.copyOf(classInfoRecids,classInfoRecids.length+toBeAdded.length);

                final ClassLoader classLoader = SerializerPojo.classForNameClassLoader();
                for (String className : toBeAdded) {
                    SerializerPojo.ClassInfo classInfo = SerializerPojo.makeClassInfo(classLoader, className);
                    //persist and add new recids
                    classInfoRecids[pos++] = engine.put(classInfo,SerializerPojo.CLASS_INFO_SERIALIZER);
                }
                if(!engine.compareAndSwap(Engine.RECID_CLASS_CATALOG, classInfoRecidsOrig, classInfoRecids, Serializer.RECID_ARRAY)){
                    LOG.log(Level.WARNING, "Could not update class catalog with new classes, CAS failed");
                }
            }


            engine.commit();

            if (toBeAdded != null) {
                for (String className : toBeAdded) {
                    unknownClasses.remove(className);
                }
            }
        }finally {
            consistencyLock.writeLock().unlock();
        }
    }

    /**
     * Rollback changes made on collections loaded by this DB
     *
     * @see org.mapdb.Engine#rollback()
     */
    synchronized public void rollback() {
        checkNotClosed();
        consistencyLock.writeLock().lock();
        try {
            engine.rollback();
        }finally {
            consistencyLock.writeLock().unlock();
        }
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
        consistencyLock.writeLock().lock();
        try {
            Engine snapshot = TxEngine.createSnapshotFor(engine);
            return new DB(snapshot);
        }finally {
            consistencyLock.writeLock().unlock();
        }
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

    /**
     * Returns consistency lock which groups operation together and ensures consistency.
     * Operations which depends on each other are performed under read lock.
     * Snapshots, close etc are performed under write-lock.
     *
     * @return
     */
    public ReadWriteLock consistencyLock(){
        return consistencyLock;
    }


}
