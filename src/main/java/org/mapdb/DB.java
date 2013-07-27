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
import java.util.concurrent.TimeUnit;

/**
 * A database with easy access to named maps and other collections.
 *
 * @author Jan Kotek
 */
@SuppressWarnings("unchecked")
public class DB {

    protected final boolean strictDBGet;

    /** Engine which provides persistence for this DB*/
    protected Engine engine;
    /** already loaded named collections. It is important to keep collections as singletons, because of 'in-memory' locking*/
    protected Map<String, WeakReference<?>> collections = new HashMap<String, WeakReference<?>>();

    /** view over named records */
    protected SortedMap<String, Object> catalog;

    /** default serializer used for persistence. Handles POJO and other stuff which requires write-able access to Engine */
    protected Serializer<?> defaultSerializer;

    /**
     * Construct new DB. It is just thin layer over {@link Engine} which does the real work.
     * @param engine
     */
    public DB(final Engine engine){
        this(engine,false);
    }

    public DB(Engine engine, boolean strictDBGet) {
        this.engine = engine;
        this.strictDBGet = strictDBGet;
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
        catalog = BTreeMap.preinitCatalog(this);
    }

    protected <A> A catGet(String name, A init){
        A ret = (A) catalog.get(name);
        return ret!=null? ret : init;
    }


    protected <A> A catGet(String name){
        return (A) catalog.get(name);
    }

    protected <A> A catPut(String name, A value){
        catalog.put(name, value);
        return value;
    }

    protected <A> A catPut(String name, A value, A retValueIfNull){
        if(value==null) return retValueIfNull;
        catalog.put(name, value);
        return value;
    }



    public class HTreeMapMaker{
        protected final String name;

        public HTreeMapMaker(String name) {
            this.name = name;
        }


        protected boolean keepCounter = false;
        protected Serializer keySerializer = null;
        protected Serializer valueSerializer = null;
        protected long expireMaxSize = 0L;
        protected long expire = 0L;
        protected long expireAccess = 0L;

        protected Fun.Function1 valueCreator = null;




        /** keepCounter if counter should be kept, without counter updates are faster, but entire collection needs to be traversed to count items.*/
        public HTreeMapMaker keepCounter(boolean keepCounter){
            this.keepCounter = keepCounter;
            return this;
        }



        /** keySerializer used to convert keys into/from binary form. */
        public HTreeMapMaker keySerializer(Serializer keySerializer){
            this.keySerializer = keySerializer;
            return this;
        }

        /** valueSerializer used to convert values into/from binary form. */
        public HTreeMapMaker valueSerializer(Serializer valueSerializer){
            this.valueSerializer = valueSerializer;
            return this;
        }

        /** maximal number of entries in this map. Less used entries will be expired and removed to make collection smaller  */
        public HTreeMapMaker expireMaxSize(long maxSize){
            this.expireMaxSize = maxSize;
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

        /** If value is not found, HTreeMap can fetch and insert default value. `valueCreator` is used to return new value.
         * This way `HTreeMap.get()` never returns null */
        public HTreeMapMaker valueCreator(Fun.Function1 valueCreator){
            this.valueCreator = valueCreator;
            return this;
        }


        public <K,V> HTreeMap<K,V> make(){
            if(expireMaxSize!=0) keepCounter=true;
            return DB.this.createHashMap(HTreeMapMaker.this);
        }


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
        return getHashMap(name, null);
    }

    /**
     * Opens existing or creates new Hash Tree Map.
     * This collection perform well under concurrent access.
     * Is best for large keys and large values.
     *
     * @param name of map
     * @param valueCreator if value is not found, new is created and placed into map.
     * @param <K> key
     * @param <V> value
     * @return map
     */
    synchronized public <K,V> HTreeMap<K,V> getHashMap(String name, Fun.Function1<V,K> valueCreator){
        checkNotClosed();
        HTreeMap<K,V> ret = (HTreeMap<K, V>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            return createHashMap(name).make();
        }


        //check type
        checkType(type, "HashMap");
        //open existing map
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
                (long[])catGet(name+".expireHeads",null),
                (long[])catGet(name+".expireTails",null),
                valueCreator
        );


        collections.put(name, new WeakReference<Object>(ret));
        return ret;
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
     * @param <K> key type
     * @param <V> value type
     * @throws IllegalArgumentException if name is already used
     * @return newly created map
     */
    synchronized protected <K,V> HTreeMap<K,V> createHashMap(HTreeMapMaker m){
        String name = m.name;
        checkNameNotExists(name);

        long expireTimeStart=0, expire=0, expireAccess=0, expireMaxSize = 0;
        long[] expireHeads=null, expireTails=null;

        if(m.expire!=0 || m.expireAccess!=0 || m.expireMaxSize !=0){
            expireTimeStart = catPut(name+".expireTimeStart",System.currentTimeMillis());
            expire = catPut(name+".expire",m.expire);
            expireAccess = catPut(name+".expireAccess",m.expireAccess);
            expireMaxSize = catPut(name+".expireMaxSize",m.expireMaxSize);
            expireHeads = new long[16];
            expireTails = new long[16];
            for(int i=0;i<16;i++){
                expireHeads[i] = engine.put(0L,Serializer.LONG_SERIALIZER);
                expireTails[i] = engine.put(0L,Serializer.LONG_SERIALIZER);
            }
            catPut(name+".expireHeads",expireHeads);
            catPut(name+".expireTails",expireHeads);

        }


        HTreeMap<K,V> ret = new HTreeMap<K,V>(engine,
                catPut(name+".counterRecid",!m.keepCounter?0L:engine.put(0L, Serializer.LONG_SERIALIZER)),
                catPut(name+".hashSalt",Utils.RANDOM.nextInt()),
                catPut(name+".segmentRecids",HTreeMap.preallocateSegments(engine)),
                catPut(name+".keySerializer",m.keySerializer,getDefaultSerializer()),
                catPut(name+".valueSerializer",m.valueSerializer,getDefaultSerializer()),
                expireTimeStart,expire,expireAccess,expireMaxSize, expireHeads ,expireTails,
                m.valueCreator

        );

        catalog.put(name + ".type", "HashMap");
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
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            return createHashSet(name,false,null);
        }


        //check type
        checkType(type, "HashSet");
        //open existing map
        ret = new HTreeMap(engine,
                (Long)catGet(name+".counterRecid"),
                (Integer)catGet(name+".hashSalt"),
                (long[])catGet(name+".segmentRecids"),
                catGet(name+".serializer",getDefaultSerializer()),
                null, 0L,0L,0L,0L,null,null,null
        ).keySet();


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



        Set<K> ret = new HTreeMap<K,Object>(engine,
                catPut(name+".counterRecid",!keepCounter?0L:engine.put(0L, Serializer.LONG_SERIALIZER)),
                catPut(name+".hashSalt",Utils.RANDOM.nextInt()),
                catPut(name+".segmentRecids",HTreeMap.preallocateSegments(engine)),
                catPut(name+".serializer",serializer,getDefaultSerializer()),
                null, 0L,0L,0L,0L,null,null,null
        ).keySet();

        catalog.put(name + ".type", "HashSet");
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }



    public class BTreeMapMaker{
        protected final String name;

        public BTreeMapMaker(String name) {
            this.name = name;
        }

        protected int nodeSize = 32;
        protected boolean valuesStoredOutsideNodes = false;
        protected boolean keepCounter = false;
        protected BTreeKeySerializer keySerializer;
        protected Serializer valueSerializer;
        protected Comparator comparator;


        /** nodeSize maximal size of node, larger node causes overflow and creation of new BTree node. Use large number for small keys, use small number for large keys.*/
        public BTreeMapMaker nodeSize(int nodeSize){
            this.nodeSize = nodeSize;
            return this;
        }

        /** valuesStoredOutsideNodes if true, values are stored outside of BTree nodes. Use 'true' if your values are large.*/
        public BTreeMapMaker valuesStoredOutsideNodes(boolean outside){
            this.valuesStoredOutsideNodes = outside;
            return this;
        }

        /** keepCounter if counter should be kept, without counter updates are faster, but entire collection needs to be traversed to count items.*/
        public BTreeMapMaker keepCounter(boolean keepCounter){
            this.keepCounter = keepCounter;
            return this;
        }

        /** keySerializer used to convert keys into/from binary form. */
        public BTreeMapMaker keySerializer(BTreeKeySerializer keySerializer){
            this.keySerializer = keySerializer;
            return this;
        }

        /** valueSerializer used to convert values into/from binary form. */
        public BTreeMapMaker valueSerializer(Serializer valueSerializer){
            this.valueSerializer = valueSerializer;
            return this;
        }

        /** comparator used to sort keys.  */
        public BTreeMapMaker comparator(Comparator comparator){
            this.comparator = comparator;
            return this;
        }


        public <K,V> BTreeMap<K,V> make(){
            return DB.this.createTreeMap(BTreeMapMaker.this);
        }

        /** creates map optimized for using `String` keys */
        public <V> Map<String, V> makeStringMap() {
            keySerializer = BTreeKeySerializer.STRING;
            return make();
        }

        /** creates map optimized for using zero or positive `Long` keys */
        public <V> Map<Long, V> makeLongMap() {
            keySerializer = BTreeKeySerializer.ZERO_OR_POSITIVE_LONG;
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
     * @param <K> key
     * @param <V> value
     * @return map
     */
    synchronized public <K,V> BTreeMap<K,V> getTreeMap(String name){
        checkNotClosed();
        BTreeMap<K,V> ret = (BTreeMap<K,V>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            return createTreeMap(name).make();

        }
        checkType(type, "TreeMap");

        ret = new BTreeMap<K, V>(engine,
                (Long) catGet(name + ".rootRecidRef"),
                catGet(name+".maxNodeSize",32),
                catGet(name+".valuesOutsideNodes",false),
                catGet(name+".counterRecid",0L),
                (BTreeKeySerializer)catGet(name+".keySerializer",new BTreeKeySerializer.BasicKeySerializer(getDefaultSerializer())),
                catGet(name+".valueSerializer",getDefaultSerializer()),
                (Comparator)catGet(name+".comparator",Utils.COMPARABLE_COMPARATOR)
                );
        collections.put(name, new WeakReference<Object>(ret));
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


    synchronized protected <K,V> BTreeMap<K,V> createTreeMap(BTreeMapMaker m){
        String name = m.name;
        checkNameNotExists(name);
        m.keySerializer = fillNulls(m.keySerializer);
        m.keySerializer = (BTreeKeySerializer)catPut(name+".keySerializer",m.keySerializer,new BTreeKeySerializer.BasicKeySerializer(getDefaultSerializer()));
        m.valueSerializer = catPut(name+".valueSerializer",m.valueSerializer,getDefaultSerializer());
        m.comparator = (Comparator)catPut(name+".comparator",m.comparator,Utils.COMPARABLE_COMPARATOR);
        BTreeMap<K,V> ret = new BTreeMap<K,V>(engine,
                catPut(name+".rootRecidRef", BTreeMap.createRootRef(engine,m.keySerializer,m.valueSerializer,m.comparator)),
                catPut(name+".maxNodeSize",m.nodeSize),
                catPut(name+".valuesOutsideNodes",m.valuesStoredOutsideNodes),
                catPut(name+".counterRecid",!m.keepCounter?0L:engine.put(0L, Serializer.LONG_SERIALIZER)),
                m.keySerializer,
                m.valueSerializer,
                m.comparator
        );
        catalog.put(name + ".type", "TreeMap");
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
    public SortedMap<String, Object> getCatalog(){
        return catalog;
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
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            return createTreeSet(name,32,false,null, null);

        }
        checkType(type, "TreeSet");

        ret = new BTreeMap<K, Object>(engine,
                (Long) catGet(name+".rootRecidRef"),
                catGet(name+".maxNodeSize",32),
                false,
                catGet(name+".counterRecid",0L),
                (BTreeKeySerializer) catGet(name+".keySerializer",new BTreeKeySerializer.BasicKeySerializer(getDefaultSerializer())),
                null,
                (Comparator) catGet(name+".comparator",Utils.COMPARABLE_COMPARATOR)
        ).keySet();

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
        serializer = (BTreeKeySerializer)catPut(name+".keySerializer",serializer,new BTreeKeySerializer.BasicKeySerializer(getDefaultSerializer()));
        comparator = (Comparator)catPut(name+".comparator",comparator,Utils.COMPARABLE_COMPARATOR);
        NavigableSet<K> ret = new BTreeMap<K,Object>(engine,
                catPut(name+".rootRecidRef", BTreeMap.createRootRef(engine,serializer, null,comparator)),
                catPut(name+".maxNodeSize",nodeSize),
                false,
                catPut(name+".counterRecid",!keepCounter?0L:engine.put(0L, Serializer.LONG_SERIALIZER)),
                serializer,
                null,
                comparator
        ).keySet();
        catalog.put(name + ".type", "TreeSet");
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }

//    synchronized public <E> Queue<E> getQueue(String name){
//        Long recid = catalog.get(name);
//        if(recid==null){
//
//        }else{
//            return new Queues.Lifo<E>(engine, getDefaultSerializer(),  recid, true);
//        }
//    }


    synchronized public <E> Queue<E> getQueue(String name) {
        checkNotClosed();
        Queues.Queue<E> ret = (Queues.Queue<E>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            return createQueue(name,null);
        }
        checkType(type, "Queue");

        ret = new Queues.Queue<E>(engine,
                (Serializer<E>) catGet(name+".serializer",getDefaultSerializer()),
                (Long)catGet(name+".headRecid"),
                (Long)catGet(name+".nextTailRecid"),
                (Long)catGet(name+".sizeRecid")
                );

        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }

    synchronized public <E> Queue<E> createQueue(String name, Serializer<E> serializer) {
        checkNameNotExists(name);

        long headerRecid = engine.put(0L, Serializer.LONG_SERIALIZER);
        long nextTail = engine.put(Queues.SimpleQueue.Node.EMPTY, new Queues.SimpleQueue.NodeSerializer(null));
        long nextTailRecid = engine.put(nextTail, Serializer.LONG_SERIALIZER);
        long sizeRecid = engine.put(0L, Serializer.LONG_SERIALIZER);

        Queues.Queue<E> ret = new Queues.Queue<E>(engine,
                catPut(name+".serializer",serializer,getDefaultSerializer()),
                catPut(name+".headRecid",headerRecid),
                catPut(name+".nextTailRecid",nextTailRecid),
                catPut(name+".sizeRecid",sizeRecid)
                );
        catalog.put(name + ".type", "Queue");
        collections.put(name, new WeakReference<Object>(ret));
        return ret;

    }


    synchronized public <E> Queue<E> getStack(String name) {
        checkNotClosed();
        Queues.Stack<E> ret = (Queues.Stack<E>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            return createStack(name,null,true);
        }

        checkType(type, "Stack");

        ret = new Queues.Stack<E>(engine,
                (Serializer<E>) catGet(name+".serializer",getDefaultSerializer()),
                (Long)catGet(name+".headRecid"),
                (Boolean)catGet(name+".useLocks")
        );

        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }



    synchronized public <E> Queue<E> createStack(String name, Serializer<E> serializer, boolean useLocks) {
        checkNameNotExists(name);

        long headerRecid = engine.put(0L, Serializer.LONG_SERIALIZER);


        Queues.Stack<E> ret = new Queues.Stack<E>(engine,
                catPut(name+".serializer",serializer,getDefaultSerializer()),
                catPut(name+".headRecid",headerRecid),
                catPut(name+".useLocks",useLocks)
        );
        catalog.put(name + ".type", "Stack");
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }


    synchronized public <E> Queue<E> getCircularQueue(String name) {
        checkNotClosed();
        Queue<E> ret = (Queue<E>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            return createCircularQueue(name,null, 1024);
        }

        checkType(type, "CircularQueue");

        ret = new Queues.CircularQueue<E>(engine,
                (Serializer<E>) catGet(name+".serializer",getDefaultSerializer()),
                (Long)catGet(name+".headRecid"),
                (Long)catGet(name+".headInsertRecid"),
                (Long)catGet(name+".size")
        );

        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }



    synchronized public <E> Queue<E> createCircularQueue(String name, Serializer<E> serializer, long size) {
        checkNameNotExists(name);
        if(serializer==null) serializer = getDefaultSerializer();

//        long headerRecid = engine.put(0L, Serializer.LONG_SERIALIZER);
        //insert N Nodes empty nodes into a circle
        long prevRecid = 0;
        long firstRecid = 0;
        Serializer<Queues.SimpleQueue.Node> nodeSer = new Queues.SimpleQueue.NodeSerializer(serializer);
        for(long i=0;i<size;i++){
            Queues.SimpleQueue.Node n = new Queues.SimpleQueue.Node(prevRecid, null);
            prevRecid = engine.put(n, nodeSer);
            if(firstRecid==0) firstRecid = prevRecid;
        }
        //update first node to point to last recid
        engine.update(firstRecid, new Queues.SimpleQueue.Node(prevRecid, null), nodeSer );

        long headRecid = engine.put(prevRecid, Serializer.LONG_SERIALIZER);
        long headInsertRecid = engine.put(prevRecid, Serializer.LONG_SERIALIZER);



        Queues.CircularQueue<E> ret = new Queues.CircularQueue<E>(engine,
                catPut(name+".serializer",serializer),
                catPut(name+".headRecid",headRecid),
                catPut(name+".headInsertRecid",headInsertRecid),
                catPut(name+".size",size)
        );
        catalog.put(name + ".type", "CircularQueue");
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }

    synchronized public Atomic.Long createAtomicLong(String name, long initValue){
        checkNameNotExists(name);
        long recid = engine.put(initValue,Serializer.LONG_SERIALIZER);
        Atomic.Long ret = new Atomic.Long(engine,
                catPut(name+".recid",recid)
        );
        catalog.put(name + ".type", "AtomicLong");
        collections.put(name, new WeakReference<Object>(ret));
        return ret;

    }


    synchronized public Atomic.Long getAtomicLong(String name){
        checkNotClosed();
        Atomic.Long ret = (Atomic.Long) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            return createAtomicLong(name,0L);
        }
        checkType(type, "AtomicLong");

        ret = new Atomic.Long(engine, (Long) catGet(name+".recid"));
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }



    synchronized public Atomic.Integer createAtomicInteger(String name, int initValue){
        checkNameNotExists(name);
        long recid = engine.put(initValue,Serializer.INTEGER_SERIALIZER);
        Atomic.Integer ret = new Atomic.Integer(engine,
                catPut(name+".recid",recid)
        );
        catalog.put(name + ".type", "AtomicInteger");
        collections.put(name, new WeakReference<Object>(ret));
        return ret;

    }


    synchronized public Atomic.Integer getAtomicInteger(String name){
        checkNotClosed();
        Atomic.Integer ret = (Atomic.Integer) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            return createAtomicInteger(name, 0);
        }
        checkType(type, "AtomicInteger");

        ret = new Atomic.Integer(engine, (Long) catGet(name+".recid"));
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }



    synchronized public Atomic.Boolean createAtomicBoolean(String name, boolean initValue){
        checkNameNotExists(name);
        long recid = engine.put(initValue,Serializer.BOOLEAN_SERIALIZER);
        Atomic.Boolean ret = new Atomic.Boolean(engine,
                catPut(name+".recid",recid)
        );
        catalog.put(name + ".type", "AtomicBoolean");
        collections.put(name, new WeakReference<Object>(ret));
        return ret;

    }


    synchronized public Atomic.Boolean getAtomicBoolean(String name){
        checkNotClosed();
        Atomic.Boolean ret = (Atomic.Boolean) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            return createAtomicBoolean(name, false);
        }
        checkType(type, "AtomicBoolean");

        ret = new Atomic.Boolean(engine, (Long) catGet(name+".recid"));
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }

    protected void checkShouldCreate(String name) {
        if(strictDBGet) throw new NoSuchElementException("No record with this name was found: "+name);
    }


    synchronized public Atomic.String createAtomicString(String name, String initValue){
        checkNameNotExists(name);
        if(initValue==null) throw new IllegalArgumentException("initValue may not be null");
        long recid = engine.put(initValue,Serializer.STRING_SERIALIZER);
        Atomic.String ret = new Atomic.String(engine,
                catPut(name+".recid",recid)
        );
        catalog.put(name + ".type", "AtomicString");
        collections.put(name, new WeakReference<Object>(ret));
        return ret;

    }


    synchronized public Atomic.String getAtomicString(String name){
        checkNotClosed();
        Atomic.String ret = (Atomic.String) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            return createAtomicString(name, "");
        }
        checkType(type, "AtomicString");

        ret = new Atomic.String(engine, (Long) catGet(name+".recid"));
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }

    synchronized public <E> Atomic.Var<E> createAtomicVar(String name, E initValue, Serializer<E> serializer){
        checkNameNotExists(name);
        if(serializer==null) serializer=getDefaultSerializer();
        long recid = engine.put(initValue,serializer);
        Atomic.Var ret = new Atomic.Var(engine,
                catPut(name+".recid",recid),
                catPut(name+".serializer",serializer)
        );
        catalog.put(name + ".type", "AtomicVar");
        collections.put(name, new WeakReference<Object>(ret));
        return ret;

    }


    synchronized public <E> Atomic.Var<E> getAtomicVar(String name){
        checkNotClosed();
        Atomic.Var ret = (Atomic.Var) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + ".type", null);
        if(type==null){
            checkShouldCreate(name);
            return createAtomicVar(name, null, getDefaultSerializer());
        }
        checkType(type, "AtomicString");

        ret = new Atomic.Var(engine, (Long) catGet(name+".recid"), (Serializer) catGet(name+".serializer"));
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }

    /** return record with given name or null if name does not exist*/
    synchronized public <E> E get(String name){
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
        throw new InternalError("Unknown type: "+name);
    }

    /** delete record/collection with given name*/
    synchronized public void delete(String name){
        Object r = get(name);
        if(r instanceof Atomic.Boolean){
            engine.delete(((Atomic.Boolean)r).recid, Serializer.BOOLEAN_SERIALIZER);
        }else if(r instanceof Atomic.Integer){
            engine.delete(((Atomic.Integer)r).recid, Serializer.INTEGER_SERIALIZER);
        }else if(r instanceof Atomic.Long){
            engine.delete(((Atomic.Long)r).recid, Serializer.LONG_SERIALIZER);
        }else if(r instanceof Atomic.String){
            engine.delete(((Atomic.String)r).recid, Serializer.STRING_SERIALIZER);
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
            //delete segments
            for(long segmentRecid:m.segmentRecids){
                engine.delete(segmentRecid, HTreeMap.DIR_SERIALIZER);
            }
        }else if(r instanceof BTreeMap || r instanceof BTreeMap.KeySet){
            BTreeMap m = (r instanceof BTreeMap)? (BTreeMap) r : (BTreeMap) ((BTreeMap.KeySet) r).m;

            //TODO on BTreeMap recursively delete all nodes
            m.clear();

            if(m.counter!=null)
                engine.delete(m.counter.recid,Serializer.LONG_SERIALIZER);
        }

        for(String n:catalog.keySet()){
            if(!n.startsWith(name)) continue;
            catalog.remove(n);
        }
        collections.remove(name);

    }


    /**
     * return map of all named collections/records
     */
    synchronized public Map<String,Object> getAll(){
        TreeMap<String,Object> ret= new TreeMap<String, Object>();

        for(String name:catalog.keySet()){
            if(!name.endsWith(".type")) continue;
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

        Map<String, Object> sub = catalog.tailMap(oldName);
        List<String> toRemove = new ArrayList<String>();

        for(String param:sub.keySet()){
            if(!param.startsWith(oldName)) break;

            String suffix = param.substring(oldName.length());
            catalog.put(newName+suffix, catalog.get(param));
            toRemove.add(param);
        }
        if(toRemove.isEmpty()) throw new NoSuchElementException("Could not rename, name does not exist: "+oldName);

        WeakReference old = collections.remove(oldName);
        if(old!=null) collections.put(newName,old);
        for(String param:toRemove) catalog.remove(param);
    }


    /**
     * Checks that object with given name does not exist yet.
     * @param name to check
     * @throws IllegalArgumentException if name is already used
     */
    protected void checkNameNotExists(String name) {
        if(catalog.get(name)!=null)
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

    protected void checkType(String type, String expected) {
        if(!expected.equals(type)) throw new IllegalArgumentException("Wrong type: "+type);
    }


}
