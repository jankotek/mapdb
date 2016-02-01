package org.mapdb

import org.eclipse.collections.api.map.primitive.MutableLongLongMap
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import java.io.Closeable
import java.security.SecureRandom
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

/**
 * A database with easy access to named maps and other collections.
 */
open class DB(
        /** Stores all underlying data */
        val store:Store,
        /** True if store existed before and was opened, false if store was created and is completely empty */
        val storeOpened:Boolean
): Closeable {

    @Volatile private  var closed = false;

    companion object{
        internal val RECID_NAME_CATALOG:Long = 1L
        internal val RECID_MAX_RESERVED:Long = 1L

        internal val NAME_CATALOG_SERIALIZER:Serializer<SortedMap<String, String>> = object:Serializer<SortedMap<String, String>>(){

            override fun deserialize(input: DataInput2, available: Int): SortedMap<String, String>? {
                val size = input.unpackInt()
                val ret = TreeMap<String, String>()
                for(i in 0 until size){
                    ret.put(input.readUTF(), input.readUTF())
                }
                return ret
            }

            override fun serialize(out: DataOutput2, value: SortedMap<String, String>) {
                out.packInt(value.size)
                value.forEach { e ->
                    out.writeUTF(e.key)
                    out.writeUTF(e.value)
                }
            }
        }

        internal val NAME_PATTERN = "^[a-zA-Z0-9_]+$".toPattern()
    }


    object Keys {
        val type = ".type"

        val keySerializer = ".keySerializer"
        val valueSerializer = ".valueSerializer"
        val serializer = ".serializer"

        val valueInline = ".valueInline"

        val counterRecids = ".counterRecids"

        val hashSeed = ".hashSeed"
        val segmentRecids = ".segmentRecids"

        val expireCreateTTL = ".expireCreateTTL"
        val expireUpdateTTL = ".expireUpdateTTL"
        val expireGetTTL = ".expireGetTTL"

        val rootRecids = ".rootRecids"
        /** concurrency shift, 1<<it is number of concurrent segments in HashMap*/
        val concShift = ".concShift"
        val dirShift = ".dirShift"
        val levels = ".levels"
        val removeCollapsesIndexTree = ".removeCollapsesIndexTree"

        val rootRecidRecid = ".rootRecidRecid"
        val counterRecid = ".counterRecid"
        val maxNodeSize = ".maxNodeSize"

//        val valuesOutsideNodes = ".valuesOutsideNodes"
//        val numberOfNodeMetas = ".numberOfNodeMetas"
//
//        val headRecid = ".headRecid"
//        val tailRecid = ".tailRecid"
//        val useLocks = ".useLocks"
        val size = ".size"
        val recid = ".recid"
//        val headInsertRecid = ".headInsertRecid"
    }


    init{
        if(storeOpened.not()){
            //preallocate 16 recids
            if(RECID_NAME_CATALOG != store.put(TreeMap<String, String>(), NAME_CATALOG_SERIALIZER))
                throw DBException.WrongConfiguration("Store does not support Reserved Recids: "+store.javaClass)

            for(recid in 2L..RECID_MAX_RESERVED){
                val recid2 = store.put(0L, Serializer.LONG_PACKED)
                if(recid!==recid2){
                    throw DBException.WrongConfiguration("Store does not support Reserved Recids: "+store.javaClass)
                }
            }
        }
    }

    private val classSingletonCat = IdentityHashMap<Any,String>()
    private val classSingletonRev = HashMap<String, Any>()

    init{
        //read all singleton from Serializer fields
        Serializer::class.java.declaredFields.forEach { f ->
            val name = Serializer::class.java.canonicalName + "#"+f.name
            val obj = f.get(null)
            classSingletonCat.put(obj, name)
            classSingletonRev.put(name, obj)
        }
    }


    /** List of executors associated with this database. Those will be terminated on close() */
    internal val executors:MutableSet<ExecutorService> = Collections.synchronizedSet(LinkedHashSet());

    fun nameCatalogLoad():SortedMap<String, String> =
            store.get(RECID_NAME_CATALOG, NAME_CATALOG_SERIALIZER)
                    ?: throw DBException.WrongConfiguration("Could not open store, it has no Named Catalog");

    fun nameCatalogSave(nameCatalog: SortedMap<String, String>) {
        store.update(RECID_NAME_CATALOG, nameCatalog, NAME_CATALOG_SERIALIZER)
    }


    internal fun checkName(name: String) {
        if (NAME_PATTERN.matcher(name).matches().not())
            throw DBException.WrongConfiguration("Name contains illegal character. Only letters, numbers and `_` is allowed")
    }

    internal fun  nameCatalogPutClass(
            nameCatalog: SortedMap<String, String>,
            key: String,
            obj: Any
    ) {
        val value:String? = classSingletonCat.get(obj)

        if(value== null){
            //not in singletons, try to resolve public no ARG constructor of given class
            //TODO get public no arg constructor if exist
        }

        if(value!=null)
            nameCatalog.put(key, value)
    }

    internal fun <E> nameCatalogGetClass(
            nameCatalog: SortedMap<String, String>,
            key: String
    ):E?{
        val clazz = nameCatalog.get(key)
            ?: return null

        val singleton = classSingletonRev.get(clazz)
        if(singleton!=null)
            return singleton as E

        throw DBException.WrongConfiguration("Could not load object: "+clazz)
    }

    fun nameCatalogParamsFor(name: String): Map<String,String> {
        val ret = TreeMap<String,String>()
        ret.putAll(nameCatalogLoad().filter {
            it.key.startsWith(name+".")
        })
        return Collections.unmodifiableMap(ret)
    }

    fun commit(){
        store.commit()
    }

    fun rollback(){
        if(store !is StoreTx)
            throw UnsupportedOperationException("Store does not support rollback")

        store.rollback()
    }

    fun isClosed() = closed;

    override fun close(){
        //shutdown running executors if any
        executors.forEach { it.shutdown() }
        //await termination on all
        executors.forEach {
            // TODO LOG this could use some warnings, if background tasks fails to shutdown
            while(!it.awaitTermination(1, TimeUnit.DAYS)){
            }
        }
        executors.clear()
        closed = true;
        store.close()
    }


    class HashMapMaker<K,V>(
            private val db:DB,
            private val name:String
    ){
        private var _keySerializer:Serializer<K> = Serializer.JAVA as Serializer<K>
        private var _valueSerializer:Serializer<V> = Serializer.JAVA as Serializer<V>
        private var _valueInline = false

        private var _concShift = CC.HTREEMAP_CONC_SHIFT
        private var _dirShift = CC.HTREEMAP_DIR_SHIFT
        private var _levels = CC.HTREEMAP_LEVELS

        private var _hashSeed:Int? = null
        private var _expireCreateTTL:Long = 0L
        private var _expireUpdateTTL:Long = 0L
        private var _expireGetTTL:Long = 0L
        private var _expireExecutor:ScheduledExecutorService? = null
        private var _expireExecutorPeriod:Long = 10000
        private var _expireMaxSize:Long = 0
        private var _expireStoreSize:Long = 0
        private var _expireCompactThreshold:Double? = null

        private var _counterEnable: Boolean = false

        private var _storeFactory:(segment:Int)->Store = {i-> db.store}

        private var _valueLoader:((key:K)->V?)? = null
        private var _modListeners:MutableList<MapModificationListener<K,V>> = ArrayList()
        private var _expireOverflow:MutableMap<K,V>? = null;
        private var _removeCollapsesIndexTree:Boolean = true


        fun <A> keySerializer(keySerializer:Serializer<A>):HashMapMaker<A,V>{
            _keySerializer = keySerializer as Serializer<K>
            return this as HashMapMaker<A, V>
        }

        fun <A> valueSerializer(valueSerializer:Serializer<A>):HashMapMaker<K,A>{
            _valueSerializer = valueSerializer as Serializer<V>
            return this as HashMapMaker<K, A>
        }


        fun valueInline():HashMapMaker<K,V>{
            _valueInline = true
            return this
        }


        fun removeCollapsesIndexTreeDisable():HashMapMaker<K,V>{
            _removeCollapsesIndexTree = false
            return this
        }

        fun hashSeed(hashSeed:Int):HashMapMaker<K,V>{
            _hashSeed = hashSeed
            return this
        }

        fun layout(concurrency:Int, dirSize:Int, levels:Int):HashMapMaker<K,V>{
            fun toShift(value:Int):Int{
                return 31 - Integer.numberOfLeadingZeros(DataIO.nextPowTwo(Math.max(1,value)))
            }
            _concShift = toShift(concurrency)
            _dirShift = toShift(dirSize)
            _levels = levels
            return this
        }

        fun expireAfterCreate():HashMapMaker<K,V>{
            return expireAfterCreate(-1)
        }

        fun expireAfterCreate(ttl:Long):HashMapMaker<K,V>{
            _expireCreateTTL = ttl
            return this
        }


        fun expireAfterCreate(ttl:Long, unit:TimeUnit):HashMapMaker<K,V> {
            return expireAfterCreate(unit.toMillis(ttl))
        }

        fun expireAfterUpdate():HashMapMaker<K,V>{
            return expireAfterUpdate(-1)
        }


        fun expireAfterUpdate(ttl:Long):HashMapMaker<K,V>{
            _expireUpdateTTL = ttl
            return this
        }

        fun expireAfterUpdate(ttl:Long, unit:TimeUnit):HashMapMaker<K,V> {
            return expireAfterUpdate(unit.toMillis(ttl))
        }

        fun expireAfterGet():HashMapMaker<K,V>{
            return expireAfterGet(-1)
        }

        fun expireAfterGet(ttl:Long):HashMapMaker<K,V>{
            _expireGetTTL = ttl
            return this
        }


        fun expireAfterGet(ttl:Long, unit:TimeUnit):HashMapMaker<K,V> {
            return expireAfterGet(unit.toMillis(ttl))
        }


        fun expireExecutor(executor: ScheduledExecutorService?):HashMapMaker<K,V>{
            _expireExecutor = executor;
            return this
        }

        fun expireExecutorPeriod(period:Long):HashMapMaker<K,V>{
            _expireExecutorPeriod = period
            return this
        }

        fun expireCompactThreshold(freeFraction: Double):HashMapMaker<K,V>{
            _expireCompactThreshold = freeFraction
            return this
        }


        fun expireMaxSize(maxSize:Long):HashMapMaker<K,V>{
            _expireMaxSize = maxSize;
            return counterEnable()
        }

        fun expireStoreSize(storeSize:Long):HashMapMaker<K,V>{
            _expireStoreSize = storeSize;
            return this
        }

        fun expireOverflow(overflowMap:MutableMap<K,V>):HashMapMaker<K,V>{
            _expireOverflow = overflowMap
            return this
        }

        internal fun storeFactory(storeFactory:(segment:Int)->Store):HashMapMaker<K,V>{
            _storeFactory = storeFactory
            return this
        }

        fun valueLoader(valueLoader:(key:K)->V):HashMapMaker<K,V>{
            _valueLoader = valueLoader
            return this
        }

        fun counterEnable():HashMapMaker<K,V>{
            _counterEnable = true
            return this;
        }

        fun modificationListener(listener:MapModificationListener<K,V>):HashMapMaker<K,V>{
            if(_modListeners==null)
                _modListeners = ArrayList()
            _modListeners?.add(listener)
            return this;
        }


        fun create() = make(true)
        fun createOrOpen() = make(null)
        fun open() = make(false)



        internal fun make(
                create:Boolean?
        ):HTreeMap<K,V>{
            db.checkName(name)
            if(_expireOverflow!=null && _valueLoader !=null)
                throw DBException.WrongConfiguration("ExpireOverflow and ValueLoader can not be used at the same time")


            val hashSeed = _hashSeed ?: SecureRandom().nextInt()
            val nameCatalog = db.nameCatalogLoad()

            val contains = nameCatalog.containsKey(name + Keys.type);
            if (create != null) {
                if (contains && create)
                    throw DBException.WrongConfiguration("Named record already exists: $name")
                if (!create && !contains)
                    throw DBException.WrongConfiguration("Named record does not exist: $name")
            }

            val segmentCount = 1.shl(_concShift)
            val stores = Array(segmentCount, _storeFactory)

            val rootRecids = LongArray(segmentCount)
            var rootRecidsStr = "";
            for(i in 0 until segmentCount) {
                val rootRecid = stores[i].put(IndexTreeListJava.dirEmpty(), IndexTreeListJava.dirSer)
                rootRecids[i] = rootRecid
                rootRecidsStr+= (if(i==0)"" else ",") + rootRecid
            }

            nameCatalog.put(name + Keys.type, "HashMap")
            db.nameCatalogPutClass(nameCatalog, name + Keys.keySerializer, _keySerializer)
            db.nameCatalogPutClass(nameCatalog, name + Keys.valueSerializer, _valueSerializer)

            nameCatalog.put(name + Keys.valueInline, _valueInline.toString())

            nameCatalog.put(name + Keys.rootRecids, rootRecidsStr)
            nameCatalog.put(name + Keys.hashSeed, hashSeed.toString())
            nameCatalog.put(name + Keys.concShift, _concShift.toString())
            nameCatalog.put(name + Keys.dirShift, _dirShift.toString())
            nameCatalog.put(name + Keys.levels, _levels.toString())
            nameCatalog.put(name + Keys.removeCollapsesIndexTree, _removeCollapsesIndexTree.toString())

            val counterRecids:LongArray? = if(_counterEnable){
                val cr = LongArray(segmentCount, {segment->
                    stores[segment].put(0L, Serializer.LONG_PACKED)
                })
                nameCatalog.put(name + Keys.counterRecids, LongArrayList.newListWith(*cr).makeString("",",",""))
                cr
            }else{
                nameCatalog.put(name + Keys.counterRecids, "")
                null
            }

            nameCatalog.put(name + Keys.expireCreateTTL, _expireCreateTTL.toString())
            nameCatalog.put(name + Keys.expireUpdateTTL, _expireUpdateTTL.toString())
            nameCatalog.put(name + Keys.expireGetTTL, _expireGetTTL.toString())

            fun emptyLongQueue(segment:Int):QueueLong{
                val store = stores[segment]
                val q = store.put(null, QueueLong.Node.SERIALIZER);
                val tailRecid = store.put(q, Serializer.RECID)
                val headRecid = store.put(q, Serializer.RECID)
                val headPrevRecid = store.put(0L, Serializer.RECID)
                return QueueLong(store=store, tailRecid = tailRecid, headRecid = headRecid, headPrevRecid = headPrevRecid)
            }

            val expireCreateQueues:Array<QueueLong>? =
                    if(_expireCreateTTL==0L) null
                    else Array(segmentCount,{emptyLongQueue(it)})

            val expireUpdateQueues:Array<QueueLong>? =
                    if(_expireUpdateTTL==0L) null
                    else Array(segmentCount,{emptyLongQueue(it)})

            val expireGetQueues:Array<QueueLong>? =
                    if(_expireGetTTL==0L) null
                    else Array(segmentCount,{emptyLongQueue(it)})

            if(_expireExecutor!=null)
                db.executors.add(_expireExecutor!!)

            db.nameCatalogSave(nameCatalog)

            val indexTrees = Array<MutableLongLongMap>(1.shl(_concShift),{segment->
                IndexTreeLongLongMap(
                        store=stores[segment],
                        rootRecid=rootRecids[segment],
                        dirShift = _dirShift,
                        levels=_levels,
                        collapseOnRemove = _removeCollapsesIndexTree
                )
            })

            var valueLoader2 = _valueLoader
            val expireOverflow = _expireOverflow
            if(expireOverflow!=null) {
                //load non existing values from overflow
                valueLoader2 = { key-> expireOverflow[key]}

                //forward modifications to overflow
                val listener = MapModificationListener<K,V>{ key, oldVal, newVal, triggered ->
                    if(!triggered && newVal==null && oldVal!=null){
                        //removal, also remove from overflow map
                        val oldVal2 = expireOverflow.remove(key)
                        if(oldVal2!=null && _valueSerializer.equals(oldVal as V, oldVal2 as V)){
                            Utils.LOG.warning { "Key also removed from overflow Map, but value in overflow Map differs" }
                        }
                    }else if(triggered && newVal==null){
                        // triggered by eviction, put evicted entry into overflow map
                        expireOverflow.put(key, oldVal)
                    }
                }
                _modListeners.add(listener)
            }

            val htreemap = HTreeMap(
                    keySerializer = _keySerializer,
                    valueSerializer = _valueSerializer,
                    valueInline = _valueInline,
                    concShift = _concShift,
                    dirShift = _dirShift,
                    levels = _levels,
                    stores = stores,
                    indexTrees = indexTrees,
                    hashSeed = hashSeed,
                    counterRecids = counterRecids,
                    expireCreateTTL = _expireCreateTTL,
                    expireUpdateTTL = _expireUpdateTTL,
                    expireGetTTL = _expireGetTTL,
                    expireMaxSize = _expireMaxSize,
                    expireStoreSize = _expireStoreSize,
                    expireCreateQueues = expireCreateQueues,
                    expireUpdateQueues = expireUpdateQueues,
                    expireGetQueues = expireGetQueues,
                    expireExecutor = _expireExecutor,
                    expireExecutorPeriod = _expireExecutorPeriod,
                    expireCompactThreshold = _expireCompactThreshold,
                    threadSafe = true,
                    valueLoader = valueLoader2,
                    modificationListeners = if(_modListeners.isEmpty()) null else _modListeners.toTypedArray(),
                    closeable = db
            )
            return htreemap
        }
    }

    fun hashMap(name:String):HashMapMaker<*,*> = HashMapMaker<Any?,Any?>(this, name)
    fun <K,V> hashMap(name:String, keySerializer: Serializer<K>, valueSerializer: Serializer<V>) =
            HashMapMaker<K,V>(this, name)
            .keySerializer(keySerializer)
            .valueSerializer(valueSerializer)


    class TreeMapMaker<K,V>(
            private val db:DB,
            private val name:String
    ){

        private var _keySerializer:Serializer<K> = Serializer.JAVA as Serializer<K>
        private var _valueSerializer:Serializer<V> = Serializer.JAVA as Serializer<V>
        private var _maxNodeSize = CC.BTREEMAP_MAX_NODE_SIZE
        private var _counterEnable: Boolean = false
        private var _valueLoader:((key:K)->V)? = null
        private var _modListeners:MutableList<MapModificationListener<K,V>>? = null


        fun <A> keySerializer(keySerializer:Serializer<A>):TreeMapMaker<A,V>{
            _keySerializer = keySerializer as Serializer<K>
            return this as TreeMapMaker<A, V>
        }

        fun <A> valueSerializer(valueSerializer:Serializer<A>):TreeMapMaker<K,A>{
            _valueSerializer = valueSerializer as Serializer<V>
            return this as TreeMapMaker<K, A>
        }

        fun valueLoader(valueLoader:(key:K)->V):TreeMapMaker<K,V>{
            //TODO BTree value loader
            _valueLoader = valueLoader
            return this
        }


        fun maxNodeSize(size:Int):TreeMapMaker<K,V>{
            _maxNodeSize = size
            return this;
        }

        fun counterEnable():TreeMapMaker<K,V>{
            //TODO BTree counter
            _counterEnable = true
            return this;
        }

        fun modificationListener(listener:MapModificationListener<K,V>):TreeMapMaker<K,V>{
            //TODO BTree modification listener
            if(_modListeners==null)
                _modListeners = ArrayList()
            _modListeners?.add(listener)
            return this;
        }


        fun create() = make( true)
        fun createOrOpen() = make(null)
        fun open() = make( false)



        internal fun make(
                create:Boolean?
        ):BTreeMap<K,V>{
            db.checkName(name)
            val nameCatalog = db.nameCatalogLoad()

            val contains = nameCatalog.containsKey(name + Keys.type);
            if (create != null) {
                if (contains && create)
                    throw DBException.WrongConfiguration("Named record already exists: $name")
                if (!create && !contains)
                    throw DBException.WrongConfiguration("Named record does not exist: $name")
            }


            nameCatalog.put(name + Keys.type, "TreeMap")
            db.nameCatalogPutClass(nameCatalog, name + Keys.keySerializer, _keySerializer)
            db.nameCatalogPutClass(nameCatalog, name + Keys.valueSerializer, _valueSerializer)

            val rootRecidRecid =  BTreeMap.putEmptyRoot(db.store, _keySerializer, _valueSerializer)
            nameCatalog.put(name + Keys.rootRecidRecid, rootRecidRecid.toString())
            val counterRecid =
                    if(_counterEnable) db.store.put(0L, Serializer.LONG)
                    else 0L
            nameCatalog.put(name + Keys.counterRecid, counterRecid.toString())

            nameCatalog.put(name + Keys.maxNodeSize, _maxNodeSize.toString())

            db.nameCatalogSave(nameCatalog)

            val btreemap = BTreeMap(
                    keySerializer = _keySerializer,
                    valueSerializer = _valueSerializer,
                    rootRecidRecid = rootRecidRecid,
                    store = db.store,
                    maxNodeSize = _maxNodeSize,
                    comparator = _keySerializer //TODO custom comparator
            )
            return btreemap
        }
    }


    fun treeMap(name:String):TreeMapMaker<*,*> = TreeMapMaker<Any?,Any?>(this, name)
    fun <K,V> treeMap(name:String, keySerializer: Serializer<K>, valueSerializer: Serializer<V>) =
            TreeMapMaker<K,V>(this, name)
                    .keySerializer(keySerializer)
                    .valueSerializer(valueSerializer)

}