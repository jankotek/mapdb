package org.mapdb

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import org.eclipse.collections.api.map.primitive.MutableLongLongMap
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.mapdb.serializer.GroupSerializer
import java.io.Closeable
import java.security.SecureRandom
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * A database with easy access to named maps and other collections.
 */
open class DB(
        /** Stores all underlying data */
        val store:Store,
        /** True if store existed before and was opened, false if store was created and is completely empty */
        val storeOpened:Boolean
): Closeable {

    companion object{
        internal val RECID_NAME_CATALOG:Long = 1L
        internal val RECID_MAX_RESERVED:Long = 1L

        internal val NAME_CATALOG_SERIALIZER:Serializer<SortedMap<String, String>> = object:Serializer<SortedMap<String, String>>{

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
        
    }


    object Keys {
        val type = "#type"

        val keySerializer = "#keySerializer"
        val valueSerializer = "#valueSerializer"
        val serializer = "#serializer"

        val valueInline = "#valueInline"

        val counterRecids = "#counterRecids"

        val hashSeed = "#hashSeed"
        val segmentRecids = "#segmentRecids"

        val expireCreateTTL = "#expireCreateTTL"
        val expireUpdateTTL = "#expireUpdateTTL"
        val expireGetTTL = "#expireGetTTL"

        val expireCreateQueues = "#expireCreateQueue"
        val expireUpdateQueues = "#expireUpdateQueue"
        val expireGetQueues = "#expireGetQueue"


        val rootRecids = "#rootRecids"
        val rootRecid = "#rootRecid"
        /** concurrency shift, 1<<it is number of concurrent segments in HashMap*/
        val concShift = "#concShift"
        val dirShift = "#dirShift"
        val levels = "#levels"
        val removeCollapsesIndexTree = "#removeCollapsesIndexTree"

        val rootRecidRecid = "#rootRecidRecid"
        val counterRecid = "#counterRecid"
        val maxNodeSize = "#maxNodeSize"

//        val valuesOutsideNodes = "#valuesOutsideNodes"
//        val numberOfNodeMetas = "#numberOfNodeMetas"
//
//        val headRecid = "#headRecid"
//        val tailRecid = "#tailRecid"
//        val useLocks = "#useLocks"
        val size = "#size"
        val recid = "#recid"
//        val headInsertRecid = "#headInsertRecid"
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

    internal val lock = ReentrantReadWriteLock();

    @Volatile private  var closed = false;

    /** Already loaded named collections. Values are weakly referenced. We need singletons for locking */
    protected var namesInstanciated: Cache<String, Any?> = CacheBuilder.newBuilder().concurrencyLevel(1).weakValues().build()


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

    fun nameCatalogLoad():SortedMap<String, String> {
        if(CC.ASSERT)
            Utils.assertReadLock(lock)
        return store.get(RECID_NAME_CATALOG, NAME_CATALOG_SERIALIZER)
                ?: throw DBException.WrongConfiguration("Could not open store, it has no Named Catalog");
    }

    fun nameCatalogSave(nameCatalog: SortedMap<String, String>) {
        if(CC.ASSERT)
            Utils.assertWriteLock(lock)
        store.update(RECID_NAME_CATALOG, nameCatalog, NAME_CATALOG_SERIALIZER)
    }


    internal fun checkName(name: String) {
        //TODO limit characters in name?
        if(name.contains('#'))
            throw DBException.WrongConfiguration("Name contains illegal character, '#' is not allowed.")
    }

    internal fun nameCatalogGet(name: String): String? {
        return nameCatalogLoad()[name]
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
            it.key.startsWith(name+"#")
        })
        return Collections.unmodifiableMap(ret)
    }

    fun commit(){
        Utils.lockWrite(lock) {
            store.commit()
        }
    }

    fun rollback(){
        if(store !is StoreTx)
            throw UnsupportedOperationException("Store does not support rollback")

        Utils.lockWrite(lock) {
            store.rollback()
        }
    }

    fun isClosed() = closed;

    override fun close(){
        Utils.lockWrite(lock) {
            //shutdown running executors if any
            executors.forEach { it.shutdown() }
            //await termination on all
            executors.forEach {
                // TODO LOG this could use some warnings, if background tasks fails to shutdown
                while (!it.awaitTermination(1, TimeUnit.DAYS)) {
                }
            }
            executors.clear()
            closed = true;
            store.close()
        }
    }

    fun <E> get(name:String):E{
        Utils.lockWrite(lock) {
            val type = nameCatalogGet(name + Keys.type)
            return when (type) {
                "HashMap" -> hashMap(name).make()
                "HashSet" -> hashSet(name).make()
                "TreeMap" -> treeMap(name).make()
                "TreeSet" -> treeSet(name).make()

                "AtomicBoolean" -> atomicBoolean(name).make()
                "AtomicInteger" -> atomicInteger(name).make()
                "AtomicVar" -> atomicVar(name).make()
                "AtomicString" -> atomicString(name).make()
                "AtomicLong" -> atomicLong(name).make()

                "IndexTreeList" -> indexTreeList(name).make()
                "IndexTreeLongLongMap" -> indexTreeLongLongMap(name).make()

                null -> null
                else -> DBException.WrongConfiguration("Collection has unknown type: "+type)
            } as E
        }
    }

    fun exists(name: String): Boolean {
        Utils.lockRead(lock) {
            return nameCatalogGet(name + Keys.type) != null
        }
    }

    fun getAllNames():Iterable<String>{
        return nameCatalogLoad().keys
                .filter { it.endsWith(Keys.type) }
                .map {it.split("#")[0]}
    }

    fun getAll():Map<String, Any?>{
        val ret = TreeMap<String,Any?>();
        getAllNames().forEach { ret.put(it, get(it)) }
        return ret
    }
//
//
//    /** rename named record into newName
//
//     * @param oldName current name of record/collection
//     * *
//     * @param newName new name of record/collection
//     * *
//     * @throws NoSuchElementException if oldName does not exist
//     */
//    @Synchronized fun rename(oldName: String, newName: String) {
//        if (oldName == newName) return
//        //$DELAY$
//        val sub = catalog.tailMap(oldName)
//        val toRemove = ArrayList<String>()
//        //$DELAY$
//        for (param in sub.keys) {
//            if (!param.startsWith(oldName)) break
//
//            val suffix = param.substring(oldName.length)
//            catalog.put(newName + suffix, catalog.get(param))
//            toRemove.add(param)
//        }
//        if (toRemove.isEmpty()) throw NoSuchElementException("Could not rename, name does not exist: " + oldName)
//        //$DELAY$
//        val old = namesInstanciated.remove(oldName)
//        if (old != null) {
//            val old2 = old!!.get()
//            if (old2 != null) {
//                namesLookup.remove(IdentityWrapper(old2))
//                namedPut(newName, old2)
//            }
//        }
//        for (param in toRemove) catalog.remove(param)
//    }


    class HashMapMaker<K,V>(
            override val db:DB,
            override val name:String,
            val hasValues:Boolean=true
    ):Maker<HTreeMap<K,V>>(){

        override val type = "HashMap"
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
                return 31 - Integer.numberOfLeadingZeros(DBUtil.nextPowTwo(Math.max(1,value)))
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

        override fun verify(){
            if (_expireOverflow != null && _valueLoader != null)
                throw DBException.WrongConfiguration("ExpireOverflow and ValueLoader can not be used at the same time")

            val expireOverflow = _expireOverflow
            if (expireOverflow != null) {
                //load non existing values from overflow
                _valueLoader = { key -> expireOverflow[key] }

                //forward modifications to overflow
                val listener = MapModificationListener<K, V> { key, oldVal, newVal, triggered ->
                    if (!triggered && newVal == null && oldVal != null) {
                        //removal, also remove from overflow map
                        val oldVal2 = expireOverflow.remove(key)
                        if (oldVal2 != null && _valueSerializer.equals(oldVal as V, oldVal2 as V)) {
                            Utils.LOG.warning { "Key also removed from overflow Map, but value in overflow Map differs" }
                        }
                    } else if (triggered && newVal == null) {
                        // triggered by eviction, put evicted entry into overflow map
                        expireOverflow.put(key, oldVal)
                    }
                }
                _modListeners.add(listener)
            }

            if (_expireExecutor != null)
                db.executors.add(_expireExecutor!!)
        }

        override fun create2(catalog: SortedMap<String, String>): HTreeMap<K, V> {
            val segmentCount = 1.shl(_concShift)
            val hashSeed = _hashSeed ?: SecureRandom().nextInt()
            val stores = Array(segmentCount, _storeFactory)

            val rootRecids = LongArray(segmentCount)
            var rootRecidsStr = "";
            for (i in 0 until segmentCount) {
                val rootRecid = stores[i].put(IndexTreeListJava.dirEmpty(), IndexTreeListJava.dirSer)
                rootRecids[i] = rootRecid
                rootRecidsStr += (if (i == 0) "" else ",") + rootRecid
            }

            db.nameCatalogPutClass(catalog, name + if(hasValues) Keys.keySerializer else Keys.serializer, _keySerializer)
            if(hasValues) {
                db.nameCatalogPutClass(catalog, name + Keys.valueSerializer, _valueSerializer)
            }
            if(hasValues)
                catalog[name + Keys.valueInline] = _valueInline.toString()

            catalog[name + Keys.rootRecids] = rootRecidsStr
            catalog[name + Keys.hashSeed] = hashSeed.toString()
            catalog[name + Keys.concShift] = _concShift.toString()
            catalog[name + Keys.dirShift] = _dirShift.toString()
            catalog[name + Keys.levels] = _levels.toString()
            catalog[name + Keys.removeCollapsesIndexTree] = _removeCollapsesIndexTree.toString()

            val counterRecids = if (_counterEnable) {
                val cr = LongArray(segmentCount, { segment ->
                    stores[segment].put(0L, Serializer.LONG_PACKED)
                })
                catalog[name + Keys.counterRecids] = LongArrayList.newListWith(*cr).makeString("", ",", "")
                cr
            } else {
                catalog[name + Keys.counterRecids] = ""
                null
            }

            catalog[name + Keys.expireCreateTTL] = _expireCreateTTL.toString()
            if(hasValues)
                catalog[name + Keys.expireUpdateTTL] = _expireUpdateTTL.toString()
            catalog[name + Keys.expireGetTTL] = _expireGetTTL.toString()

            var createQ = LongArrayList()
            var updateQ = LongArrayList()
            var getQ = LongArrayList()


            fun emptyLongQueue(segment: Int, qq: LongArrayList): QueueLong {
                val store = stores[segment]
                val q = store.put(null, QueueLong.Node.SERIALIZER);
                val tailRecid = store.put(q, Serializer.RECID)
                val headRecid = store.put(q, Serializer.RECID)
                val headPrevRecid = store.put(0L, Serializer.RECID)
                qq.add(tailRecid)
                qq.add(headRecid)
                qq.add(headPrevRecid)
                return QueueLong(store = store, tailRecid = tailRecid, headRecid = headRecid, headPrevRecid = headPrevRecid)
            }

            val expireCreateQueues =
                    if (_expireCreateTTL == 0L) null
                    else Array(segmentCount, { emptyLongQueue(it, createQ) })

            val expireUpdateQueues =
                    if (_expireUpdateTTL == 0L) null
                    else Array(segmentCount, { emptyLongQueue(it, updateQ) })
            val expireGetQueues =
                    if (_expireGetTTL == 0L) null
                    else Array(segmentCount, { emptyLongQueue(it, getQ) })

            catalog[name + Keys.expireCreateQueues] = createQ.makeString("", ",", "")
            if(hasValues)
                catalog[name + Keys.expireUpdateQueues] = updateQ.makeString("", ",", "")
            catalog[name + Keys.expireGetQueues] = getQ.makeString("", ",", "")

            val indexTrees = Array<MutableLongLongMap>(1.shl(_concShift), { segment ->
                IndexTreeLongLongMap(
                        store = stores[segment],
                        rootRecid = rootRecids[segment],
                        dirShift = _dirShift,
                        levels = _levels,
                        collapseOnRemove = _removeCollapsesIndexTree
                )
            })

            return HTreeMap(
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
                    valueLoader = _valueLoader,
                    modificationListeners = if (_modListeners.isEmpty()) null else _modListeners.toTypedArray(),
                    closeable = db,
                    hasValues = hasValues
            )
        }

        override fun open2(catalog: SortedMap<String, String>): HTreeMap<K, V> {
            val segmentCount = 1.shl(_concShift)
            val stores = Array(segmentCount, _storeFactory)

            _keySerializer =
                    db.nameCatalogGetClass(catalog, name + if(hasValues)Keys.keySerializer else Keys.serializer)
                            ?: _keySerializer
            _valueSerializer = if(!hasValues) BTreeMap.NO_VAL_SERIALIZER as Serializer<V>
                    else {
                       db.nameCatalogGetClass(catalog, name + Keys.valueSerializer)?: _valueSerializer
                    }
            _valueInline = if(hasValues) catalog[name + Keys.valueInline]!!.toBoolean() else false

            val hashSeed = catalog[name + Keys.hashSeed]!!.toInt()
            val rootRecids = catalog[name + Keys.rootRecids]!!.split(",").map { it.toLong() }.toLongArray()
            val counterRecidsStr = catalog[name + Keys.counterRecids]!!
            val counterRecids =
                    if ("" == counterRecidsStr) null
                    else counterRecidsStr.split(",").map { it.toLong() }.toLongArray()

            _concShift = catalog[name + Keys.concShift]!!.toInt()
            _dirShift = catalog[name + Keys.dirShift]!!.toInt()
            _levels = catalog[name + Keys.levels]!!.toInt()
            _removeCollapsesIndexTree = catalog[name + Keys.removeCollapsesIndexTree]!!.toBoolean()


            _expireCreateTTL = catalog[name + Keys.expireCreateTTL]!!.toLong()
            _expireUpdateTTL = if(hasValues)catalog[name + Keys.expireUpdateTTL]!!.toLong() else 0L
            _expireGetTTL = catalog[name + Keys.expireGetTTL]!!.toLong()


            fun queues(ttl: Long, queuesName: String): Array<QueueLong>? {
                if (ttl == 0L)
                    return null
                val rr = catalog[queuesName]!!.split(",").map { it.toLong() }.toLongArray()
                if (rr.size != segmentCount * 3)
                    throw DBException.WrongConfiguration("wrong segment count");
                return Array(segmentCount, { segment ->
                    QueueLong(store = stores[segment],
                            tailRecid = rr[segment * 3 + 0], headRecid = rr[segment * 3 + 1], headPrevRecid = rr[segment * 3 + 2]
                    )
                })
            }

            val expireCreateQueues = queues(_expireCreateTTL, name + Keys.expireCreateQueues)
            val expireUpdateQueues = queues(_expireUpdateTTL, name + Keys.expireUpdateQueues)
            val expireGetQueues = queues(_expireGetTTL, name + Keys.expireGetQueues)

            val indexTrees = Array<MutableLongLongMap>(1.shl(_concShift), { segment ->
                IndexTreeLongLongMap(
                        store = stores[segment],
                        rootRecid = rootRecids[segment],
                        dirShift = _dirShift,
                        levels = _levels,
                        collapseOnRemove = _removeCollapsesIndexTree
                )
            })
            return HTreeMap(
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
                    valueLoader = _valueLoader,
                    modificationListeners = if (_modListeners.isEmpty()) null else _modListeners.toTypedArray(),
                    closeable = db,
                    hasValues = hasValues
            )
        }

        override fun create(): HTreeMap<K, V> {
            return super.create()
        }

        override fun createOrOpen(): HTreeMap<K, V> {
            return super.createOrOpen()
        }

        override fun open(): HTreeMap<K, V> {
            return super.open()
        }
    }

    fun hashMap(name:String):HashMapMaker<*,*> = HashMapMaker<Any?, Any?>(this, name)
    fun <K,V> hashMap(name:String, keySerializer: Serializer<K>, valueSerializer: Serializer<V>) =
            HashMapMaker<K,V>(this, name)
            .keySerializer(keySerializer)
            .valueSerializer(valueSerializer)

    abstract class TreeMapPump<K,V>:Pump.Consumer<Pair<K,V>, BTreeMap<K,V>>(){
        fun take(key:K, value:V) {
            take(Pair(key, value))
        }

        fun takeAll(map:SortedMap<K,V>){
            map.forEach { e ->
                take(e.key, e.value)
            }
        }
    }

    class TreeMapMaker<K,V>(
            override val db:DB,
            override val name:String,
            val hasValues:Boolean=true
    ):Maker<BTreeMap<K,V>>(){

        override val type = "TreeMap"

        private var _keySerializer:GroupSerializer<K> = Serializer.JAVA as GroupSerializer<K>
        private var _valueSerializer:GroupSerializer<V> =
                (if(hasValues) Serializer.JAVA else BTreeMap.NO_VAL_SERIALIZER) as GroupSerializer<V>
        private var _maxNodeSize = CC.BTREEMAP_MAX_NODE_SIZE
        private var _counterEnable: Boolean = false
        private var _valueLoader:((key:K)->V)? = null
        private var _modListeners:MutableList<MapModificationListener<K,V>>? = null
        private var _threadSafe = true;

        private var _rootRecidRecid:Long? = null
        private var _counterRecid:Long? = null


        fun <A> keySerializer(keySerializer:GroupSerializer<A>):TreeMapMaker<A,V>{
            _keySerializer = keySerializer as GroupSerializer<K>
            return this as TreeMapMaker<A, V>
        }

        fun <A> valueSerializer(valueSerializer:GroupSerializer<A>):TreeMapMaker<K,A>{
            if(!hasValues)
                throw DBException.WrongConfiguration("Set, no vals")
            _valueSerializer = valueSerializer as GroupSerializer<V>
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
            _counterEnable = true
            return this;
        }


        fun threadSafeDisable():TreeMapMaker<K,V>{
            _threadSafe = false
            return this;
        }

        fun modificationListener(listener:MapModificationListener<K,V>):TreeMapMaker<K,V>{
            //TODO BTree modification listener
            if(_modListeners==null)
                _modListeners = ArrayList()
            _modListeners?.add(listener)
            return this;
        }


        fun import(iterator:Iterator<Pair<K,V>>):BTreeMap<K,V>{
            val consumer = import()
            while(iterator.hasNext()){
                consumer.take(iterator.next())
            }
            return consumer.finish()
        }

        fun import():TreeMapPump<K,V>{

            val consumer = Pump.treeMap(
                store = db.store,
                keySerializer = _keySerializer,
                valueSerializer = _valueSerializer,
                //TODO add custom comparator, once its enabled
                dirNodeSize = _maxNodeSize *3/4,
                leafNodeSize = _maxNodeSize *3/4
            )

            return object:TreeMapPump<K,V>(){

                override fun take(e: Pair<K, V>) {
                    consumer.take(e)
                }

                override fun finish(): BTreeMap<K, V> {
                    consumer.finish()
                    this@TreeMapMaker._rootRecidRecid = consumer.rootRecidRecid
                        ?: throw AssertionError()
                    this@TreeMapMaker._counterRecid =
                            if(_counterEnable) db.store.put(consumer.counter, Serializer.LONG)
                            else 0L
                    return this@TreeMapMaker.make2(create=true)
                }

            }
        }

        override fun create2(catalog: SortedMap<String, String>): BTreeMap<K, V> {
            db.nameCatalogPutClass(catalog, name +
                    (if(hasValues)Keys.keySerializer else Keys.serializer), _keySerializer)
            if(hasValues) {
                db.nameCatalogPutClass(catalog, name + Keys.valueSerializer, _valueSerializer)
            }

            val rootRecidRecid2 = _rootRecidRecid
                    ?: BTreeMap.putEmptyRoot(db.store, _keySerializer , _valueSerializer)
            catalog[name + Keys.rootRecidRecid] = rootRecidRecid2.toString()

            val counterRecid2 =
                    if (_counterEnable) _counterRecid ?: db.store.put(0L, Serializer.LONG)
                    else 0L
            catalog[name + Keys.counterRecid] = counterRecid2.toString()

            catalog[name + Keys.maxNodeSize] = _maxNodeSize.toString()

            return BTreeMap(
                    keySerializer = _keySerializer,
                    valueSerializer = _valueSerializer,
                    rootRecidRecid = rootRecidRecid2,
                    store = db.store,
                    maxNodeSize = _maxNodeSize,
                    comparator = _keySerializer, //TODO custom comparator
                    threadSafe = _threadSafe, //TODO threadSafe in catalog?
                    counterRecid = counterRecid2,
                    hasValues = hasValues
            )
        }

        override fun open2(catalog: SortedMap<String, String>): BTreeMap<K, V> {
            val rootRecidRecid2 = catalog[name + Keys.rootRecidRecid]!!.toLong()

            _keySerializer =
                    db.nameCatalogGetClass(catalog, name +
                            if(hasValues)Keys.keySerializer else Keys.serializer)
                            ?: _keySerializer
            _valueSerializer =
                    if(!hasValues) {
                        BTreeMap.NO_VAL_SERIALIZER as GroupSerializer<V>
                    }else {
                        db.nameCatalogGetClass(catalog, name + Keys.valueSerializer) ?: _valueSerializer
                    }

            val counterRecid2 = catalog[name + Keys.counterRecid]!!.toLong()
            _maxNodeSize = catalog[name + Keys.maxNodeSize]!!.toInt()
            return BTreeMap(
                    keySerializer = _keySerializer,
                    valueSerializer = _valueSerializer,
                    rootRecidRecid = rootRecidRecid2,
                    store = db.store,
                    maxNodeSize = _maxNodeSize,
                    comparator = _keySerializer, //TODO custom comparator
                    threadSafe = _threadSafe, //TODO threadSafe in catalog?
                    counterRecid = counterRecid2,
                    hasValues = hasValues
            )
        }

        override fun create(): BTreeMap<K, V> {
            return super.create()
        }

        override fun createOrOpen(): BTreeMap<K, V> {
            return super.createOrOpen()
        }

        override fun open(): BTreeMap<K, V> {
            return super.open()
        }
    }

    class TreeSetMaker<E>(
            override val db:DB,
            override val name:String
    ) :Maker<NavigableSet<E>>(){

        protected val maker = TreeMapMaker<E, Any?>(db, name, hasValues = false)


        fun <A> serializer(serializer:GroupSerializer<A>):TreeSetMaker<A>{
            maker.keySerializer(serializer)
            return this as TreeSetMaker<A>
        }

        fun maxNodeSize(size:Int):TreeSetMaker<E>{
            maker.maxNodeSize(size)
            return this;
        }

        fun counterEnable():TreeSetMaker<E>{
            maker.counterEnable()
            return this;
        }


        fun threadSafeDisable():TreeSetMaker<E>{
            maker.threadSafeDisable()
            return this;
        }


        override fun verify() {
            maker.verify()
        }

        override fun open2(catalog: SortedMap<String, String>): NavigableSet<E> {
            return maker.open2(catalog).keys as NavigableSet<E>
        }

        override fun create2(catalog: SortedMap<String, String>): NavigableSet<E> {
            return maker.create2(catalog).keys as NavigableSet<E>
        }

        override val type = "TreeSet"
    }

    fun treeMap(name:String):TreeMapMaker<*,*> = TreeMapMaker<Any?, Any?>(this, name)
    fun <K,V> treeMap(name:String, keySerializer: GroupSerializer<K>, valueSerializer: GroupSerializer<V>) =
            TreeMapMaker<K,V>(this, name)
                    .keySerializer(keySerializer)
                    .valueSerializer(valueSerializer)

    fun treeSet(name:String):TreeSetMaker<*> = TreeSetMaker<Any?>(this, name)
    fun <E> treeSet(name:String, serializer: GroupSerializer<E>) =
            TreeSetMaker<E>(this, name)
                    .serializer(serializer)



    class HashSetMaker<E>(
            override val db:DB,
            override val name:String
    ) :Maker<HTreeMap.KeySet<E>>(){

        protected val maker = HashMapMaker<E, Any?>(db, name, hasValues=false)


        fun <A> serializer(serializer:Serializer<A>):HashSetMaker<A>{
            maker.keySerializer(serializer)
            return this as HashSetMaker<A>
        }

        fun counterEnable():HashSetMaker<E>{
            maker.counterEnable()
            return this;
        }

        fun removeCollapsesIndexTreeDisable():HashSetMaker<E>{
            maker.removeCollapsesIndexTreeDisable()
            return this
        }

        fun hashSeed(hashSeed:Int):HashSetMaker<E>{
            maker.hashSeed(hashSeed)
            return this
        }

        fun layout(concurrency:Int, dirSize:Int, levels:Int):HashSetMaker<E>{
            maker.layout(concurrency, dirSize, levels)
            return this
        }

        fun expireAfterCreate():HashSetMaker<E>{
            return expireAfterCreate(-1)
        }

        fun expireAfterCreate(ttl:Long):HashSetMaker<E>{
            maker.expireAfterCreate(ttl)
            return this
        }


        fun expireAfterCreate(ttl:Long, unit:TimeUnit):HashSetMaker<E> {
            return expireAfterCreate(unit.toMillis(ttl))
        }

        fun expireAfterGet():HashSetMaker<E>{
            return expireAfterGet(-1)
        }

        fun expireAfterGet(ttl:Long):HashSetMaker<E>{
            maker.expireAfterGet(ttl)
            return this
        }


        fun expireAfterGet(ttl:Long, unit:TimeUnit):HashSetMaker<E> {
            return expireAfterGet(unit.toMillis(ttl))
        }


        fun expireExecutor(executor: ScheduledExecutorService?):HashSetMaker<E>{
            maker.expireExecutor(executor)
            return this
        }

        fun expireExecutorPeriod(period:Long):HashSetMaker<E>{
            maker.expireExecutorPeriod(period)
            return this
        }

        fun expireCompactThreshold(freeFraction: Double):HashSetMaker<E>{
            maker.expireCompactThreshold(freeFraction)
            return this
        }


        fun expireMaxSize(maxSize:Long):HashSetMaker<E>{
            maker.expireMaxSize(maxSize)
            return this
        }

        fun expireStoreSize(storeSize:Long):HashSetMaker<E>{
            maker.expireStoreSize(storeSize)
            return this
        }


        internal fun storeFactory(storeFactory:(segment:Int)->Store):HashSetMaker<E>{
            maker.storeFactory(storeFactory)
            return this
        }

        override fun verify() {
            maker.verify()
        }

        override fun open2(catalog: SortedMap<String, String>): HTreeMap.KeySet<E> {
            return maker.open2(catalog).keys
        }

        override fun create2(catalog: SortedMap<String, String>): HTreeMap.KeySet<E> {
            return maker.create2(catalog).keys
        }

        override val type = "HashSet"
    }

    fun hashSet(name:String):HashSetMaker<*> = HashSetMaker<Any?>(this, name)
    fun <E> hashSet(name:String, serializer: Serializer<E>) =
            HashSetMaker<E>(this, name)
                    .serializer(serializer)



    abstract class Maker<E>(){
        open fun create():E = make2( true)
        open fun make():E = make2(null)
        open fun createOrOpen():E = make2(null)
        open fun open():E = make2( false)

        protected fun make2(create:Boolean?):E{
            Utils.lockWrite(db.lock){
                verify()
                val ref = db.namesInstanciated.getIfPresent(name)
                if(ref!=null)
                    return ref as E;

                val catalog = db.nameCatalogLoad()
                //check existence
                val typeFromDb = catalog[name+Keys.type]
                if (create != null) {
                    if (typeFromDb!=null && create)
                        throw DBException.WrongConfiguration("Named record already exists: $name")
                    if (!create && typeFromDb==null)
                        throw DBException.WrongConfiguration("Named record does not exist: $name")
                }
                //check type
                if(typeFromDb!=null && type!=typeFromDb){
                    throw DBException.WrongConfiguration("Wrong type for named record '$name'. Expected '$type', but catalog has '$typeFromDb'")
                }

                if(typeFromDb!=null) {
                    val ret = open2(catalog)
                    db.namesInstanciated.put(name,ret)
                    return ret;
                }

                catalog.put(name+Keys.type,type)
                val ret = create2(catalog)
                db.nameCatalogSave(catalog)
                db.namesInstanciated.put(name,ret)
                return ret
            }
        }

        open internal fun verify(){}
        abstract internal fun create2(catalog:SortedMap<String,String>):E
        abstract internal fun open2(catalog:SortedMap<String,String>):E

        abstract protected val db:DB
        abstract protected val name:String
        abstract protected val type:String
    }

    class AtomicIntegerMaker(override val db:DB, override val name:String, val value:Int=0):Maker<Atomic.Integer>(){

        override val type = "AtomicInteger"

        override fun create2(catalog: SortedMap<String, String>): Atomic.Integer {
            val recid = db.store.put(value, Serializer.INTEGER)
            catalog[name+Keys.recid] = recid.toString()
            return Atomic.Integer(db.store, recid)
        }

        override fun open2(catalog: SortedMap<String, String>): Atomic.Integer {
            val recid = catalog[name+Keys.recid]!!.toLong()
            return Atomic.Integer(db.store, recid)
        }
    }

    fun atomicInteger(name:String) = AtomicIntegerMaker(this, name)

    fun atomicInteger(name:String, value:Int) = AtomicIntegerMaker(this, name, value)



    class AtomicLongMaker(override val db:DB, override val name:String, val value:Long=0):Maker<Atomic.Long>(){

        override val type = "AtomicLong"

        override fun create2(catalog: SortedMap<String, String>): Atomic.Long {
            val recid = db.store.put(value, Serializer.LONG)
            catalog[name+Keys.recid] = recid.toString()
            return Atomic.Long(db.store, recid)
        }

        override fun open2(catalog: SortedMap<String, String>): Atomic.Long {
            val recid = catalog[name+Keys.recid]!!.toLong()
            return Atomic.Long(db.store, recid)
        }
    }

    fun atomicLong(name:String) = AtomicLongMaker(this, name)

    fun atomicLong(name:String, value:Long) = AtomicLongMaker(this, name, value)


    class AtomicBooleanMaker(override val db:DB, override val name:String, val value:Boolean=false):Maker<Atomic.Boolean>(){

        override val type = "AtomicBoolean"

        override fun create2(catalog: SortedMap<String, String>): Atomic.Boolean {
            val recid = db.store.put(value, Serializer.BOOLEAN)
            catalog[name+Keys.recid] = recid.toString()
            return Atomic.Boolean(db.store, recid)
        }

        override fun open2(catalog: SortedMap<String, String>): Atomic.Boolean {
            val recid = catalog[name+Keys.recid]!!.toLong()
            return Atomic.Boolean(db.store, recid)
        }
    }

    fun atomicBoolean(name:String) = AtomicBooleanMaker(this, name)

    fun atomicBoolean(name:String, value:Boolean) = AtomicBooleanMaker(this, name, value)


    class AtomicStringMaker(override val db:DB, override val name:String, val value:String?=null):Maker<Atomic.String>(){

        override val type = "AtomicString"

        override fun create2(catalog: SortedMap<String, String>): Atomic.String {
            val recid = db.store.put(value, Serializer.STRING_NOSIZE)
            catalog[name+Keys.recid] = recid.toString()
            return Atomic.String(db.store, recid)
        }

        override fun open2(catalog: SortedMap<String, String>): Atomic.String {
            val recid = catalog[name+Keys.recid]!!.toLong()
            return Atomic.String(db.store, recid)
        }
    }

    fun atomicString(name:String) = AtomicStringMaker(this, name)

    fun atomicString(name:String, value:String?) = AtomicStringMaker(this, name, value)


    class AtomicVarMaker<E>(override val db:DB,
                            override val name:String,
                            protected val serializer:Serializer<E> = Serializer.JAVA as Serializer<E>,
                            protected val value:E? = null):Maker<Atomic.Var<E>>(){

        override val type = "AtomicVar"

        override fun create2(catalog: SortedMap<String, String>): Atomic.Var<E> {
            val recid = db.store.put(value, serializer)
            catalog[name+Keys.recid] = recid.toString()
            db.nameCatalogPutClass(catalog, name+Keys.serializer, serializer)

            return Atomic.Var(db.store, recid, serializer)
        }

        override fun open2(catalog: SortedMap<String, String>): Atomic.Var<E> {
            val recid = catalog[name+Keys.recid]!!.toLong()
            val serializer = db.nameCatalogGetClass<Serializer<E>>(catalog, name+Keys.serializer)
                    ?: this.serializer
            return Atomic.Var(db.store, recid, serializer)
        }
    }

    fun atomicVar(name:String) = atomicVar(name, Serializer.JAVA)
    fun <E> atomicVar(name:String, serializer:Serializer<E> ) = AtomicVarMaker(this, name, serializer)

    fun <E> atomicVar(name:String, serializer:Serializer<E>, value:E? ) = AtomicVarMaker(this, name, serializer, value)

    class IndexTreeLongLongMapMaker(
            override val db:DB,
            override val name:String
    ):Maker<IndexTreeLongLongMap>(){

        private var _dirShift = CC.HTREEMAP_DIR_SHIFT
        private var _levels = CC.HTREEMAP_LEVELS
        private var _removeCollapsesIndexTree:Boolean = true

        override val type = "IndexTreeLongLongMap"

        fun layout(dirSize:Int, levels:Int):IndexTreeLongLongMapMaker{
            fun toShift(value:Int):Int{
                return 31 - Integer.numberOfLeadingZeros(DBUtil.nextPowTwo(Math.max(1,value)))
            }
            _dirShift = toShift(dirSize)
            _levels = levels
            return this
        }


        fun removeCollapsesIndexTreeDisable():IndexTreeLongLongMapMaker{
            _removeCollapsesIndexTree = false
            return this
        }



        override fun create2(catalog: SortedMap<String, String>): IndexTreeLongLongMap {
            catalog[name+Keys.dirShift] = _dirShift.toString()
            catalog[name+Keys.levels] = _levels.toString()
            catalog[name + Keys.removeCollapsesIndexTree] = _removeCollapsesIndexTree.toString()

            val rootRecid = db.store.put(IndexTreeListJava.dirEmpty(), IndexTreeListJava.dirSer)
            catalog[name+Keys.rootRecid] = rootRecid.toString()
            return IndexTreeLongLongMap(
                    store=db.store,
                    rootRecid = rootRecid,
                    dirShift = _dirShift,
                    levels=_levels,
                    collapseOnRemove = _removeCollapsesIndexTree);
        }

        override fun open2(catalog: SortedMap<String, String>): IndexTreeLongLongMap {
            return IndexTreeLongLongMap(
                    store = db.store,
                    dirShift = catalog[name+Keys.dirShift]!!.toInt(),
                    levels = catalog[name+Keys.levels]!!.toInt(),
                    rootRecid = catalog[name+Keys.rootRecid]!!.toLong(),
                    collapseOnRemove = catalog[name + Keys.removeCollapsesIndexTree]!!.toBoolean())
        }
    }

    //TODO this is thread unsafe, but locks should not be added directly due to code overhead on HTreeMap
    fun indexTreeLongLongMap(name: String) = IndexTreeLongLongMapMaker(this, name)


    class IndexTreeListMaker<E>(
            override val db:DB,
            override val name:String,
            protected val serializer:Serializer<E>
    ):Maker<IndexTreeList<E>>(){

        private var _dirShift = CC.HTREEMAP_DIR_SHIFT
        private var _levels = CC.HTREEMAP_LEVELS
        private var _removeCollapsesIndexTree:Boolean = true

        override val type = "IndexTreeLongLongMap"

        fun layout(dirSize:Int, levels:Int):IndexTreeListMaker<E>{
            fun toShift(value:Int):Int{
                return 31 - Integer.numberOfLeadingZeros(DBUtil.nextPowTwo(Math.max(1,value)))
            }
            _dirShift = toShift(dirSize)
            _levels = levels
            return this
        }


        fun removeCollapsesIndexTreeDisable():IndexTreeListMaker<E>{
            _removeCollapsesIndexTree = false
            return this
        }

        override fun create2(catalog: SortedMap<String, String>): IndexTreeList<E> {
            catalog[name+Keys.dirShift] = _dirShift.toString()
            catalog[name+Keys.levels] = _levels.toString()
            catalog[name + Keys.removeCollapsesIndexTree] = _removeCollapsesIndexTree.toString()
            db.nameCatalogPutClass(catalog, name + Keys.serializer, serializer)

            val counterRecid = db.store.put(0L, Serializer.LONG_PACKED)
            catalog[name+Keys.counterRecid] = counterRecid.toString()
            val rootRecid = db.store.put(IndexTreeListJava.dirEmpty(), IndexTreeListJava.dirSer)
            catalog[name+Keys.rootRecid] = rootRecid.toString()
            val map = IndexTreeLongLongMap(
                    store=db.store,
                    rootRecid = rootRecid,
                    dirShift = _dirShift,
                    levels=_levels,
                    collapseOnRemove = _removeCollapsesIndexTree);

            return IndexTreeList(
                    store = db.store,
                    map = map,
                    serializer = serializer,
                    isThreadSafe = true,
                    counterRecid = counterRecid
            )
        }

        override fun open2(catalog: SortedMap<String, String>): IndexTreeList<E> {
            val map =  IndexTreeLongLongMap(
                    store = db.store,
                    dirShift = catalog[name+Keys.dirShift]!!.toInt(),
                    levels = catalog[name+Keys.levels]!!.toInt(),
                    rootRecid = catalog[name+Keys.rootRecid]!!.toLong(),
                    collapseOnRemove = catalog[name + Keys.removeCollapsesIndexTree]!!.toBoolean())
            return IndexTreeList(
                    store = db.store,
                    map = map,
                    serializer =  db.nameCatalogGetClass(catalog, name + Keys.serializer)?: serializer,
                    isThreadSafe = true,
                    counterRecid = catalog[name+Keys.counterRecid]!!.toLong()
            )
        }
    }

    fun <E> indexTreeList(name: String, serializer:Serializer<E>) = IndexTreeListMaker(this, name, serializer)
    fun indexTreeList(name: String) = indexTreeList(name, Serializer.JAVA)

}