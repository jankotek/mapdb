package org.mapdb

import com.gs.collections.api.map.primitive.MutableLongLongMap
import com.gs.collections.impl.list.mutable.primitive.LongArrayList
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
){

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

        val keyInline = ".keyInline"
        val valueInline = ".valueInline"

        val counterRecids = ".counterRecids"

        val hashSeed = ".hashSeed"
        val segmentRecids = ".segmentRecids"

        val expireCreateTTL = ".expireCreateTTL"
        val expireUpdateTTL = ".expireUpdateTTL"
        val expireGetTTL = ".expireGetTTL"

        val rootRecidRef = ".rootRecidRef"
        val rootRecids = ".rootRecids"
        /** concurrency shift, 1<<it is number of concurrent segments in HashMap*/
        val concShift = ".concShift"
        val dirShift = ".dirShift"
        val levels = ".levels"

//        val maxNodeSize = ".maxNodeSize"
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

    fun close(){
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
            private val _db:DB,
            private val _name:String
    ){
        private var _keySerializer:Serializer<*> = Serializer.JAVA
        private var _valueSerializer:Serializer<*> = Serializer.JAVA
        private var _keyInline = false
        private var _valueInline = false

        private var _concShift = 3
        private var _dirShift = 4
        private var _levels = 4
        private var _hashSeed:Int? = null
        private var _expireCreateTTL:Long = 0L
        private var _expireUpdateTTL:Long = 0L
        private var _expireGetTTL:Long = 0L
        private var _expireExecutor:ScheduledExecutorService? = null
        private var _expireExecutorPeriod:Long = 10000
        private var _expireMaxSize:Long = 0
        private var _expireStoreSize:Long = 0

        private var _counterEnable: Boolean = false

        private var _storeFactory:(segment:Int)->Store = {i->_db.store}

        private var _valueCreator:((key:K)->V)? = null
        private var _modListeners:MutableList<MapModificationListener<K,V>>? = null

        fun <A> keySerializer(keySerializer:Serializer<A>):HashMapMaker<A,V>{
            _keySerializer = keySerializer
            return this as HashMapMaker<A, V>
        }

        fun <A> valueSerializer(valueSerializer:Serializer<A>):HashMapMaker<K,A>{
            _valueSerializer = valueSerializer
            return this as HashMapMaker<K, A>
        }

        fun keyInline():HashMapMaker<K,V>{
            _keyInline = true
            return this
        }


        fun valueInline():HashMapMaker<K,V>{
            _valueInline = true
            return this
        }
        fun hashSeed(hashSeed:Int):HashMapMaker<K,V>{
            _hashSeed = hashSeed
            return this
        }

        //TODO convert user facing shifts into roundUp numbers
        fun layout(concShift:Int, levels:Int, dirShift:Int):HashMapMaker<K,V>{
            _concShift = concShift
            _levels = levels
            _dirShift = dirShift
            return this
        }

        fun expireTTL(ttl:Long):HashMapMaker<K,V>{
            _expireCreateTTL = ttl
            _expireUpdateTTL = ttl
            _expireGetTTL = ttl
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

        fun expireMaxSize(maxSize:Long):HashMapMaker<K,V>{
            _expireMaxSize = maxSize;
            return counterEnable()
        }

        fun expireStoreSize(storeSize:Long):HashMapMaker<K,V>{
            _expireStoreSize = storeSize;
            return this
        }

        internal fun storeFactory(storeFactory:(segment:Int)->Store):HashMapMaker<K,V>{
            _storeFactory = storeFactory
            return this
        }

        fun valueCreator(valueCreator:(key:K)->V):HashMapMaker<K,V>{
            _valueCreator = valueCreator
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


        internal fun run(create:Boolean?) = _db.hashMapInternal(
                name = _name,
                create = true,
                keySerializer = _keySerializer as Serializer<K>,
                valueSerializer = _valueSerializer as Serializer<V>,
                keyInline = _keyInline,
                valueInline = _valueInline,
                concShift = _concShift,
                dirShift = _dirShift,
                levels = _levels,
                hashSeed = _hashSeed,
                counterEnable = _counterEnable,
                expireCreateTTL = _expireCreateTTL,
                expireUpdateTTL = _expireUpdateTTL,
                expireGetTTL = _expireGetTTL,
                expireMaxSize = _expireMaxSize,
                expireStoreSize = _expireStoreSize,
                expireExecutor = _expireExecutor,
                expireExecutorPeriod = _expireExecutorPeriod,
                storeFactory = _storeFactory,
                valueCreator = _valueCreator,
                modificationListeners =  _modListeners?.toTypedArray()
        )

        fun create() = run(true)
        fun createOrOpen() = run(null)
        fun open() = run(false)


    }

    fun hashMap(name:String):HashMapMaker<*,*> = HashMapMaker<Any?,Any?>(this, name)
    fun <K,V> hashMap(name:String, keySerializer: Serializer<K>, valueSerializer: Serializer<V>) =
            HashMapMaker<K,V>(this, name)
            .keySerializer(keySerializer)
            .valueSerializer(valueSerializer)

    internal fun <K,V> hashMapInternal(
            name:String,
            create:Boolean?,
            keySerializer:Serializer<K>,
            valueSerializer:Serializer<V>,
            keyInline:Boolean,
            valueInline:Boolean,
            concShift:Int,
            dirShift:Int,
            levels:Int,
            hashSeed:Int?,
            counterEnable:Boolean,
            expireCreateTTL:Long,
            expireUpdateTTL:Long,
            expireGetTTL:Long,
            expireMaxSize:Long,
            expireStoreSize:Long,
            expireExecutor: ScheduledExecutorService?,
            expireExecutorPeriod:Long,
            storeFactory:(segment:Int)->Store,
            valueCreator:((key:K)->V)?,
            modificationListeners: Array<MapModificationListener<K,V>>?

    ):HTreeMap<K,V>{
        checkName(name)
        val hashSeed = hashSeed ?: SecureRandom().nextInt()
        val nameCatalog = nameCatalogLoad()

        val contains = nameCatalog.containsKey(name + Keys.type);
        if (create != null) {
            if (contains && create)
                throw DBException.WrongConfiguration("Named record already exists: $name")
            if (!create && !contains)
                throw DBException.WrongConfiguration("Named record does not exist: $name")
        }

        val segmentCount = 1.shl(concShift)
        val stores = Array(segmentCount, storeFactory)

        val rootRecids = LongArray(segmentCount)
        var rootRecidsStr = "";
        for(i in 0 until segmentCount) {
            val rootRecid = stores[i].put(IndexTreeListJava.dirEmpty(), IndexTreeListJava.dirSer)
            rootRecids[i] = rootRecid
            rootRecidsStr+= (if(i==0)"" else ",") + rootRecid
        }

        nameCatalog.put(name + Keys.type, "HashMap")
        nameCatalogPutClass(nameCatalog, name + Keys.keySerializer, keySerializer)
        nameCatalogPutClass(nameCatalog, name + Keys.valueSerializer, valueSerializer)

        nameCatalog.put(name + Keys.keyInline, keyInline.toString())
        nameCatalog.put(name + Keys.valueInline, valueInline.toString())

        nameCatalog.put(name + Keys.rootRecids, rootRecidsStr)
        nameCatalog.put(name + Keys.hashSeed, hashSeed.toString())
        nameCatalog.put(name + Keys.concShift, concShift.toString())
        nameCatalog.put(name + Keys.dirShift, dirShift.toString())
        nameCatalog.put(name + Keys.levels, levels.toString())

        val counterRecids:LongArray? = if(counterEnable){
            val cr = LongArray(segmentCount, {segment->
                stores[segment].put(0L, Serializer.LONG_PACKED)
            })
            nameCatalog.put(name + Keys.counterRecids, LongArrayList.newListWith(*cr).makeString("",",",""))
            cr
        }else{
            nameCatalog.put(name + Keys.counterRecids, "")
            null
        }

        nameCatalog.put(name + Keys.expireCreateTTL, expireCreateTTL.toString())
        nameCatalog.put(name + Keys.expireUpdateTTL, expireUpdateTTL.toString())
        nameCatalog.put(name + Keys.expireGetTTL, expireGetTTL.toString())

        fun emptyLongQueue(segment:Int):QueueLong{
            val store = stores[segment]
            val q = store.put(null, QueueLong.Node.SERIALIZER);
            val tailRecid = store.put(q, Serializer.RECID)
            val headRecid = store.put(q, Serializer.RECID)
            val headPrevRecid = store.put(0L, Serializer.RECID)
            return QueueLong(store=store, tailRecid = tailRecid, headRecid = headRecid, headPrevRecid = headPrevRecid)
        }

        val expireCreateQueues:Array<QueueLong>? =
                if(expireCreateTTL==0L) null
                else Array(segmentCount,{emptyLongQueue(it)})

        val expireUpdateQueues:Array<QueueLong>? =
                if(expireUpdateTTL==0L) null
                else Array(segmentCount,{emptyLongQueue(it)})

        val expireGetQueues:Array<QueueLong>? =
                if(expireGetTTL==0L) null
                else Array(segmentCount,{emptyLongQueue(it)})

        if(expireExecutor!=null)
            executors.add(expireExecutor)

        nameCatalogSave(nameCatalog)

        val indexTrees = Array<MutableLongLongMap>(1.shl(concShift),{segment->
            IndexTreeLongLongMap(
                store=stores[segment],
                rootRecid=rootRecids[segment],
                dirShift = dirShift,
                levels=levels)
        })
        val htreemap = HTreeMap(
                keySerializer=keySerializer,
                valueSerializer = valueSerializer,
                keyInline = keyInline,
                valueInline = valueInline,
                concShift = concShift,
                dirShift = dirShift,
                levels = levels,
                stores = stores,
                indexTrees = indexTrees,
                hashSeed = hashSeed,
                counterRecids = counterRecids,
                expireCreateTTL = expireCreateTTL,
                expireUpdateTTL = expireUpdateTTL,
                expireGetTTL = expireGetTTL,
                expireMaxSize = expireMaxSize,
                expireStoreSize = expireStoreSize,
                expireCreateQueues = expireCreateQueues,
                expireUpdateQueues = expireUpdateQueues,
                expireGetQueues = expireGetQueues,
                expireExecutor = expireExecutor,
                expireExecutorPeriod = expireExecutorPeriod,
                threadSafe = true,
                valueCreator = valueCreator,
                modificationListeners = modificationListeners
        )
        return htreemap
    }



}