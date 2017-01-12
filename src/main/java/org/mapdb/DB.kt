package org.mapdb

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import org.eclipse.collections.api.map.primitive.MutableLongLongMap
import org.eclipse.collections.api.map.primitive.MutableLongValuesMap
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.mapdb.elsa.*
import org.mapdb.elsa.ElsaSerializerPojo.ClassInfo
import org.mapdb.serializer.GroupSerializer
import org.mapdb.serializer.GroupSerializerObjectArray
import org.mapdb.tuple.*
import java.io.Closeable
import java.io.DataInput
import java.io.DataOutput
import java.lang.ref.Reference
import java.lang.ref.WeakReference
import java.math.BigDecimal
import java.math.BigInteger
import java.security.SecureRandom
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.logging.Level

/**
 * A database with easy access to named maps and other collections.
 */
//TODO consistency lock
//TODO rename nemed object
//TOOD metrics logger
open class DB(
        /** Stores all underlying data */
       store:Store,
        /** True if store existed before and was opened, false if store was created and is completely empty */
        protected val storeOpened:Boolean,
        override val isThreadSafe:Boolean = true,
        val classLoader:ClassLoader = Thread.currentThread().contextClassLoader,
        /** type of shutdown hook, 0 is disabled, 1 is hard ref, 2 is weak ref*/
        val shutdownHook:Int = 0
): Closeable, ConcurrencyAware {

    private val store2 = store

    val store:Store
        get() {
            checkNotClosed();
            return store2
        }

    companion object{

        protected val NAME_CATALOG_SERIALIZER:Serializer<SortedMap<String, String>> = object:Serializer<SortedMap<String, String>>{

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

        protected val NAMED_SERIALIZATION_HEADER = 1

        /** list of DB objects to be closed */
        private val shutdownHooks = Collections.synchronizedMap(IdentityHashMap<Any, Any>())

        private var shutdownHookInstalled = AtomicBoolean(false)

        protected fun addShutdownHook(ref:Any){
            if(shutdownHookInstalled.compareAndSet(false, true)){
                Runtime.getRuntime().addShutdownHook(object:Thread(){
                    override fun run() {
                        for(o in shutdownHooks.keys.toTypedArray()) { //defensive copy, DB.close() modifies the set
                            try {
                                var a = o
                                if (a is Reference<*>)
                                    a = a.get()
                                if (a is DB)
                                    a.close()
                            } catch(e: Throwable) {
                                //consume all exceptions from this DB object, so other DBs are also closed
                                Utils.LOG.log(Level.SEVERE, "DB.close() thrown exception in shutdown hook.", e)
                            }
                        }
                    }
                })
            }
            shutdownHooks.put(ref, ref)
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

        val expireCreateQueue = "#expireCreateQueue"
        val expireUpdateQueue = "#expireUpdateQueue"
        val expireGetQueue = "#expireGetQueue"


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

    protected val lock = if(isThreadSafe) ReentrantReadWriteLock() else null

    private  val closed = AtomicBoolean(false);

    protected fun checkNotClosed(){
        if(closed.get())
            throw IllegalAccessError("DB was closed")
    }

    /** Already loaded named collections. Values are weakly referenced. We need singletons for locking */
    protected var namesInstanciated: Cache<String, Any?> = CacheBuilder.newBuilder().concurrencyLevel(1).weakValues().build()


    private val classSingletonCat = IdentityHashMap<Any,String>()
    private val classSingletonRev = HashMap<String, Any>()

    private val unknownClasses = Collections.synchronizedSet(HashSet<Class<*>>())

    private fun namedClasses() = arrayOf(
            BTreeMap::class.java,
            HTreeMap::class.java,
            HTreeMap.KeySet::class.java,
            BTreeMapJava.KeySet::class.java,
            Atomic.Integer::class.java,
            Atomic.Long::class.java,
            Atomic.String::class.java,
            Atomic.Boolean::class.java,
            Atomic.Var::class.java,
            IndexTreeList::class.java
    )

    private val nameSer = object:ElsaSerializerBase.Ser<Any>(){
        override fun serialize(out: DataOutput, value: Any, objectStack: ElsaStack?) {
            val name = getNameForObject(value)
                    ?: throw DBException.SerializationError("Could not serialize named object, it was not instantiated by this db")

            out.writeUTF(name)
        }
    }

    private val nameDeser = object:ElsaSerializerBase.Deser<Any>(){
        override fun deserialize(input: DataInput, objectStack: ElsaStack): Any? {
            val name = input.readUTF()
            return this@DB.get(name)
        }
    }

    private val elsaSerializer:ElsaSerializerPojo = ElsaSerializerPojo(
            0,
            pojoSingletons(),
            //TODO add Tuples into default serializer
            namedClasses().map { Pair(it, nameSer) }.toMap(),
            namedClasses().map { Pair(it, NAMED_SERIALIZATION_HEADER)}.toMap(),
            mapOf(Pair(NAMED_SERIALIZATION_HEADER, nameDeser)),
            ElsaClassCallback { unknownClasses.add(it) },
            object:ElsaClassInfoResolver {
                override fun classToId(className: String): Int {
                    val classInfos = loadClassInfos()
                    classInfos.forEachIndexed { i, classInfo ->
                        if(classInfo.name==className)
                            return i
                    }
                    return -1
                }

                override fun getClassInfo(classId: Int): ElsaSerializerPojo.ClassInfo? {
                    return loadClassInfos()[classId]
                }
            } )

    protected fun  <K> serializerForClass(clazz: Class<K>): GroupSerializer<K> {
        return when(clazz){

            Character.TYPE -> Serializer.CHAR
            Char::class.java -> Serializer.CHAR
            String::class.java -> Serializer.STRING
            java.lang.Long.TYPE -> Serializer.LONG
            Long::class.java -> Serializer.LONG
            Integer.TYPE -> Serializer.INTEGER
            Int::class.java -> Serializer.INTEGER
            java.lang.Boolean.TYPE -> Serializer.BOOLEAN
            Boolean::class.java -> Serializer.BOOLEAN
            ByteArray::class.java -> Serializer.BYTE_ARRAY
            CharArray::class.java -> Serializer.CHAR_ARRAY
            IntArray::class.java -> Serializer.INT_ARRAY
            LongArray::class.java -> Serializer.LONG_ARRAY
            DoubleArray::class.java -> Serializer.DOUBLE_ARRAY
            UUID::class.java -> Serializer.UUID
            java.lang.Byte.TYPE -> Serializer.BYTE
            Byte::class.java -> Serializer.BYTE
            java.lang.Float.TYPE -> Serializer.FLOAT
            Float::class.java -> Serializer.FLOAT
            java.lang.Double.TYPE -> Serializer.DOUBLE
            Double::class.java -> Serializer.DOUBLE
            java.lang.Short.TYPE -> Serializer.SHORT
            Short::class.java -> Serializer.SHORT
            ShortArray::class.java -> Serializer.SHORT_ARRAY
            FloatArray::class.java -> Serializer.FLOAT_ARRAY
            BigDecimal::class.java -> Serializer.BIG_DECIMAL
            BigInteger::class.java -> Serializer.BIG_INTEGER
            Class::class.java -> Serializer.CLASS
            Date::class.java -> Serializer.DATE
            Tuple2::class.java -> Tuple2Serializer<Any?,Any?>(defaultSerializer)
            Tuple3::class.java -> Tuple3Serializer<Any?,Any?,Any?>(defaultSerializer)
            Tuple4::class.java -> Tuple4Serializer<Any?,Any?,Any?,Any?>(defaultSerializer)
            Tuple5::class.java -> Tuple5Serializer<Any?,Any?,Any?,Any?,Any?>(defaultSerializer)
            Tuple6::class.java -> Tuple6Serializer<Any?,Any?,Any?,Any?,Any?,Any?>(defaultSerializer)

            else -> defaultSerializer
        } as GroupSerializer<K>
    }
    /**
     * Default serializer used if collection does not specify specialized serializer.
     * It uses Elsa Serializer.
     */
    val defaultSerializer:GroupSerializer<Any?> = object: GroupSerializerObjectArray<Any?>() {

        override fun deserialize(input: DataInput2, available: Int): Any? {
            return elsaSerializer.deserialize(input)
        }

        override fun serialize(out: DataOutput2, value: Any) {
            elsaSerializer.serialize(out, value)
        }

    }

    //fun <E> getDefaultSerializer() = defaultSerializer as GroupSerializer<E>


    protected val classInfoSerializer = object : Serializer<Array<ClassInfo>> {

        override fun serialize(out: DataOutput2, ci: Array<ClassInfo>) {
            out.packInt(ci.size)
            for(c in ci)
                elsaSerializer.classInfoSerialize(out, c)
        }

        override fun deserialize(input: DataInput2, available: Int): Array<ClassInfo> {
            return Array(input.unpackInt(), {
                elsaSerializer.classInfoDeserialize(input)
            })
        }

    }

    init{
        if(storeOpened.not()){
            //create new structure
            if(store.isReadOnly){
                throw DBException.WrongConfiguration("Can not create new store in read-only mode")
            }
            //preallocate 16 recids
            val nameCatalogRecid = store.put(TreeMap<String, String>(), NAME_CATALOG_SERIALIZER)
            if(CC.RECID_NAME_CATALOG != nameCatalogRecid)
                throw DBException.WrongConfiguration("Store does not support Reserved Recids: "+store.javaClass)

            val classCatalogRecid = store.put(arrayOf<ClassInfo>(), classInfoSerializer)
            if(CC.RECID_CLASS_INFOS != classCatalogRecid)
                throw DBException.WrongConfiguration("Store does not support Reserved Recids: "+store.javaClass)


            for(recid in 3L..CC.RECID_MAX_RESERVED){
                val recid2 = store.put(null, Serializer.LONG_PACKED)
                if(recid!==recid2){
                    throw DBException.WrongConfiguration("Store does not support Reserved Recids: "+store.javaClass)
                }
            }
            store.commit()
        }

        val msgs = nameCatalogVerifyGetMessages().toList()
        if(!msgs.isEmpty())
            throw DBException.NewMapDBFormat("Name Catalog has some new unsupported features: "+msgs.toString());
    }


    init{
        //read all singleton from Serializer fields
        Serializer::class.java.declaredFields.forEach { f ->
            val name = Serializer::class.java.canonicalName + "#"+f.name
            val obj = f.get(null)
            classSingletonCat.put(obj, name)
            classSingletonRev.put(name, obj)
        }
        val defSerName = "org.mapdb.DB#defaultSerializer"
        classSingletonCat.put(defaultSerializer, defSerName)
        classSingletonRev.put(defSerName, defaultSerializer)

    }


    private val shutdownReference:Any? =
            when(shutdownHook){
                0 -> null
                1 -> this@DB
                2 -> WeakReference(this@DB)
                else -> throw IllegalArgumentException()
            }

    init{
        if(shutdownReference!=null){
            DB.addShutdownHook(shutdownReference)
        }
    }


    private fun pojoSingletons():Array<Any>{
        // NOTE !!! do not change index of any element!!!
        // it is storage format definition
        return arrayOf(
                this@DB, this@DB.defaultSerializer,
                Serializer.CHAR, Serializer.STRING_ORIGHASH , Serializer.STRING, Serializer.STRING_DELTA,
                Serializer.STRING_DELTA2, Serializer.STRING_INTERN, Serializer.STRING_ASCII, Serializer.STRING_NOSIZE,
                Serializer.LONG, Serializer.LONG_PACKED, Serializer.LONG_DELTA, Serializer.INTEGER,
                Serializer.INTEGER_PACKED, Serializer.INTEGER_DELTA, Serializer.BOOLEAN, Serializer.RECID,
                Serializer.RECID_ARRAY, Serializer.ILLEGAL_ACCESS, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY_DELTA,
                Serializer.BYTE_ARRAY_DELTA2, Serializer.BYTE_ARRAY_NOSIZE, Serializer.CHAR_ARRAY, Serializer.INT_ARRAY,
                Serializer.LONG_ARRAY, Serializer.DOUBLE_ARRAY, Serializer.JAVA, Serializer.ELSA, Serializer.UUID,
                Serializer.BYTE, Serializer.FLOAT, Serializer.DOUBLE, Serializer.SHORT, Serializer.SHORT_ARRAY,
                Serializer.FLOAT_ARRAY, Serializer.BIG_INTEGER, Serializer.BIG_DECIMAL, Serializer.CLASS,
                Serializer.DATE,
                Collections.EMPTY_LIST,
                Collections.EMPTY_SET,
                Collections.EMPTY_MAP,
                Serializer.SQL_DATE,
                Serializer.SQL_TIME,
                Serializer.SQL_TIMESTAMP
        )

    }

    private fun loadClassInfos():Array<ElsaSerializerPojo.ClassInfo>{
        return store2.get(CC.RECID_CLASS_INFOS, classInfoSerializer)!!
    }




    /** List of executors associated with this database. Those will be terminated on close() */
    protected val executors:MutableSet<ExecutorService> = Collections.synchronizedSet(LinkedHashSet())

    fun nameCatalogLoad():SortedMap<String, String> {
        return Utils.lockRead(lock){
            checkNotClosed()
            nameCatalogLoadLocked()
        }

    }
    protected fun nameCatalogLoadLocked():SortedMap<String, String> {
        if(CC.ASSERT)
            Utils.assertReadLock(lock)
        return store.get(CC.RECID_NAME_CATALOG, NAME_CATALOG_SERIALIZER)
                ?: throw DBException.WrongConfiguration("Could not open store, it has no Named Catalog");
    }

    fun nameCatalogSave(nameCatalog: SortedMap<String, String>) {
        Utils.lockWrite(lock){
            checkNotClosed()
            nameCatalogSaveLocked(nameCatalog)
        }
    }

    protected fun nameCatalogSaveLocked(nameCatalog: SortedMap<String, String>) {
        if(CC.ASSERT)
            Utils.assertWriteLock(lock)
        store.update(CC.RECID_NAME_CATALOG, nameCatalog, NAME_CATALOG_SERIALIZER)
    }


    private val nameRegex = "[A-Z0-9._-]".toRegex()

    protected fun checkName(name: String) {
        if(name.contains('#'))
            throw DBException.WrongConfiguration("Name contains illegal character, '#' is not allowed.")
        if(!name.matches(nameRegex))
            throw DBException.WrongConfiguration("Name contains illegal character")
    }

    protected fun nameCatalogGet(name: String): String? {
        return nameCatalogLoadLocked()[name]
    }


    fun  nameCatalogPutClass(
            nameCatalog: SortedMap<String, String>,
            key: String,
            obj: Any
    ) {
        val value:String? = classSingletonCat[obj]

        if(value== null){
            //not in singletons, try to resolve public no ARG constructor of given class
            //TODO get public no arg constructor if exist
        }

        if(value!=null)
            nameCatalog.put(key, value)
    }

    fun <E> nameCatalogGetClass(
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


    private fun unknownClassesSave(){
        if(CC.ASSERT)
            Utils.assertWriteLock(lock)
        //TODO batch class dump
        unknownClasses.forEach {
            defaultSerializerRegisterClass_noLock(it)
        }
        unknownClasses.clear()
    }

    fun commit(){
        Utils.lockWrite(lock) {
            checkNotClosed()
            unknownClassesSave()
            store.commit()
        }
    }

    fun rollback(){
        val store = store
        if(store !is StoreTx)
            throw UnsupportedOperationException("Store does not support rollback")

        Utils.lockWrite(lock) {
            checkNotClosed()
            unknownClasses.clear()
            store.rollback()
        }
    }

    fun isClosed() = closed.get();

    override fun close(){
        if(closed.compareAndSet(false,true).not())
            return

        // do not close this DB from JVM shutdown hook
        if(shutdownReference!=null)
            shutdownHooks.remove(shutdownReference)

        Utils.lockWrite(lock) {
            unknownClassesSave()

            //shutdown running executors if any
            executors.forEach { it.shutdown() }
            //await termination on all
            executors.forEach {
                // TODO LOG this could use some warnings, if background tasks fails to shutdown
                while (!it.awaitTermination(1, TimeUnit.DAYS)) {
                }
            }
            executors.clear()
            store2.close()
        }
    }

    fun <E> get(name:String):E{
        Utils.lockWrite(lock) {
            checkNotClosed()
            val type = nameCatalogGet(name + Keys.type)
            return when (type) {
                "HashMap" -> hashMap(name).open()
                "HashSet" -> hashSet(name).open()
                "TreeMap" -> treeMap(name).open()
                "TreeSet" -> treeSet(name).open()

                "AtomicBoolean" -> atomicBoolean(name).open()
                "AtomicInteger" -> atomicInteger(name).open()
                "AtomicVar" -> atomicVar(name).open()
                "AtomicString" -> atomicString(name).open()
                "AtomicLong" -> atomicLong(name).open()

                "IndexTreeList" -> indexTreeList(name).open()
                "IndexTreeLongLongMap" -> indexTreeLongLongMap(name).open()

                null -> null
                else -> DBException.WrongConfiguration("Collection has unknown type: "+type)
            } as E
        }
    }

    fun getNameForObject(e:Any):String? =
            namesInstanciated.asMap().filterValues { it===e }.keys.firstOrNull()

    fun exists(name: String): Boolean {
        Utils.lockRead(lock) {
            checkNotClosed()
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

    fun delete(name:String){
        Utils.lockWrite(lock) {

            val params = nameCatalogParamsFor(name)
            if (params.isEmpty())
                return
            val obj = get<Any>(name)

            fun deleteRecid(serializer: Serializer<*>) {
                val recid = params.get(name + Keys.recid)!!.toLong()
                store.delete(recid, serializer)
            }

            //clear collection
            when (obj) {
                is Atomic.Boolean -> deleteRecid(Serializer.BOOLEAN)
                is Atomic.Integer -> deleteRecid(Serializer.INTEGER)
                is Atomic.Long -> deleteRecid(Serializer.LONG)
                is Atomic.String -> deleteRecid(Serializer.STRING)
                is Atomic.Var<*> -> deleteRecid(obj.serializer)

                is MutableCollection<*> -> obj.clear()
                is MutableMap<*, *> -> obj.clear()
                is MutableLongValuesMap -> obj.clear()

                null -> null
                else -> DBException.WrongConfiguration("Collection has unknown class: " + obj.javaClass)
            }

            //remove all parameters
            val nameParams = nameCatalogLoad()
            nameParams.keys.removeAll(params.keys)
            nameCatalogSave(nameParams)

            //remove instantiated objects
            namesInstanciated.invalidate(name)
        }
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


    open class HTreeMapMaker<K,V, MAP>(
            db:DB,
            name:String,
            protected val hasValues:Boolean=true,
            protected val _storeFactory:(segment:Int)->Store = {i-> db.store}
    ):Maker<MAP>(db,name, if(hasValues) "HashMap" else "HashSet"){
        override fun awareItems() = arrayOf(_keySerializer, _valueSerializer, _valueLoader)

        protected var _keySerializer:Serializer<K> = db.defaultSerializer as Serializer<K>
        protected var _valueSerializer:Serializer<V> = db.defaultSerializer as Serializer<V>
        protected var _valueInline = false

        protected var _concShift = CC.HTREEMAP_CONC_SHIFT
        protected var _dirShift = CC.HTREEMAP_DIR_SHIFT
        protected var _levels = CC.HTREEMAP_LEVELS

        protected var _hashSeed:Int? = null
        protected var _expireCreateTTL:Long = 0L
        protected var _expireUpdateTTL:Long = 0L
        protected var _expireGetTTL:Long = 0L
        protected var _expireExecutor:ScheduledExecutorService? = null
        protected var _expireExecutorPeriod:Long = 10000
        protected var _expireMaxSize:Long = 0
        protected var _expireStoreSize:Long = 0
        protected var _expireCompactThreshold:Double? = null

        protected var _counterEnable: Boolean = false

        protected var _valueLoader:((key:K)->V?)? = null
        protected var _modListeners:MutableList<MapModificationListener<K,V>> = ArrayList()
        protected var _expireOverflow:MutableMap<K,V?>? = null;
        protected var _removeCollapsesIndexTree:Boolean = true


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

        override fun create2(catalog: SortedMap<String, String>): MAP {
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

            catalog[name + Keys.expireCreateQueue] = createQ.makeString("", ",", "")
            if(hasValues)
                catalog[name + Keys.expireUpdateQueue] = updateQ.makeString("", ",", "")
            catalog[name + Keys.expireGetQueue] = getQ.makeString("", ",", "")

            val indexTrees = Array<MutableLongLongMap>(1.shl(_concShift), { segment ->
                IndexTreeLongLongMap(
                        store = stores[segment],
                        rootRecid = rootRecids[segment],
                        dirShift = _dirShift,
                        levels = _levels,
                        collapseOnRemove = _removeCollapsesIndexTree
                )
            })

            val ret = HTreeMap(
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
                    isThreadSafe = db.isThreadSafe,
                    valueLoader = _valueLoader,
                    modificationListeners = if (_modListeners.isEmpty()) null else _modListeners.toTypedArray(),
                    closeable = db,
                    hasValues = hasValues
            )
            return (if(hasValues)ret else ret.keys) as MAP
        }

        override fun open2(catalog: SortedMap<String, String>): MAP {
            val segmentCount = 1.shl(_concShift)
            val stores = Array(segmentCount, _storeFactory)

            _keySerializer =
                    db.nameCatalogGetClass(catalog, name + if(hasValues)Keys.keySerializer else Keys.serializer)
                            ?: _keySerializer
            _valueSerializer = if(!hasValues) BTreeMap.NO_VAL_SERIALIZER as Serializer<V>
            else {
                db.nameCatalogGetClass(catalog, name + Keys.valueSerializer)?: _valueSerializer
            }
            _valueInline = if(hasValues) catalog[name + Keys.valueInline]!!.toBoolean() else true

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

            val expireCreateQueues = queues(_expireCreateTTL, name + Keys.expireCreateQueue)
            val expireUpdateQueues = queues(_expireUpdateTTL, name + Keys.expireUpdateQueue)
            val expireGetQueues = queues(_expireGetTTL, name + Keys.expireGetQueue)

            val indexTrees = Array<MutableLongLongMap>(1.shl(_concShift), { segment ->
                IndexTreeLongLongMap(
                        store = stores[segment],
                        rootRecid = rootRecids[segment],
                        dirShift = _dirShift,
                        levels = _levels,
                        collapseOnRemove = _removeCollapsesIndexTree
                )
            })
            val ret = HTreeMap(
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
                    isThreadSafe = db.isThreadSafe,
                    valueLoader = _valueLoader,
                    modificationListeners = if (_modListeners.isEmpty()) null else _modListeners.toTypedArray(),
                    closeable = db,
                    hasValues = hasValues
            )
            return (if(hasValues)ret else ret.keys) as MAP
        }

    }


    class HashMapMaker<K,V>(
        db:DB,
        name:String,
        storeFactory:(segment:Int)->Store = {i-> db.store}
    ):HTreeMapMaker<K,V,HTreeMap<K,V>>(db,name,true,storeFactory){

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

        fun expireOverflow(overflowMap:MutableMap<K,V?>):HashMapMaker<K,V>{
            _expireOverflow = overflowMap
            return this
        }



        fun valueLoader(valueLoader:(key:K)->V):HashMapMaker<K,V>{
            _valueLoader = valueLoader
            return this
        }

        fun counterEnable():HashMapMaker<K,V>{
            _counterEnable = true
            return this
        }


        fun modificationListener(listener:MapModificationListener<K,V>):HashMapMaker<K,V>{
            if(_modListeners==null)
                _modListeners = ArrayList()
            _modListeners?.add(listener)
            return this;
        }

        override fun create() = make2(true)

        override fun createOrOpen() =  make2(null)

        override fun open() = make2(false)

    }

    fun hashMap(name:String):HashMapMaker<Any?,Any?> = HashMapMaker<Any?, Any?>(this, name)

    fun <K,V> hashMap(name:String, keySerializer: Serializer<K>, valueSerializer: Serializer<V>) =
            HashMapMaker<K,V>(this, name)
                    .keySerializer(keySerializer)
                    .valueSerializer(valueSerializer)


    fun <K,V> hashMap(name:String, keyClass: Class<K>, valueClass: Class<V>) =
            HashMapMaker<K,V>(this, name)
                    .keySerializer(serializerForClass(keyClass))
                    .valueSerializer(serializerForClass(valueClass))



    abstract class TreeMapSink<K,V>:Pump.Sink<Pair<K,V>, BTreeMap<K,V>>(){

        fun put(key:K, value:V) {
            put(Pair(key, value))
        }

        fun putAll(map:SortedMap<K,V>){
            map.forEach { e ->
                put(e.key, e.value)
            }
        }
    }

    abstract class TreeSetSink<E>:Pump.Sink<E, NavigableSet<E>>(){}

    abstract class BTreeMapMaker<K,V,MAP>(
             db:DB,
             name:String,
             protected val hasValues:Boolean
         ) :Maker<MAP>(db,name, if(hasValues)"TreeMap" else "TreeSet"){

        override fun awareItems() = arrayOf(_keySerializer, _valueSerializer, _valueLoader)


        protected var _keySerializer:GroupSerializer<K> = db.defaultSerializer as GroupSerializer<K>
         protected var _valueSerializer:GroupSerializer<V> =
                 (if(hasValues) db.defaultSerializer else BTreeMap.NO_VAL_SERIALIZER) as GroupSerializer<V>
         protected var _maxNodeSize = CC.BTREEMAP_MAX_NODE_SIZE
         protected var _counterEnable: Boolean = false
         protected var _valueLoader:((key:K)->V)? = null
         protected var _modListeners:MutableList<MapModificationListener<K,V>>? = null

         protected var _rootRecidRecid:Long? = null
         protected var _counterRecid:Long? = null
         protected var _valueInline:Boolean = true


         override fun create2(catalog: SortedMap<String, String>): MAP {
             db.nameCatalogPutClass(catalog, name +
                     (if(hasValues)Keys.keySerializer else Keys.serializer), _keySerializer)
             if(hasValues) {
                 db.nameCatalogPutClass(catalog, name + Keys.valueSerializer, _valueSerializer)
                 catalog[name + Keys.valueInline] = _valueInline.toString()
             }

             val rootRecidRecid2 = _rootRecidRecid
                     ?: BTreeMap.putEmptyRoot(db.store, _keySerializer , _valueSerializer)
             catalog[name + Keys.rootRecidRecid] = rootRecidRecid2.toString()

             val counterRecid2 =
                     if (_counterEnable) _counterRecid ?: db.store.put(0L, Serializer.LONG)
                     else 0L
             catalog[name + Keys.counterRecid] = counterRecid2.toString()

             catalog[name + Keys.maxNodeSize] = _maxNodeSize.toString()

             val ret = BTreeMap(
                     keySerializer = _keySerializer,
                     valueSerializer = _valueSerializer,
                     rootRecidRecid = rootRecidRecid2,
                     store = db.store,
                     maxNodeSize = _maxNodeSize,
                     comparator = _keySerializer, //TODO custom comparator
                     isThreadSafe = db.isThreadSafe,
                     counterRecid = counterRecid2,
                     hasValues = hasValues,
                     valueInline = _valueInline,
                     modificationListeners = if(_modListeners==null) null else _modListeners!!.toTypedArray()
             )

             return (if(hasValues) ret else ret.keys) as MAP
         }

         override fun open2(catalog: SortedMap<String, String>): MAP {
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

             //TODO compatibility with older versions, remove before stable version
             if(_valueSerializer!= BTreeMap.Companion.NO_VAL_SERIALIZER &&
                     catalog[name + Keys.valueInline]==null
                     && db.store.isReadOnly.not()){
                 //patch store with default value
                 catalog[name + Keys.valueInline] = "true"
                 db.nameCatalogSaveLocked(catalog)
             }

             _valueInline = (catalog[name + Keys.valueInline]?:"true").toBoolean()
             val ret = BTreeMap(
                     keySerializer = _keySerializer,
                     valueSerializer = _valueSerializer,
                     rootRecidRecid = rootRecidRecid2,
                     store = db.store,
                     maxNodeSize = _maxNodeSize,
                     comparator = _keySerializer, //TODO custom comparator
                     isThreadSafe = db.isThreadSafe,
                     counterRecid = counterRecid2,
                     hasValues = hasValues,
                     valueInline = _valueInline,
                     modificationListeners = if(_modListeners==null)null else _modListeners!!.toTypedArray()
             )
             return (if(hasValues) ret else ret.keys) as MAP
         }

     }

    class TreeMapMaker<K,V>(
            db:DB,
            name:String
    ):BTreeMapMaker<K,V,BTreeMap<K,V>>(db,name,hasValues=true){

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
//
//        fun valueLoader(valueLoader:(key:K)->V):TreeMapMaker<K,V>{
//            //TODO BTree value loader
//            _valueLoader = valueLoader
//            return this
//        }


        fun maxNodeSize(size:Int):TreeMapMaker<K,V>{
            _maxNodeSize = size
            return this;
        }

        fun counterEnable():TreeMapMaker<K,V>{
            _counterEnable = true
            return this;
        }

        fun valuesOutsideNodesEnable():TreeMapMaker<K,V>{
            _valueInline = false
            return this;
        }

        fun modificationListener(listener:MapModificationListener<K,V>):TreeMapMaker<K,V>{
            if(_modListeners==null)
                _modListeners = ArrayList()
            _modListeners?.add(listener)
            return this;
        }

        fun createFrom(iterator:Iterator<Pair<K,V>>):BTreeMap<K,V>{
            val consumer = createFromSink()
            while(iterator.hasNext()){
                consumer.put(iterator.next())
            }
            return consumer.create()
        }

        fun createFrom(source:Iterable<Pair<K,V>>):BTreeMap<K,V> = createFrom(source.iterator())

        fun createFrom(source:SortedMap<K,V>):BTreeMap<K,V>{
            val consumer = createFromSink()
            for(e in source){
                consumer.put(e.key, e.value)
            }
            return consumer.create()
        }

        fun createFromSink(): TreeMapSink<K,V>{

            val consumer = Pump.treeMap(
                    store = db.store,
                    keySerializer = _keySerializer,
                    valueSerializer = _valueSerializer,
                    //TODO add custom comparator, once its enabled
                    dirNodeSize = _maxNodeSize *3/4,
                    leafNodeSize = _maxNodeSize *3/4,
                    valueInline = _valueInline
            )

            return object: TreeMapSink<K,V>(){

                override fun put(e: Pair<K, V>) {
                    consumer.put(e)
                }

                override fun create(): BTreeMap<K, V> {
                    consumer.create()
                    this@TreeMapMaker._rootRecidRecid = consumer.rootRecidRecid
                            ?: throw AssertionError()
                    this@TreeMapMaker._counterRecid =
                            if(_counterEnable) db.store.put(consumer.counter, Serializer.LONG)
                            else 0L
                    return this@TreeMapMaker.make2(create=true)
                }

            }
        }

        override fun create() = make2(true)

        override fun createOrOpen() =  make2(null)

        override fun open() = make2(false)

    }

    class TreeSetMaker<E>(
            db:DB,
            name:String
    ) :BTreeMapMaker<E, Boolean, NavigableSet<E>>(db,name,hasValues=false){


        fun <A> serializer(serializer:GroupSerializer<A>):TreeSetMaker<A>{
            this._keySerializer = serializer as GroupSerializer<E>
            return this as TreeSetMaker<A>
        }

        fun maxNodeSize(size:Int):TreeSetMaker<E>{
            this._maxNodeSize = size
            return this;
        }

        fun counterEnable():TreeSetMaker<E>{
            this._counterEnable = true
            return this;
        }

        fun createFrom(source:Iterable<E>):NavigableSet<E> = createFrom(source.iterator())

        fun createFrom(iterator:Iterator<E>):NavigableSet<E>{
            val consumer = createFromSink()
            while(iterator.hasNext()){
                consumer.put(iterator.next())
            }
            return consumer.create()
        }

        fun createFromSink(): TreeSetSink<E>{

            val consumer = Pump.treeMap(
                    store = db.store,
                    keySerializer = _keySerializer,
                    valueSerializer = _valueSerializer,
                    //TODO add custom comparator, once its enabled
                    dirNodeSize = _maxNodeSize *3/4,
                    leafNodeSize = _maxNodeSize *3/4,
                    hasValues = false
            )

            return object: TreeSetSink<E>(){

                override fun put(e: E) {
                    consumer.put(Pair(e, true))
                }

                override fun create(): NavigableSet<E> {
                    consumer.create()
                    this@TreeSetMaker._rootRecidRecid = consumer.rootRecidRecid
                            ?: throw AssertionError()
                    this@TreeSetMaker._counterRecid =
                            if(_counterEnable) db.store.put(consumer.counter, Serializer.LONG)
                            else 0L
                    return this@TreeSetMaker.make2(create=true)
                }
            }
        }

        override fun create() = make2(true)

        override fun createOrOpen() =  make2(null)

        override fun open() = make2(false)


    }

    fun treeMap(name:String):TreeMapMaker<Any?,Any?> = TreeMapMaker<Any?, Any?>(this, name)
    fun <K,V> treeMap(name:String, keySerializer: GroupSerializer<K>, valueSerializer: GroupSerializer<V>) =
            TreeMapMaker<K,V>(this, name)
                    .keySerializer(keySerializer)
                    .valueSerializer(valueSerializer)

    fun <K,V> treeMap(name:String, keyClass: Class<K>, valueClass: Class<V>) =
            TreeMapMaker<K,V>(this, name)
                    .keySerializer(serializerForClass(keyClass))
                    .valueSerializer(serializerForClass(valueClass))

    fun treeSet(name:String):TreeSetMaker<Any?> = TreeSetMaker<Any?>(this, name)

    fun <E> treeSet(name:String, serializer: GroupSerializer<E>) =
            TreeSetMaker<E>(this, name)
                    .serializer(serializer)

    fun <E> treeSet(name:String, clazz: Class<E>) =
            TreeSetMaker<E>(this, name)
                    .serializer(serializerForClass(clazz))


    class HashSetMaker<E>(
            db:DB,
            name:String,
            storeFactory:(segment:Int)->Store = {i-> db.store}
    ) :HTreeMapMaker<E, Void, HTreeMap.KeySet<E>>(db,name, false, storeFactory){

        init{
            _valueSerializer = BTreeMap.NO_VAL_SERIALIZER as Serializer<Void>
            _valueInline =true
        }

        fun <A> serializer(serializer:Serializer<A>):HashSetMaker<A>{
            _keySerializer = serializer as Serializer<E>
            return this as HashSetMaker<A>
        }



        fun removeCollapsesIndexTreeDisable():HashSetMaker<E>{
            _removeCollapsesIndexTree = false
            return this
        }

        fun hashSeed(hashSeed:Int):HashSetMaker<E>{
            _hashSeed = hashSeed
            return this
        }

        fun layout(concurrency:Int, dirSize:Int, levels:Int):HashSetMaker<E>{
            fun toShift(value:Int):Int{
                return 31 - Integer.numberOfLeadingZeros(DataIO.nextPowTwo(Math.max(1,value)))
            }
            _concShift = toShift(concurrency)
            _dirShift = toShift(dirSize)
            _levels = levels
            return this
        }

        fun expireAfterCreate():HashSetMaker<E>{
            return expireAfterCreate(-1)
        }

        fun expireAfterCreate(ttl:Long):HashSetMaker<E>{
            _expireCreateTTL = ttl
            return this
        }


        fun expireAfterCreate(ttl:Long, unit:TimeUnit):HashSetMaker<E> {
            return expireAfterCreate(unit.toMillis(ttl))
        }


        fun expireAfterGet():HashSetMaker<E>{
            return expireAfterGet(-1)
        }

        fun expireAfterGet(ttl:Long):HashSetMaker<E>{
            _expireGetTTL = ttl
            return this
        }


        fun expireAfterGet(ttl:Long, unit:TimeUnit):HashSetMaker<E>{
            return expireAfterGet(unit.toMillis(ttl))
        }


        fun expireExecutor(executor: ScheduledExecutorService?):HashSetMaker<E>{
            _expireExecutor = executor;
            return this
        }

        fun expireExecutorPeriod(period:Long):HashSetMaker<E>{
            _expireExecutorPeriod = period
            return this
        }

        fun expireCompactThreshold(freeFraction: Double):HashSetMaker<E>{
            _expireCompactThreshold = freeFraction
            return this
        }


        fun expireMaxSize(maxSize:Long):HashSetMaker<E>{
            _expireMaxSize = maxSize;
            return counterEnable()
        }

        fun expireStoreSize(storeSize:Long):HashSetMaker<E>{
            _expireStoreSize = storeSize;
            return this
        }


        fun counterEnable():HashSetMaker<E>{
            _counterEnable = true
            return this
        }

        override fun create() = make2(true)

        override fun createOrOpen() =  make2(null)

        override fun open() = make2(false)

    }

    fun hashSet(name:String):HashSetMaker<Any?> = HashSetMaker<Any?>(this, name)
    fun <E> hashSet(name:String, serializer: Serializer<E>) =
            HashSetMaker<E>(this, name)
                    .serializer(serializer)

    fun <E> hashSet(name:String, clazz: Class<E>) =
            HashSetMaker<E>(this, name)
                    .serializer(serializerForClass(clazz))


    abstract class Maker<E>(
        protected val db:DB,
        protected val name:String,
        protected val type:String
    ){
        /**
         * Creates new collection if it does not exist, or throw {@link DBException.WrongConfiguration}
         * if collection already exists.
         */
        open fun create():E = make2( true)

        protected abstract fun awareItems():Array<Any?>

        /**
         * Create new collection or open existing.
         */
        open fun createOrOpen():E = make2(null)

        /**
         * Open existing collection, or throw {@link DBException.WrongConfiguration}
         * if collection already exists.
         */
        open fun open():E = make2( false)

        protected fun make2(create:Boolean?):E{
            Utils.lockWrite(db.lock){
                db.checkNotClosed()
                verify()

                val catalog = db.nameCatalogLoad()
                //check existence
                val typeFromDb = catalog[name+Keys.type]
                if (create != null) {
                    if (typeFromDb!=null && create)
                        throw DBException.WrongConfiguration("Named record already exists: $name")
                    if (!create && typeFromDb==null)
                        throw DBException.WrongConfiguration("Named record does not exist: $name")
                }
                //check typeg
                if(typeFromDb!=null && type!=typeFromDb){
                    throw DBException.WrongConfiguration("Wrong type for named record '$name'. Expected '$type', but catalog has '$typeFromDb'")
                }

                val ref = db.namesInstanciated.getIfPresent(name)
                if(ref!=null)
                    return ref as E;

                if(typeFromDb!=null) {
                    val ret = open2(catalog)
                    db.namesInstanciated.put(name,ret)
                    return ret;
                }

                if(db.store.isReadOnly)
                    throw UnsupportedOperationException("Read-only")
                catalog.put(name+Keys.type,type)
                val ret = create2(catalog)
                db.nameCatalogSaveLocked(catalog)
                db.namesInstanciated.put(name,ret)
                for(obj in awareItems()){
                    if(obj is DBAware)
                        obj.callbackDB(db)
                    if(obj is NamedRecordAware)
                        obj.callbackRecord(name, ret as Any)
                }
                return ret
            }
        }

        open protected fun verify(){}
        abstract protected fun create2(catalog:SortedMap<String,String>):E
        abstract protected fun open2(catalog:SortedMap<String,String>):E

    }

    class AtomicIntegerMaker(db:DB, name:String, protected val value:Int=0):Maker<Atomic.Integer>(db, name, "AtomicInteger"){

        override fun awareItems(): Array<Any?> = arrayOf()

        override fun create2(catalog: SortedMap<String, String>): Atomic.Integer {
            val recid = db.store.put(value, Serializer.INTEGER)
            catalog[name+Keys.recid] = recid.toString()
            return Atomic.Integer(db.store, recid)
        }

        override fun open2(catalog: SortedMap<String, String>): Atomic.Integer {
            val recid = catalog[name+Keys.recid]!!.toLong()
            return Atomic.Integer(db.store, recid)
        }

        override fun create()= make2(true)
        override fun createOrOpen() = make2(null)
        override fun open() = make2(false)
    }

    fun atomicInteger(name:String) = AtomicIntegerMaker(this, name)

    fun atomicInteger(name:String, value:Int) = AtomicIntegerMaker(this, name, value)



    class AtomicLongMaker(db:DB, name:String, protected val value:Long=0):Maker<Atomic.Long>(db, name, "AtomicLong"){

        override fun awareItems(): Array<Any?> = arrayOf()

        override fun create2(catalog: SortedMap<String, String>): Atomic.Long {
            val recid = db.store.put(value, Serializer.LONG)
            catalog[name+Keys.recid] = recid.toString()
            return Atomic.Long(db.store, recid)
        }

        override fun open2(catalog: SortedMap<String, String>): Atomic.Long {
            val recid = catalog[name+Keys.recid]!!.toLong()
            return Atomic.Long(db.store, recid)
        }

        override fun create()= make2(true)
        override fun createOrOpen() = make2(null)
        override fun open() = make2(false)
    }

    fun atomicLong(name:String) = AtomicLongMaker(this, name)

    fun atomicLong(name:String, value:Long) = AtomicLongMaker(this, name, value)


    class AtomicBooleanMaker(db:DB, name:String, protected val value:Boolean=false):Maker<Atomic.Boolean>(db,name,"AtomicBoolean"){

        override fun awareItems(): Array<Any?> = arrayOf()

        override fun create2(catalog: SortedMap<String, String>): Atomic.Boolean {
            val recid = db.store.put(value, Serializer.BOOLEAN)
            catalog[name+Keys.recid] = recid.toString()
            return Atomic.Boolean(db.store, recid)
        }

        override fun open2(catalog: SortedMap<String, String>): Atomic.Boolean {
            val recid = catalog[name+Keys.recid]!!.toLong()
            return Atomic.Boolean(db.store, recid)
        }


        override fun create()= make2(true)
        override fun createOrOpen() = make2(null)
        override fun open() = make2(false)

    }

    fun atomicBoolean(name:String) = AtomicBooleanMaker(this, name)

    fun atomicBoolean(name:String, value:Boolean) = AtomicBooleanMaker(this, name, value)


    class AtomicStringMaker(db:DB, name:String, protected val value:String?=null):Maker<Atomic.String>(db,name,"AtomicString"){

        override fun awareItems(): Array<Any?> = arrayOf()

        override fun create2(catalog: SortedMap<String, String>): Atomic.String {
            val recid = db.store.put(value, Serializer.STRING_NOSIZE)
            catalog[name+Keys.recid] = recid.toString()
            return Atomic.String(db.store, recid)
        }

        override fun open2(catalog: SortedMap<String, String>): Atomic.String {
            val recid = catalog[name+Keys.recid]!!.toLong()
            return Atomic.String(db.store, recid)
        }


        override fun create()= make2(true)
        override fun createOrOpen() = make2(null)
        override fun open() = make2(false)
    }

    fun atomicString(name:String) = AtomicStringMaker(this, name)

    fun atomicString(name:String, value:String?) = AtomicStringMaker(this, name, value)


    class AtomicVarMaker<E>(db:DB,
                            name:String,
                            protected val serializer:Serializer<E> = db.defaultSerializer as Serializer<E>,
                            protected val value:E? = null):Maker<Atomic.Var<E>>(db,name, "AtomicVar"){

        override fun awareItems():Array<Any?> = arrayOf(serializer)

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

        override fun create() = make2(true)

        override fun createOrOpen() =  make2(null)

        override fun open() = make2(false)

    }

    fun atomicVar(name:String) = atomicVar(name, defaultSerializer)
    fun <E> atomicVar(name:String, serializer:Serializer<E> ) = AtomicVarMaker(this, name, serializer)
    fun <E> atomicVar(name:String, clazz:Class<E> ) = AtomicVarMaker(this, name, serializerForClass(clazz))
    fun <E> atomicVar(name:String, serializer:Serializer<E>, value:E? ) = AtomicVarMaker(this, name, serializer, value)

    fun <E> atomicVar(name:String, clazz:Class<E>, value:E? ) = AtomicVarMaker(this, name, serializerForClass(clazz), value)

    class IndexTreeLongLongMapMaker(db:DB,name:String
    ):Maker<IndexTreeLongLongMap>(db, name, "IndexTreeLongLongMap"){

        private var _dirShift = CC.HTREEMAP_DIR_SHIFT
        private var _levels = CC.HTREEMAP_LEVELS
        private var _removeCollapsesIndexTree:Boolean = true

        override fun awareItems(): Array<Any?> = arrayOf()

        fun layout(dirSize:Int, levels:Int):IndexTreeLongLongMapMaker{
            fun toShift(value:Int):Int{
                return 31 - Integer.numberOfLeadingZeros(DataIO.nextPowTwo(Math.max(1,value)))
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


        override fun create()= make2(true)
        override fun createOrOpen() = make2(null)
        override fun open() = make2(false)
    }

    //TODO this is thread unsafe, but locks should not be added directly due to code overhead on HTreeMap
    private fun indexTreeLongLongMap(name: String) = IndexTreeLongLongMapMaker(this, name)


    class IndexTreeListMaker<E>(db:DB, name:String,
            protected val serializer:Serializer<E>
    ):Maker<IndexTreeList<E>>(db, name, "IndexTreeList"){

        private var _dirShift = CC.HTREEMAP_DIR_SHIFT
        private var _levels = CC.HTREEMAP_LEVELS
        private var _removeCollapsesIndexTree:Boolean = true

        override fun awareItems(): Array<Any?> = arrayOf(serializer)

        fun layout(dirSize:Int, levels:Int):IndexTreeListMaker<E>{
            fun toShift(value:Int):Int{
                return 31 - Integer.numberOfLeadingZeros(DataIO.nextPowTwo(Math.max(1,value)))
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
                    isThreadSafe = db.isThreadSafe,
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
                    isThreadSafe = db.isThreadSafe,
                    counterRecid = catalog[name+Keys.counterRecid]!!.toLong()
            )
        }

        override fun create() = make2(true)

        override fun createOrOpen() =  make2(null)

        override fun open() = make2(false)

    }

    fun <E> indexTreeList(name: String, clazz:Class<E>) = IndexTreeListMaker<E>(this, name, serializerForClass(clazz))

    fun <E> indexTreeList(name: String, serializer:Serializer<E>) = IndexTreeListMaker<E>(this, name, serializer)
    fun indexTreeList(name: String) = IndexTreeListMaker<Any?>(this, name, defaultSerializer as Serializer<Any?>)


    override fun checkThreadSafe() {
        super.checkThreadSafe()
        if(store.isThreadSafe.not())
            throw AssertionError()
    }

    /**
     * Register Class with default POJO serializer. Class structure will be stored in store,
     * and will save space for collections which do not use specialized serializer.
     */
    fun defaultSerializerRegisterClass(clazz:Class<*>){
        Utils.lockWrite(lock) {
            checkNotClosed()
            defaultSerializerRegisterClass_noLock(clazz)
        }
    }
    private fun defaultSerializerRegisterClass_noLock(clazz:Class<*>) {
        if(CC.ASSERT)
            Utils.assertWriteLock(lock)
        var infos = loadClassInfos()
        val className = clazz.name
        if (infos.find { it.name == className } != null)
            return //class is already present
        //add as last item to an array
        infos = Arrays.copyOf(infos, infos.size + 1)
        infos[infos.size - 1] = ElsaSerializerPojo.makeClassInfo(clazz)
        //and save
        store2.update(CC.RECID_CLASS_INFOS, infos, classInfoSerializer)
    }

    protected data class CatVal(val msg:(String)->String?, val required:Boolean=true)

    private fun nameCatalogVerifyTree():Map<String, Map<String, CatVal>> {

        val all = {s:String->null}
        val recid = {s:String->
            try{
                val l = s.toLong()
                if(l<=0)
                    "Recid must be greater than 0"
                else
                    null
            }catch(e:Exception){
                "Recid must be a number"
            }
        }

        val recidOptional = {s:String->
            try{
                val l = s.toLong()
                if(l<0)
                    "Recid can not be negative"
                else
                    null
            }catch(e:Exception){
                "Recid must be a number"
            }
        }

        val long = { s: String ->
            try {
                s.toLong()
                null
            } catch(e: Exception) {
                "Must be a number"
            }
        }


        val int = { s: String ->
            try {
                s.toInt()
                null
            } catch(e: Exception) {
                "Must be a number"
            }
        }

        val recidArray = all

        val serializer = all
        val boolean = {s:String ->
            if(s!="true" && s!="false")
                "Not boolean"
            else
                null
        }

        return mapOf(
                Pair("HashMap", mapOf(
                        Pair(Keys.keySerializer, CatVal(serializer, required=false)),
                        Pair(Keys.valueSerializer,CatVal(serializer, required=false)),
                        Pair(Keys.rootRecids,CatVal(recidArray)),
                        Pair(Keys.valueInline, CatVal(boolean)),
                        Pair(Keys.hashSeed, CatVal(int)),
                        Pair(Keys.concShift, CatVal(int)),
                        Pair(Keys.levels, CatVal(int)),
                        Pair(Keys.dirShift, CatVal(int)),
                        Pair(Keys.removeCollapsesIndexTree, CatVal(boolean)),
                        Pair(Keys.counterRecids, CatVal(recidArray)),
                        Pair(Keys.expireCreateQueue, CatVal(all)),
                        Pair(Keys.expireUpdateQueue, CatVal(all)),
                        Pair(Keys.expireGetQueue, CatVal(all)),
                        Pair(Keys.expireCreateTTL, CatVal(long)),
                        Pair(Keys.expireUpdateTTL, CatVal(long)),
                        Pair(Keys.expireGetTTL, CatVal(long))
                )),
                Pair("HashSet", mapOf(
                        Pair(Keys.serializer, CatVal(serializer, required=false)),
                        Pair(Keys.rootRecids, CatVal(recidArray)),
                        Pair(Keys.hashSeed, CatVal(int)),
                        Pair(Keys.concShift, CatVal(int)),
                        Pair(Keys.dirShift, CatVal(int)),
                        Pair(Keys.levels, CatVal(int)),
                        Pair(Keys.removeCollapsesIndexTree, CatVal(boolean)),
                        Pair(Keys.counterRecids, CatVal(recidArray)),
                        Pair(Keys.expireCreateQueue, CatVal(all)),
                        Pair(Keys.expireGetQueue, CatVal(all)),
                        Pair(Keys.expireCreateTTL, CatVal(long)),
                        Pair(Keys.expireGetTTL, CatVal(long))
                )),
                Pair("TreeMap", mapOf(
                        Pair(Keys.keySerializer, CatVal(serializer, required=false)),
                        Pair(Keys.valueSerializer, CatVal(serializer, required=false)),
                        Pair(Keys.rootRecidRecid, CatVal(recid)),
                        Pair(Keys.counterRecid, CatVal(recidOptional)),
                        Pair(Keys.maxNodeSize, CatVal(int)),
                        Pair(Keys.valueInline, CatVal(boolean))
                )),
                Pair("TreeSet", mapOf(
                        Pair(Keys.serializer, CatVal(serializer, required=false)),
                        Pair(Keys.rootRecidRecid, CatVal(recid)),
                        Pair(Keys.counterRecid, CatVal(recidOptional)),
                        Pair(Keys.maxNodeSize, CatVal(int))
                )),
                Pair("AtomicBoolean", mapOf(
                        Pair(Keys.recid, CatVal(recid))
                )),
                Pair("AtomicInteger", mapOf(
                        Pair(Keys.recid, CatVal(recid))
                )),
                Pair("AtomicVar", mapOf(
                        Pair(Keys.recid, CatVal(recid)),
                        Pair(Keys.serializer, CatVal(serializer, false))
                )),
                Pair("AtomicString", mapOf(
                        Pair(Keys.recid, CatVal(recid))
                )),
                Pair("AtomicLong", mapOf(
                        Pair(Keys.recid, CatVal(recid))
                )),
                Pair("IndexTreeList", mapOf(
                        Pair(Keys.serializer, CatVal(serializer, required=false)),
                        Pair(Keys.dirShift, CatVal(int)),
                        Pair(Keys.levels, CatVal(int)),
                        Pair(Keys.removeCollapsesIndexTree, CatVal(boolean)),
                        Pair(Keys.counterRecid, CatVal(recid)),
                        Pair(Keys.rootRecid, CatVal(recid))
                )),
                Pair("IndexTreeLongLongMap", mapOf(
                        Pair(Keys.dirShift, CatVal(int)),
                        Pair(Keys.levels, CatVal(int)),
                        Pair(Keys.removeCollapsesIndexTree, CatVal(boolean)),
                        Pair(Keys.rootRecid, CatVal(recid))
                ))
        )
    }

    /** verifies name catalog is valid (all parameters are known and have required values). If there are problems, it return list of messages */
    fun nameCatalogVerifyGetMessages():Iterable<String>{
        val ret = ArrayList<String>()

        val ver = nameCatalogVerifyTree()
        val catalog = nameCatalogLoad()
        val names = catalog.keys.filter{it.endsWith(Keys.type)}.map{it.substring(0, it.lastIndexOf('#'))}.toSet()

        val known = HashSet<String>()

        //iterate over names, check all required parameters are present
        nameLoop@ for(name in names){

            //get type
            known+=name+Keys.type
            val type = catalog[name+Keys.type]
            val reqParams = ver[type]
            if(reqParams==null){
                ret+=name+Keys.type+": unknown type '$type'"
                continue@nameLoop
            }
            paramLoop@ for((param, catVal) in reqParams){
                known+=name+param
                val value = catalog[name+param]
                if(value==null) {
                    if(catVal.required)
                        ret += name + param+": required parameter not found"
                    continue@paramLoop
                }
                val msg = catVal.msg(value) //validate value, get msg if not valid
                if(msg!=null)
                    ret+=name+param+": "+msg
            }
        }

        //check for extra params which are not known
        for(param in catalog.keys)
            if(known.contains(param).not())
                ret+=param+": unknown parameter"

        return ret;
    }


    /**
     * Callback interface which gets reference to DB object. Classes which implements it (for example serializers)
     * can get reference to `DB` object
     */
    interface DBAware{
        fun callbackDB(db:DB)
    }


    /**
     * Callback interface which gets reference to collection(record) and its name. Classes which implements it (for example serializers)
     * can get reference to collection they were created with
     */
    interface NamedRecordAware {
        fun callbackRecord(name: String, collection: Any)
    }

    fun compact(){
        store.compact()
    }


}