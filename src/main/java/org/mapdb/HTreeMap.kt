package org.mapdb

import com.google.common.collect.Iterators
import org.eclipse.collections.api.map.primitive.MutableLongLongMap
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet
import java.io.Closeable
import java.security.SecureRandom

import java.util.*
import java.util.concurrent.*
import java.util.concurrent.locks.ReadWriteLock
import java.util.function.BiConsumer

/**
 * Concurrent HashMap which uses IndexTree for hash table
 */
//TODO there are many casts, catch ClassCastException and return false/null
class HTreeMap<K,V>(
        override val keySerializer:Serializer<K>,
        override val valueSerializer:Serializer<V>,
        val valueInline:Boolean,
        val concShift: Int,
        val dirShift: Int,
        val levels: Int,
        val stores: Array<Store>,
        val indexTrees: Array<MutableLongLongMap>,
        private val hashSeed:Int,
        val counterRecids:LongArray?,
        val expireCreateTTL:Long,
        val expireUpdateTTL:Long,
        val expireGetTTL:Long,
        val expireMaxSize:Long,
        val expireStoreSize:Long,
        val expireCreateQueues:Array<QueueLong>?,
        val expireUpdateQueues:Array<QueueLong>?,
        val expireGetQueues:Array<QueueLong>?,
        val expireExecutor: ScheduledExecutorService?,
        val expireExecutorPeriod:Long,
        val expireCompactThreshold:Double?,
        val threadSafe:Boolean,
        val valueLoader:((key:K)->V?)?,
        private val modificationListeners: Array<MapModificationListener<K,V>>?,
        private val closeable:Closeable?,
        val hasValues:Boolean = true

        //TODO queue is probably sequentially unsafe

) : ConcurrentMap<K,V>, MapExtra<K,V>, Verifiable, Closeable{


    companion object{
        /** constructor with default values */
        fun <K,V> make(
                keySerializer:Serializer<K> = Serializer.JAVA as Serializer<K>,
                valueSerializer:Serializer<V> = Serializer.JAVA as Serializer<V>,
                valueInline:Boolean = false,
                concShift: Int = CC.HTREEMAP_CONC_SHIFT,
                dirShift: Int = CC.HTREEMAP_DIR_SHIFT,
                levels:Int = CC.HTREEMAP_LEVELS,
                stores:Array<Store> = Array(1.shl(concShift), {StoreTrivial()}),
                indexTrees: Array<MutableLongLongMap> = Array(1.shl(concShift), { i->IndexTreeLongLongMap.make(stores[i], levels=levels, dirShift = dirShift)}),
                hashSeed:Int = SecureRandom().nextInt(),
                counterRecids:LongArray? = null,
                expireCreateTTL:Long = 0L,
                expireUpdateTTL:Long = 0L,
                expireGetTTL:Long = 0L,
                expireMaxSize:Long = 0L,
                expireStoreSize:Long = 0L,
                expireCreateQueues:Array<QueueLong>? = if(expireCreateTTL<=0L) null else Array(stores.size, { i->QueueLong.make(store = stores[i])}),
                expireUpdateQueues:Array<QueueLong>? = if(expireUpdateTTL<=0L) null else Array(stores.size, { i->QueueLong.make(store = stores[i])}),
                expireGetQueues:Array<QueueLong>? = if(expireGetTTL<=0L) null else Array(stores.size, { i->QueueLong.make(store = stores[i])}),
                expireExecutor:ScheduledExecutorService? = null,
                expireExecutorPeriod:Long = 0,
                expireCompactThreshold:Double? = null,
                threadSafe:Boolean = true,
                valueLoader:((key:K)->V)? = null,
                modificationListeners: Array<MapModificationListener<K,V>>? = null,
                closeable: Closeable? = null
        ) = HTreeMap(
                keySerializer = keySerializer,
                valueSerializer = valueSerializer,
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
                expireCompactThreshold = expireCompactThreshold,
                threadSafe = threadSafe,
                valueLoader = valueLoader,
                modificationListeners = modificationListeners,
                closeable = closeable
            )

        @JvmField internal val QUEUE_CREATE=1L
        @JvmField internal val QUEUE_UPDATE=2L
        @JvmField internal val QUEUE_GET=3L
    }

    private val segmentCount = 1.shl(concShift)

    private val storesUniqueCount = Utils.identityCount(stores)

    internal val locks:Array<ReadWriteLock?> = Array(segmentCount, {Utils.newReadWriteLock(threadSafe)})

    /** true if Eviction is executed inside user thread, as part of get/put etc operations */
    internal val expireEvict:Boolean = expireExecutor==null &&
            (expireCreateQueues!=null || expireUpdateQueues!=null || expireGetQueues!=null)

    init{
        if(segmentCount!=stores.size)
            throw IllegalArgumentException("stores size wrong")
        if(segmentCount!=indexTrees.size)
            throw IllegalArgumentException("indexTrees size wrong")
        if(expireCreateQueues!=null && segmentCount!=expireCreateQueues.size)
            throw IllegalArgumentException("expireCreateQueues size wrong")
        if(expireUpdateQueues!=null && segmentCount!=expireUpdateQueues.size)
            throw IllegalArgumentException("expireUpdateQueues size wrong")
        if(expireGetQueues!=null && segmentCount!=expireGetQueues.size)
            throw IllegalArgumentException("expireGetQueues size wrong")

        //schedule background expiration if needed
        if(expireExecutor!=null && (expireCreateQueues!=null || expireUpdateQueues!=null || expireGetQueues!=null)){
            for(segment in 0 until segmentCount){
                expireExecutor.scheduleAtFixedRate({
                    segmentWrite(segment){
                        expireEvictSegment(segment)
                    }
                },
                (expireExecutorPeriod * Math.random()).toLong(), // put random delay, so eviction are not executed all at once
                expireExecutorPeriod, TimeUnit.MILLISECONDS)
            }
        }

        //check if 32bit hash covers all indexes. In future we will upgrade to 64bit hash and this can be removed
        if(segmentCount*Math.pow(1.shl(dirShift).toDouble(),levels.toDouble()) > 2L*Integer.MAX_VALUE){
            Utils.LOG.warning { "Wrong layout, segment+index is more than 32bits, performance degradation" }
        }
    }


    private fun leafValueInlineSerializer() = object: Serializer<Array<Any>>{
        override fun serialize(out: DataOutput2, value: kotlin.Array<Any>) {
            out.packInt(value.size)
            for(i in 0 until value.size step 3) {
                keySerializer.serialize(out, value[i+0] as K)
                valueSerializer.serialize(out, value[i+1] as V)
                out.packLong(value[i+2] as Long)
            }
        }

        override fun deserialize(input: DataInput2, available: Int): kotlin.Array<Any> {
            val ret:Array<Any?> = arrayOfNulls(input.unpackInt())
            var i = 0;
            while(i<ret.size) {
                ret[i++] = keySerializer.deserialize(input, -1)
                ret[i++] = valueSerializer.deserialize(input, -1)
                ret[i++] = input.unpackLong()
            }
            return ret as Array<Any>;
        }

        override fun isTrusted(): Boolean {
            return keySerializer.isTrusted && valueSerializer.isTrusted
        }
    }


    private fun leafValueExternalSerializer() = object: Serializer<Array<Any>>{
        override fun serialize(out: DataOutput2, value: Array<Any>) {
            out.packInt(value.size)
            for(i in 0 until value.size step 3) {
                keySerializer.serialize(out, value[i+0] as K)
                out.packLong(value[i+1] as Long)
                out.packLong(value[i+2] as Long)
            }
        }

        override fun deserialize(input: DataInput2, available: Int): Array<Any> {
            val ret:Array<Any?> = arrayOfNulls(input.unpackInt())
            var i = 0;
            while(i<ret.size) {
                ret[i++] = keySerializer.deserialize(input, -1)
                ret[i++] = input.unpackLong()
                ret[i++] = input.unpackLong() //expiration timestamp
            }
            return ret as Array<Any>;
        }

        override fun isTrusted(): Boolean {
            return keySerializer.isTrusted
        }
    }



    //TODO Expiration QueueID is part of leaf, remove it if expiration is disabled!
    internal val leafSerializer:Serializer<Array<Any>> =
            if(valueInline)
                leafValueInlineSerializer()
            else
                leafValueExternalSerializer()


    private val indexMask = (IndexTreeListJava.full.shl(levels*dirShift)).inv();
    private val concMask = IndexTreeListJava.full.shl(concShift).inv().toInt();

    /**
     * Variable used to check for first put() call, it verifies that hashCode of inserted key is stable.
     * Note: this variable is not thread safe, but that is fine, worst case scenario is check will be performed multiple times.
     *
     * This step is ignored for StoreOnHeap, because serialization is not involved here, and it might failnew
     */
    private var checkHashAfterSerialization = stores.find { it is StoreOnHeap } == null


    internal fun hash(key:K):Int{
        return keySerializer.hashCode(key, 0)
    }
    internal fun hashToIndex(hash:Int) = DataIO.intToLong(hash) and indexMask
    internal fun hashToSegment(hash:Int) = hash.ushr(levels*dirShift) and concMask


    private inline fun <E> segmentWrite(segment:Int, body:()->E):E{
        val lock = locks[segment]?.writeLock()
        lock?.lock()
        try {
            return body()
        }finally{
            lock?.unlock()
        }
    }

    private inline fun <E> segmentRead(segment:Int, body:()->E):E{
        val lock =  // if expireGetQueue is modified on get, we need write lock
                if(expireGetQueues==null && valueLoader ==null) locks[segment]?.readLock()
                else locks[segment]?.writeLock()
        lock?.lock()
        try {
            return body()
        }finally{
            lock?.unlock()
        }
    }


    private fun counter(segment:Int, ammount:Int){
        if(counterRecids==null)
            return
        if(CC.ASSERT)
            Utils.assertWriteLock(locks[segment])
        val recid = counterRecids[segment]
        val count = stores[segment].get(recid, Serializer.LONG_PACKED)
            ?: throw DBException.DataCorruption("counter not found")
        stores[segment].update(recid, count+ammount, Serializer.LONG_PACKED)
    }

    override fun put(key: K?, value: V?): V? {
        if (key == null || value == null)
            throw NullPointerException()

        val hash = hash(key)
        if(checkHashAfterSerialization){
            checkHashAfterSerialization = false;
            //check if hash is the same after cloning
            val key2 = Utils.clone(key, keySerializer)
            if(hash(key2)!=hash){
                throw IllegalArgumentException("Key.hashCode() changed after serialization, make sure to use correct Key Serializer")
            }
        }

        val segment = hashToSegment(hash)
        segmentWrite(segment) {->
            if(expireEvict)
                expireEvictSegment(segment)

            return putInternal(hash, key, value,false)
        }
    }

    internal fun putInternal(hash:Int, key:K, value:V, triggered:Boolean):V?{
        val segment = hashToSegment(hash)
        if(CC.ASSERT)
            Utils.assertWriteLock(locks[segment])
        if(CC.PARANOID && hash!= hash(key))
            throw AssertionError()


        val index = hashToIndex(hash)
        val store = stores[segment]
        val indexTree = indexTrees[segment]

        val leafRecid = indexTree.get(index)

        if (leafRecid == 0L) {
            //not found, insert new record
            val wrappedValue = valueWrap(segment, value)

            val leafRecid2 =
                    if (expireCreateQueues == null) {
                        // no expiration, so just insert
                        val leaf = arrayOf(key as Any, wrappedValue, 0L)
                        store.put(leaf, leafSerializer)
                    } else {
                        // expiration is involved, and there is cyclic dependency between expireRecid and leafRecid
                        // must use preallocation and update to solve it
                        val leafRecid2 = store.preallocate()
                        val expireRecid = expireCreateQueues[segment].put(
                                if(expireCreateTTL==-1L) 0L else System.currentTimeMillis()+expireCreateTTL,
                                leafRecid2)
                        val leaf = arrayOf(key as Any, wrappedValue, expireId(expireRecid, QUEUE_CREATE))
                        store.update(leafRecid2, leaf, leafSerializer)
                        leafRecid2
                    }
            counter(segment,+1)
            indexTree.put(index, leafRecid2)

            listenerNotify(key, null, value, triggered)
            return null
        }


        var leaf = store.get(leafRecid, leafSerializer)
                ?: throw DBException.DataCorruption("linked leaf not found")

        //check existing keys in leaf
        for (i in 0 until leaf.size step 3) {
            val oldKey = leaf[i] as K

            if (keySerializer.equals(oldKey, key)) {
                //match found, update existing value
                val oldVal = valueUnwrap(segment, leaf[i + 1])

                if (expireUpdateQueues != null) {
                    //update expiration stuff
                    if (leaf[i + 2] != 0L) {
                        //it exist in old queue
                        val expireId = leaf[i + 2] as Long
                        val oldQueue = expireQueueFor(segment,expireId)
                        val nodeRecid = expireNodeRecidFor(expireId)
                        if (oldQueue === expireUpdateQueues[segment]) {
                            //just bump
                            oldQueue.bump(nodeRecid, if(expireUpdateTTL==-1L) 0L else System.currentTimeMillis()+expireUpdateTTL)
                        } else {
                            //remove from old queue
                            val oldNode = oldQueue.remove(nodeRecid, removeNode = false)

                            //and put into new queue, reuse recid
                            expireUpdateQueues[segment].put(
                                    timestamp = if(expireUpdateTTL==-1L) 0L else System.currentTimeMillis()+expireUpdateTTL,
                                    value=oldNode.value, nodeRecid = nodeRecid )

                            leaf = leaf.clone()
                            leaf[i + 2] = expireId(nodeRecid, QUEUE_UPDATE)
                            store.update(leafRecid, leaf, leafSerializer)
                        }
                    } else {
                        //does not exist in old queue, insert new
                        val expireRecid = expireUpdateQueues[segment].put(
                                if(expireUpdateTTL==-1L) 0L else System.currentTimeMillis()+expireUpdateTTL,
                                leafRecid);
                        leaf = leaf.clone()
                        leaf[i + 2] = expireId(expireRecid, QUEUE_UPDATE)
                        store.update(leafRecid, leaf, leafSerializer)
                    }
                }

                if(!valueInline) {
                    //update external record
                    store.update(leaf[i+1] as Long, value, valueSerializer)
                }else{
                    //stored inside leaf, so clone leaf, swap and update
                    leaf = leaf.clone();
                    leaf[i+1] = value as Any;
                    store.update(leafRecid, leaf, leafSerializer)
                }
                listenerNotify(key, oldVal, value, triggered)
                return oldVal
            }
        }

        //no key in leaf matches ours, so insert new key and update leaf
        val wrappedValue = valueWrap(segment, value)

        leaf = Arrays.copyOf(leaf, leaf.size + 3)
        leaf[leaf.size-3] = key as Any
        leaf[leaf.size-2] = wrappedValue
        leaf[leaf.size-1] = 0L

        if (expireCreateQueues != null) {
            val expireRecid = expireCreateQueues[segment].put(
                    if(expireCreateTTL==-1L) 0L else System.currentTimeMillis()+expireCreateTTL,
                    leafRecid);
            leaf[leaf.size-1] = expireId(expireRecid, QUEUE_CREATE)
        }

        store.update(leafRecid, leaf, leafSerializer)
        counter(segment,+1)
        listenerNotify(key, null, value, triggered)
        return null

    }

    override fun putAll(from: Map<out K?, V?>) {
        for(e in from.entries){
            put(e.key, e.value)
        }
    }

    override fun remove(key: K?): V? {
        if(key == null)
            throw NullPointerException()
        val hash = hash(key)
        val segment = hashToSegment(hash)
        segmentWrite(segment) {->
            if(expireEvict)
                expireEvictSegment(segment)

            return removeInternal(hash, key, false)
        }
    }

    internal fun removeInternal(hash:Int, key: K, evicted:Boolean): V? {
        val segment = hashToSegment(hash)
        if(CC.ASSERT)
            Utils.assertWriteLock(locks[segment])
        if(CC.PARANOID && hash!= hash(key))
            throw AssertionError()

        val index = hashToIndex(hash)
        val store = stores[segment]
        val indexTree = indexTrees[segment]

        val leafRecid = indexTree.get(index)
        if (leafRecid == 0L)
            return null

        val leaf = store.get(leafRecid, leafSerializer)
                ?: throw DBException.DataCorruption("linked leaf not found")

        //check existing keys in leaf
        for (i in 0 until leaf.size step 3) {
            val oldKey = leaf[i] as K

            if (keySerializer.equals(oldKey, key)) {
                if (!evicted && leaf[i + 2] != 0L) {
                    //if entry is evicted, queue will be updated at other place, so no need to remove queue in that case
                    val queue = expireQueueFor(segment, leaf[i + 2] as Long)
                    queue.remove(expireNodeRecidFor(leaf[i + 2] as Long), removeNode = true)
                }

                val oldVal = valueUnwrap(segment, leaf[i + 1])

                //remove from leaf and from store
                if (leaf.size == 3) {
                    //single entry, collapse leaf
                    indexTree.remove(index)
                    store.delete(leafRecid, leafSerializer)
                } else {
                    //more entries, update leaf
                    store.update(leafRecid,
                            DataIO.arrayDelete(leaf, i + 3, 3),
                            leafSerializer)
                }

                if(!valueInline)
                    store.delete(leaf[i+1] as Long, valueSerializer)
                counter(segment,-1)
                listenerNotify(key, oldVal, null, evicted)
                return oldVal
            }
        }

        //nothing to delete
        return null;
    }

    override fun clear() {
        clear2(notifyListeners=true)
    }

    fun clear2(notifyListeners:Boolean=true) {
        //TODO not sequentially safe
        val notify = notifyListeners &&  modificationListeners!=null && modificationListeners.isEmpty().not()
        for(segment in 0 until segmentCount) {
            Utils.lockWrite(locks[segment]) {
                val indexTree = indexTrees[segment]
                val store = stores[segment]
                indexTree.forEachKeyValue { index, leafRecid ->
                    val leaf = store.get(leafRecid, leafSerializer)
                            ?: throw DBException.DataCorruption("linked leaf not found")
                    store.delete(leafRecid, leafSerializer);
                    for (i in 0 until leaf.size step 3) {
                        val key = leaf[i]
                        val wrappedValue = leaf[i + 1]
                        if (notify)
                            listenerNotify(key as K, valueUnwrap(segment, wrappedValue), null, false)
                        if (!valueInline)
                            store.delete(wrappedValue as Long, valueSerializer)
                    }
                }
                expireCreateQueues?.get(segment)?.clear()
                expireUpdateQueues?.get(segment)?.clear()
                expireGetQueues?.get(segment)?.clear()
                indexTree.clear()

                if(counterRecids!=null)
                    store.update(counterRecids[segment],0L, Serializer.LONG_PACKED)
            }
        }
    }


    override fun containsKey(key: K?): Boolean {
        if (key == null)
            throw NullPointerException()

        val hash = hash(key)

        segmentRead(hashToSegment(hash)) { ->
            return null!=getInternal(hash, key, updateQueue = false)
        }
    }

    override fun containsValue(value: V?): Boolean {
        if(value==null)
            throw NullPointerException();
        return values.contains(value)
    }

    override fun get(key: K?): V? {
        if (key == null)
            throw NullPointerException()

        val hash = hash(key)
        val segment = hashToSegment(hash)
        segmentRead(segment) { ->
            if(expireEvict && expireGetQueues!=null)
                expireEvictSegment(segment)
            var ret =  getInternal(hash, key, updateQueue = true)
            if(ret==null && valueLoader !=null){
                ret = valueLoader!!(key)
                if(ret!=null)
                    putInternal(hash, key, ret, true)
            }
            return ret
        }
    }

    internal fun getInternal(hash:Int, key:K, updateQueue:Boolean):V?{
        val segment = hashToSegment(hash)
        if(CC.ASSERT) {
            if(updateQueue && expireGetQueues!=null)
                Utils.assertWriteLock(locks[segment])
            else
                Utils.assertReadLock(locks[segment])
        }
        if(CC.PARANOID && hash!= hash(key))
            throw AssertionError()


        val index = hashToIndex(hash)
        val store = stores[segment]
        val indexTree = indexTrees[segment]

        val leafRecid = indexTree.get(index)
        if (leafRecid == 0L)
            return null

        var leaf = store.get(leafRecid, leafSerializer)
                ?: throw DBException.DataCorruption("leaf not found");

        for (i in 0 until leaf.size step 3) {
            val oldKey = leaf[i] as K

            if (keySerializer.equals(oldKey, key)) {

                if (expireGetQueues != null) {
                    leaf = getInternalQueues(expireGetQueues, i, leaf, leafRecid, segment, store)
                }

                return valueUnwrap(segment, leaf[i + 1])
            }
        }
        //nothing found
        return null
    }

    private fun getInternalQueues(expireGetQueues: Array<QueueLong>, i: Int, leaf: Array<Any>, leafRecid: Long, segment: Int, store: Store): Array<Any> {
        if(CC.ASSERT)
            Utils.assertWriteLock(locks[segment])

        //update expiration stuff
        var leaf1 = leaf
        if (leaf1[i + 2] != 0L) {
            //it exist in old queue
            val expireId = leaf1[i + 2] as Long
            val oldQueue = expireQueueFor(segment, expireId)
            val nodeRecid = expireNodeRecidFor(expireId)
            if (oldQueue === expireGetQueues[segment]) {
                //just bump
                oldQueue.bump(nodeRecid, if(expireGetTTL==-1L) 0L else System.currentTimeMillis()+expireGetTTL)
            } else {
                //remove from old queue
                val oldNode = oldQueue.remove(nodeRecid, removeNode = false)
                //and put into new queue, reuse recid
                expireGetQueues[segment].put(
                        timestamp = if(expireGetTTL==-1L) 0L else System.currentTimeMillis()+expireGetTTL,
                        value = oldNode.value, nodeRecid = nodeRecid)
                //update queue id
                leaf1 = leaf1.clone()
                leaf1[i + 2] = expireId(nodeRecid, QUEUE_GET)
                store.update(leafRecid, leaf1, leafSerializer)
            }
        } else {
            //does not exist in old queue, insert new
            val expireRecid = expireGetQueues[segment].put(
                    if(expireGetTTL==-1L) 0L else System.currentTimeMillis()+expireGetTTL,
                    leafRecid);
            leaf1 = leaf1.clone()
            leaf1[i + 2] = expireId(expireRecid, QUEUE_GET)
            store.update(leafRecid, leaf1, leafSerializer)

        }
        return leaf1
    }

    override fun isEmpty(): Boolean {
        for(segment in 0 until segmentCount) {
            Utils.lockRead(locks[segment]) {
                if (!indexTrees[segment].isEmpty)
                    return false
            }
        }
        return true;
    }

    override val size: Int
        get() = Utils.roundDownToIntMAXVAL(sizeLong())

    override fun sizeLong():Long{
        var ret = 0L
        for(segment in 0 until segmentCount) {

            Utils.lockRead(locks[segment]){
                if(counterRecids!=null){
                    ret += stores[segment].get(counterRecids[segment], Serializer.LONG_PACKED)
                            ?: throw DBException.DataCorruption("counter not found")
                }else {
                    indexTrees[segment].forEachKeyValue { index, leafRecid ->
                        val leaf = stores[segment].get(leafRecid, leafSerializer)
                                ?: throw DBException.DataCorruption("Leaf not found")
                        ret += leaf.size / 3
                    }
                }
            }
        }

        return ret;
    }

    override fun putIfAbsent(key: K?, value: V?): V? {
        if(key == null || value==null)
            throw NullPointerException()

        val hash = hash(key)
        val segment = hashToSegment(hash)
        segmentWrite(segment) {
            if(expireEvict)
                expireEvictSegment(segment)

            return getInternal(hash,key, updateQueue = false) ?:
                    putInternal(hash, key, value,false)
        }
    }


    override fun putIfAbsentBoolean(key: K?, value: V?): Boolean {
        if(key == null || value==null)
            throw NullPointerException()

        val hash = hash(key)
        val segment = hashToSegment(hash)
        segmentWrite(segment) {
            if(expireEvict)
                expireEvictSegment(segment)

            if (getInternal(hash, key, updateQueue = false) != null)
                return false
            putInternal(hash, key, value, false)
            return true;
        }
    }

    override fun remove(key: Any?, value: Any?): Boolean {
        if(key == null || value==null)
            throw NullPointerException()

        val hash = hash(key as K)
        val segment = hashToSegment(hash)
        segmentWrite(segment) {
            if(expireEvict)
                expireEvictSegment(segment)

            val oldValue = getInternal(hash, key, updateQueue = false)
            if (oldValue != null && valueSerializer.equals(oldValue, value as V)) {
                removeInternal(hash, key, evicted = false)
                return true;
            } else {
                return false;
            }
        }
    }

    override fun replace(key: K?, oldValue: V?, newValue: V?): Boolean {
        if(key == null || oldValue==null || newValue==null)
            throw NullPointerException()
        val hash = hash(key)
        val segment = hashToSegment(hash)
        segmentWrite(segment) {
            if(expireEvict)
                expireEvictSegment(segment)

            val valueIn = getInternal(hash, key, updateQueue = false);
            if (valueIn != null && valueSerializer.equals(valueIn, oldValue)) {
                putInternal(hash, key, newValue,false);
                return true;
            } else {
                return false;
            }
        }
    }

    override fun replace(key: K?, value: V?): V? {
        if(key == null || value==null)
            throw NullPointerException()

        val hash = hash(key)
        val segment = hashToSegment(hash)
        segmentWrite(segment) {
            if(expireEvict)
                expireEvictSegment(segment)

            if (getInternal(hash, key,updateQueue = false)!=null) {
                return putInternal(hash, key, value, false);
            } else {
                return null;
            }
        }
    }



    internal fun expireNodeRecidFor(expireId: Long): Long {
        return expireId.ushr(2)
    }

    internal fun expireQueueFor(segment:Int, expireId: Long): QueueLong {
        return when(expireId and 3){
            1L -> expireCreateQueues?.get(segment)
            2L -> expireUpdateQueues?.get(segment)
            3L -> expireGetQueues?.get(segment)
            else -> throw DBException.DataCorruption("wrong queue")
        } ?: throw IllegalAccessError("no queue is set")

    }

    internal fun expireId(nodeRecid: Long, queue:Long):Long{
        if(CC.ASSERT && queue !in 1L..3L)
            throw AssertionError("Wrong queue id: "+queue)
        if(CC.ASSERT && nodeRecid==0L)
            throw AssertionError("zero nodeRecid")
        return nodeRecid.shl(2) + queue
    }

    /** releases old stuff from queue */
    fun expireEvict(){
        for(segment in 0 until segmentCount) {
            segmentWrite(segment){
                expireEvictSegment(segment)
            }
        }
    }

    internal fun expireEvictSegment(segment:Int){
        if(CC.ASSERT)
            Utils.assertWriteLock(locks[segment])

        val currTimestamp = System.currentTimeMillis()
        var numberToTake:Long =
                if(expireMaxSize==0L) 0L
                else{
                    val segmentSize = stores[segment].get(counterRecids!![segment], Serializer.LONG_PACKED)
                        ?: throw DBException.DataCorruption("Counter not found")
                    Math.max(0L, (segmentSize*segmentCount-expireMaxSize)/segmentCount)
                }
        for (q in arrayOf(expireGetQueues?.get(segment), expireUpdateQueues?.get(segment), expireCreateQueues?.get(segment))) {
            q?.takeUntil(QueueLongTakeUntil { nodeRecid, node ->
                var purged = false;

                //expiration based on maximal Map size
                if(numberToTake>0){
                    numberToTake--
                    purged = true
                }

                //expiration based on TTL
                if(!purged && node.timestamp!=0L && node.timestamp < currTimestamp){
                    purged = true
                }

                //expiration based on maximal store size
                if(!purged && expireStoreSize!=0L){
                    val store = stores[segment] as StoreDirect
                    purged = store.fileTail - store.getFreeSize() > expireStoreSize
                }

                if(purged) {
                    //remove entry from Map
                    expireEvictEntry(segment = segment, leafRecid = node.value, nodeRecid = nodeRecid)
                }
                purged
            })
        }

        //trigger compaction?
        if(expireCompactThreshold!=null){
            val store = stores[segment]
            if(store is StoreDirect){
                val totalSize = store.getTotalSize().toDouble()
                if(store.getFreeSize().toDouble()/totalSize > expireCompactThreshold) {
                    store.compact()
                }
            }
        }
    }

    internal fun expireEvictEntry(segment:Int, leafRecid:Long, nodeRecid:Long){
        if(CC.ASSERT)
            Utils.assertWriteLock(locks[segment])

        val leaf = stores[segment].get(leafRecid, leafSerializer)
                ?: throw DBException.DataCorruption("linked leaf not found")

        for(leafIndex in 0 until leaf.size step 3){
            if(nodeRecid != expireNodeRecidFor(leaf[leafIndex+2] as Long))
                continue
            //remove from this leaf
            val key = leaf[leafIndex] as K
            val hash = hash(key);
            if(CC.ASSERT && segment!=hashToSegment(hash))
                throw AssertionError()
            val old = removeInternal(hash = hash, key = key, evicted = true)
            //TODO PERF if leaf has two or more items, delete directly from leaf
            if(CC.ASSERT && old==null)
                throw AssertionError()
            return;
        }

        throw DBException.DataCorruption("nodeRecid not found in this leaf")
    }


    //TODO retailAll etc should use serializers for comparasions, remove AbstractSet and AbstractCollection completely
    //TODO PERF replace iterator with forEach, much faster indexTree traversal
    override val entries: MutableSet<MutableMap.MutableEntry<K?, V?>> = object : AbstractSet<MutableMap.MutableEntry<K?, V?>>() {

        override fun add(element: MutableMap.MutableEntry<K?, V?>): Boolean {
            this@HTreeMap.put(element.key, element.value)
            return true
        }


        override fun clear() {
            this@HTreeMap.clear()
        }

        override fun iterator(): MutableIterator<MutableMap.MutableEntry<K?, V?>> {
            val iters = (0 until segmentCount).map{segment->
                htreeIterator(segment) { key, wrappedValue ->
                    htreeEntry(key as K, valueUnwrap(segment, wrappedValue))
                }
            }
            return Iterators.concat(iters.iterator())
        }

        override fun remove(element: MutableMap.MutableEntry<K?, V?>): Boolean {
            return this@HTreeMap.remove(element.key as Any?, element.value)
        }


        override fun contains(element: MutableMap.MutableEntry<K?, V?>): Boolean {
            val v = this@HTreeMap.get(element.key)
                    ?: return false
            val value = element.value
                    ?: return false
            return valueSerializer.equals(value,v)
        }

        override fun isEmpty(): Boolean {
            return this@HTreeMap.isEmpty()
        }

        override val size: Int
            get() = this@HTreeMap.size

    }

    class KeySet<K>(val map:HTreeMap<K,Any?>): AbstractSet<K>(){

        override fun iterator(): MutableIterator<K?> {
            val iters = (0 until map.segmentCount).map{segment->
                map.htreeIterator(segment) {key, wrappedValue ->
                    key as K
                }
            }
            return Iterators.concat(iters.iterator())
        }

        override val size: Int
        get() = map.size


        override fun add(element: K): Boolean {
            if(map.hasValues)
                throw UnsupportedOperationException("Can not add without val")
            return map.put(element, true as Any?)!=null //TODO default val for hashsets
        }

        override fun clear() {
            map.clear()
        }

        override fun isEmpty(): Boolean {
            return map.isEmpty()
        }

        override fun remove(element: K): Boolean {
            return map.remove(element)!=null
        }
    }

    override val keys: KeySet<K> = KeySet(this as HTreeMap<K,Any?>)



    override val values: MutableCollection<V?> = object : AbstractCollection<V>(){

        override fun clear() {
            this@HTreeMap.clear()
        }

        override fun isEmpty(): Boolean {
            return this@HTreeMap.isEmpty()
        }

        override val size: Int
            get() = this@HTreeMap.size


        override fun iterator(): MutableIterator<V?> {
            val iters = (0 until segmentCount).map{segment->
                htreeIterator(segment) {keyWrapped, valueWrapped ->
                    valueUnwrap(segment, valueWrapped)
                }
            }
            return Iterators.concat(iters.iterator())
        }

    }


    protected fun <E> htreeIterator(segment:Int,  loadNext:(wrappedKey:Any, wrappedValue:Any)->E ):MutableIterator<E>{
        return object : MutableIterator<E>{

            //TODO locking

            val store = stores[segment];

            val leafRecidIter = indexTrees[segment].values().longIterator()
            var leafPos = 0

            //TODO load lazily
            var leafArray:Array<Any?>? = moveToNextLeaf();

            var lastKey:K? = null;

            private fun moveToNextLeaf(): Array<Any?>? {
                Utils.lockRead(locks[segment]) {
                    if (!leafRecidIter.hasNext()) {
                        return null
                    }
                    val leafRecid = leafRecidIter.next()
                    val leaf = store.get(leafRecid, leafSerializer)
                            ?: throw DBException.DataCorruption("linked leaf not found")
                    val ret = Array<Any?>(leaf.size, { null });
                    for (i in 0 until ret.size step 3) {
                        ret[i] = loadNext(leaf[i], leaf[i + 1])

                        //TODO PERF key is deserialized twice, modify iterators...
                        ret[i + 1] = leaf[i] as K
                    }
                    return ret
                }
            }


            override fun hasNext(): Boolean {
                return leafArray!=null;
            }

            override fun next(): E {
                val leafArray = leafArray
                        ?: throw NoSuchElementException();
                val ret = leafArray[leafPos++]
                lastKey = leafArray[leafPos++] as K?
                val expireRecid = leafArray[leafPos++]

                if(leafPos==leafArray.size){
                    this.leafArray = moveToNextLeaf()
                    this.leafPos = 0;
                }
                this

                return ret as E;
            }

            override fun remove() {
                remove(lastKey
                        ?:throw IllegalStateException())
                lastKey = null;
            }

        }
    }


    protected fun htreeEntry(key:K, valueOrig:V) : MutableMap.MutableEntry<K?,V?>{

        return object : MutableMap.MutableEntry<K?,V?>{
            override val key: K?
                get() = key

            override val value: V?
                get() = valueCached ?: this@HTreeMap.get(key)

            /** cached value, if null get value from map */
            private var valueCached:V? = valueOrig;

            override fun hashCode(): Int {
                return keySerializer.hashCode(this.key!!, hashSeed) xor valueSerializer.hashCode(this.value!!, hashSeed)
            }
            override fun setValue(newValue: V?): V? {
                valueCached = null;
                return put(key,newValue)
            }


            override fun equals(other: Any?): Boolean {
                if (other !is Map.Entry<*, *>)
                    return false
                val okey = other.key ?: return false
                val ovalue = other.value ?: return false
                try{
                    return keySerializer.equals(key, okey as K)
                            && valueSerializer.equals(this.value!!, ovalue as V)
                }catch(e:ClassCastException) {
                    return false
                }
            }

            override fun toString(): String {
                return "MapEntry[${key}=${value}]"
            }

        }
    }

    override fun hashCode(): Int {
        var h = 0
        val i = entries.iterator()
        while (i.hasNext())
            h += i.next().hashCode()
        return h
    }

    override fun equals(other: Any?): Boolean {
        if (other === this)
            return true

        if (other !is java.util.Map<*, *>)
            return false

        if (other.size() != size)
            return false

        try {
            val i = entries.iterator()
            while (i.hasNext()) {
                val e = i.next()
                val key = e.key
                val value = e.value
                if (value == null) {
                    if (!(other.get(key) == null && other.containsKey(key)))
                        return false
                } else {
                    if (value != other.get(key))
                        return false
                }
            }
        } catch (unused: ClassCastException) {
            return false
        } catch (unused: NullPointerException) {
            return false
        }


        return true
    }


    override fun isClosed(): Boolean {
        return stores[0].isClosed()
    }

    protected fun listenerNotify(key:K, oldValue:V?, newValue: V?, triggered:Boolean){
        if(modificationListeners!=null)
            for(l in modificationListeners)
                l.modify(key, oldValue, newValue, triggered)
    }


    protected fun valueUnwrap(segment:Int, wrappedValue:Any):V{
        if(valueInline)
            return wrappedValue as V
        if(CC.ASSERT)
            Utils.assertReadLock(locks[segment])
        return stores[segment].get(wrappedValue as Long, valueSerializer)
                ?: throw DBException.DataCorruption("linked value not found")
    }


    protected fun valueWrap(segment:Int, value:V):Any{
        if(CC.ASSERT)
            Utils.assertWriteLock(locks[segment])

        return if(valueInline) value as Any
        else return stores[segment].put(value, valueSerializer)
    }

    override fun forEach(action: BiConsumer<in K, in V>) {
        action!!
        for(segment in 0 until segmentCount){
            segmentRead(segment){
                val store = stores[segment]
                indexTrees[segment].forEachValue { leafRecid ->
                    val leaf = store.get(leafRecid, leafSerializer)
                        ?: throw DBException.DataCorruption("leaf not found")
                    for(i in 0 until leaf.size step 3){
                        val key = leaf[i] as K
                        val value = valueUnwrap(segment, leaf[i+1])
                        action.accept(key, value)
                    }
                }
            }
        }
    }

    override fun forEachKey(action:  (K)->Unit) {
        for(segment in 0 until segmentCount){
            segmentRead(segment){
                val store = stores[segment]
                indexTrees[segment].forEachValue { leafRecid ->
                    val leaf = store.get(leafRecid, leafSerializer)
                            ?: throw DBException.DataCorruption("leaf not found")
                    for(i in 0 until leaf.size step 3){
                        val key = leaf[i] as K
                        action(key)
                    }
                }
            }
        }

    }

    override fun forEachValue(action:  (V)->Unit) {
        for(segment in 0 until segmentCount){
            segmentRead(segment){
                val store = stores[segment]
                indexTrees[segment].forEachValue { leafRecid ->
                    val leaf = store.get(leafRecid, leafSerializer)
                            ?: throw DBException.DataCorruption("leaf not found")
                    for(i in 0 until leaf.size step 3){
                        val value = valueUnwrap(segment, leaf[i+1])
                        action(value)
                    }
                }
            }
        }
    }


    override fun verify(){

        val expireEnabled = expireCreateQueues!=null || expireUpdateQueues!=null || expireGetQueues!=null

        for(segment in 0 until segmentCount) {
            segmentRead(segment) {
                val tree = indexTrees[segment]
                if(tree is Verifiable)
                    tree.verify()

                val leafRecids = LongHashSet()
                val expireRecids = LongHashSet()

                tree.forEachKeyValue { index, leafRecid ->
                    if(leafRecids.add(leafRecid).not())
                        throw DBException.DataCorruption("Leaf recid referenced more then once")

                    if(tree.get(index)!=leafRecid)
                        throw DBException.DataCorruption("IndexTree corrupted")

                    val leaf = stores[segment].get(leafRecid, leafSerializer)
                        ?:throw DBException.DataCorruption("Leaf not found")

                    for(i in 0 until leaf.size step 3){
                        val key = leaf[i] as K
                        val hash = hash(key)
                        if(segment!=hashToSegment(hash))
                            throw DBException.DataCorruption("Hash To Segment")
                        if(index!=hashToIndex(hash))
                            throw DBException.DataCorruption("Hash To Index")
                        val value = valueUnwrap(segment, leaf[i+1])

                        val expireRecid = leaf[i+2]
                        if(expireEnabled.not() && expireRecid!=0L)
                            throw DBException.DataCorruption("Expire mismatch")
                        if(expireEnabled && expireRecid!=0L
                                && expireRecids.add(expireNodeRecidFor(expireRecid as Long)).not())
                            throw DBException.DataCorruption("Expire recid used multiple times")

                    }
                }

                fun queue(qq: Array<QueueLong>?){
                    if(qq==null)
                        return
                    val q = qq[segment]
                    q.verify()

                    q.forEach { expireRecid, leafRecid, timestamp ->
                        if(leafRecids.contains(leafRecid).not())
                            throw DBException.DataCorruption("leafRecid referenced from Queue not part of Map")
                        val leaf = stores[segment].get(leafRecid, leafSerializer)
                            ?:throw DBException.DataCorruption("Leaf not found")

                        //find entry by timestamp
                        var found = false;
                        for(i in 0 until leaf.size step 3){
                            if(expireRecid==expireNodeRecidFor(leaf[i+2] as Long)) {
                                found = true
                                break;
                            }
                        }
                        if(!found)
                            throw DBException.DataCorruption("value from Queue not found in leaf $leafRecid "+Arrays.toString(leaf))

                        if(expireRecids.remove(expireRecid).not())
                            throw DBException.DataCorruption("expireRecid not part of IndexTree")
                    }
                }
                queue(expireCreateQueues)
                queue(expireUpdateQueues)
                queue(expireGetQueues)

                if(expireRecids.isEmpty.not())
                    throw DBException.DataCorruption("Some expireRecids are not in queues")
            }
        }
    }


    override fun close() {
        Utils.lockWriteAll(locks)
        try {
            closeable?.close()
        }finally{
            Utils.unlockWriteAll(locks)
        }
    }

}
