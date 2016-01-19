package org.mapdb.jcache

import org.jsr107.ri.event.RICacheEntryListenerRegistration
import org.jsr107.ri.event.RICacheEventDispatcher
import org.jsr107.ri.management.MBeanServerRegistrationUtility
import org.jsr107.ri.management.RICacheMXBean
import org.jsr107.ri.management.RICacheStatisticsMXBean
import org.jsr107.ri.processor.MutableEntryOperation
import org.jsr107.ri.processor.RIEntryProcessorResult
import org.mapdb.*
import java.io.Closeable
import java.io.IOException
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.logging.Level
import java.util.logging.Logger
import javax.cache.Cache
import javax.cache.CacheException
import javax.cache.configuration.*
import javax.cache.event.*
import javax.cache.integration.*
import javax.cache.processor.EntryProcessor
import javax.cache.processor.EntryProcessorException
import javax.cache.processor.EntryProcessorResult

/**
 * JCache wrapper around [HTreeMap]
 */
class MJCache<K,V>(
        private val cacheManager:MJCacheManager,
        private val name:String,
        configuration: Configuration<K, V>,
        private val executor: ExecutorService
): Cache<K,V> {


    class EntryEvent<K,V>(
            cache:MJCache<K,V>,
            eventType: EventType,
            private val key:K,
            private val value:V?,
            private val oldValue:V?
    ): CacheEntryEvent<K, V>(cache, eventType) {

        override fun getKey() = key

        override fun getValue() = value

        override fun getOldValue() = oldValue

        override fun isOldValueAvailable() = null!=oldValue

        override fun <T : Any?> unwrap(clazz: Class<T>?): T {
            if (clazz != null && clazz.isInstance(this))
                return this as T
            throw IllegalArgumentException("The class $clazz is unknown to this implementation")
        }

    }

    val cacheMXBean = RICacheMXBean<K, V>(this)
    val statistics = RICacheStatisticsMXBean(this)


    private val configuration:MutableConfiguration<K,V> = {
        //we make a copy of the configuration here so that the provided one
        //may be changed and or used independently for other caches.  we do this
        //as we don't know if the provided configuration is mutable
        if (configuration is CompleteConfiguration<K,V>) {
            //support use of CompleteConfiguration
            MutableConfiguration<K, V>(configuration)
        } else {
            //support use of Basic Configuration
            val mutableConfiguration = MutableConfiguration<K,V>()
            mutableConfiguration.setStoreByValue(configuration.isStoreByValue())
            mutableConfiguration.setTypes(configuration.getKeyType(), configuration.getValueType())
            MutableConfiguration<K, V>(mutableConfiguration)
        }
    }()


    internal val isWriteThrough = this.configuration.isWriteThrough
    internal val isReadThrough = this.configuration.isReadThrough
    internal val cacheLoader: CacheLoader<K,V>? = {
        if (this.configuration.cacheLoaderFactory != null) {
            this.configuration.cacheLoaderFactory.create()
        }else{
            null
        }
    }()

    private val listenerRegistrations: CopyOnWriteArrayList<RICacheEntryListenerRegistration<K, V>> = CopyOnWriteArrayList()
    //add all listeners

    val db:DB =
            if(configuration!!.isStoreByValue())
                DBMaker.memoryDB().make()
            else
                DBMaker.heapDB().make()

    val map:HTreeMap<K,V> = {
        var c = db.hashMap("cache",
                Serializer.JAVA as Serializer<K>, Serializer.JAVA as Serializer<V>)

        val executor = Executors.newSingleThreadScheduledExecutor { runnable->
            val t = Thread(runnable)
            t.isDaemon = true
            t.name = "MapDB-JCache evict thread"
            t
        }

        var period = 1000L
        c.expireExecutor(executor)


        if(configuration is CompleteConfiguration<*,*>) {
            val expirePolicy = configuration.expiryPolicyFactory.create()

            if (expirePolicy.expiryForCreation != null && expirePolicy.expiryForCreation.isEternal.not()) {
                val ttl = expirePolicy.expiryForCreation.timeUnit.toMillis(expirePolicy.expiryForCreation.durationAmount);
                period = Math.min(ttl/4, period)
                c = c.expireAfterCreate(ttl)
            }
            if (expirePolicy.expiryForUpdate != null && expirePolicy.expiryForUpdate.isEternal.not()) {
                val ttl = expirePolicy.expiryForUpdate.timeUnit.toMillis(expirePolicy.expiryForUpdate.durationAmount);
                period = Math.min(ttl/4, period)
                c = c.expireAfterUpdate(ttl)
            }
            if (expirePolicy.expiryForAccess != null && expirePolicy.expiryForAccess.isEternal.not()) {
                val ttl = expirePolicy.expiryForAccess.timeUnit.toMillis(expirePolicy.expiryForAccess.durationAmount);
                period = Math.min(ttl/4, period)
                c = c.expireAfterGet(ttl)
            }


            if(configuration.isReadThrough) {
                val loader = cacheLoader!!
                c = c.valueLoader { key ->
                    try {
                        cacheLoader.load(key)
                    }catch(e:Exception){
                        throw CacheLoaderException(e)
                    }
                }
            }

        }

        period = Math.max(10, period)
        c.expireExecutorPeriod(period)

        c.modificationListener(MapModificationListener { key, oldVal, newVal, evicted ->
            if(oldVal==null)
                statistics.increaseCachePuts(1)
            else if(newVal==null){
                //removed
                if(evicted)
                    statistics.increaseCacheEvictions(1)
                else
                    statistics.increaseCacheRemovals(1)
            }else if(!evicted){
                statistics.increaseCachePuts(1L)
            }
        })

        c.modificationListener(MapModificationListener { key, oldValue, newValue, expired ->

            val eventType = when{
                expired -> EventType.EXPIRED
                oldValue==null -> EventType.CREATED
                newValue==null -> EventType.REMOVED
                else -> EventType.UPDATED
            }

            val event =
                    if(newValue!=null)
                        EntryEvent(this, eventType, key!!, newValue, oldValue)
                    else
                        EntryEvent(this, eventType, key!!, oldValue, null)

            for(l in listenerRegistrations){
                if(l.cacheEntryFilter!=null && l.cacheEntryFilter.evaluate(event).not())
                    continue
                val entryListener = l.cacheEntryListener;
                if(entryListener is CacheEntryExpiredListener && eventType === EventType.EXPIRED)
                    entryListener.onExpired(arrayListOf(event))
                if(entryListener is CacheEntryCreatedListener && eventType === EventType.CREATED)
                    entryListener.onCreated(arrayListOf(event))
                if(entryListener is CacheEntryUpdatedListener && eventType === EventType.UPDATED)
                    entryListener.onUpdated(arrayListOf(event))
                if(entryListener is CacheEntryRemovedListener && eventType === EventType.REMOVED)
                    entryListener.onRemoved(arrayListOf(event))
            }
        })

        c.create()
    }()

    private val cacheWriter:CacheWriter<K,V>? =
        if(configuration is CompleteConfiguration && configuration.isWriteThrough){
            configuration.cacheWriterFactory.create() as CacheWriter<K, V>
        }else{
            null
        }

    init{
        for(listener in this.configuration.cacheEntryListenerConfigurations)
            createAndAddListener(listener)

        if (this.configuration.isManagementEnabled) {
            setManagementEnabled(true)
        }

        if (this.configuration.isStatisticsEnabled) {
            setStatisticsEnabled(true)
        }
    }

    override fun clear() {
        ensureOpen()
        map.clear2(notifyListeners=false)
    }

    override fun containsKey(key: K): Boolean {
        ensureOpen()
        return map.containsKey(key)
    }

    override fun get(key: K): V? {
        ensureOpen()
        val ret =  map[key]
        if(ret==null)
            statistics.increaseCacheMisses(1)
        else
            statistics.increaseCacheHits(1)
        return ret;
    }

    override fun getAll(keys: MutableSet<out K>?): MutableMap<K, V>? {
        ensureOpen()
        if(keys==null)
            throw NullPointerException("keys are null")

        if (keys.contains(null as K)) {
            throw NullPointerException("key")
        }
        // will throw NPE if keys=null
        val map = HashMap<K, V>(keys.size)

        val dispatcher = RICacheEventDispatcher<K, V>()

        for (key in keys) {
            val value = getValue(key, dispatcher)
            if (value != null) {
                map.put(key, value)
            }
        }

//        dispatcher.dispatch(listenerRegistrations)

        return map
    }

    private fun getValue(key: K, dispatcher: RICacheEventDispatcher<K, V>?): V? {
        return map[key];
    }

    override fun getAndPut(key: K, value: V): V? {
        ensureOpen()
        try{
            cacheWriter?.write(MJCacheEntry(key,value))
        }catch(e:Exception){
            throw CacheWriterException(e)
        }
        val old =  map.put(key,value)
        if(old==null) statistics.increaseCacheMisses(1)
        else statistics.increaseCacheHits(1)
        return old
    }

    override fun getAndRemove(key: K): V? {
        ensureOpen()
        try {
            cacheWriter?.delete(key)
        }catch(e:Exception){
            throw CacheWriterException(e)
        }
        val ret =  map.remove(key)
        if(ret!=null) statistics.increaseCacheHits(1)
        else statistics.increaseCacheMisses(1)
        return ret
    }

    override fun getAndReplace(key: K, value: V): V? {
        ensureOpen()
        val ret = map.replace(key,value)
        if(ret!=null)try{
            cacheWriter?.write(MJCacheEntry(key,value))
        }catch(e:Exception){
            throw CacheWriterException(e)
        }

        if(ret!=null) statistics.increaseCacheHits(1)
        else statistics.increaseCacheMisses(1)


        return ret;
    }

    override fun getCacheManager() = cacheManager

    override fun <C : Configuration<K, V>?> getConfiguration(clazz: Class<C>): C {
        if (clazz.isInstance(configuration)) {
            return clazz.cast(configuration)
        }
        throw IllegalArgumentException("The configuration class $clazz is not supported by this implementation")
    }

    override fun getName() = name

    override fun <T : Any?> invoke(key: K?, entryProcessor: EntryProcessor<K, V, T>?, vararg arguments: Any?): T {
        ensureOpen()
        if(key==null)
            throw NullPointerException("null key")
        if(entryProcessor==null)
            throw NullPointerException("null entryProcessor")

        val entry = MJMutableEntry(key,  this)
        val ret = try{
            entryProcessor.process(entry, *arguments)
        }catch(e: CacheException){
            throw e
        }catch(e:Exception){
            throw EntryProcessorException(e)
        }


        when(entry.operation()){
            MutableEntryOperation.NONE ->{
                if(map.containsKey(entry.key)) statistics.increaseCacheHits(1)
                else statistics.increaseCacheMisses(1)
            }
            MutableEntryOperation.ACCESS ->{
                val ret = map[key]
                if(ret==null) statistics.increaseCacheMisses(1)
                else statistics.increaseCacheHits(1)

            }
            MutableEntryOperation.REMOVE ->{
                val old = map.remove(key)
                if(old!=null) {
                    statistics.increaseCacheHits(1)
                }
                try {
                    cacheWriter?.delete(key)
                }catch(e:Exception){
                    throw CacheWriterException(e)
                }
            }
            MutableEntryOperation.CREATE ->{
                try {
                    cacheWriter?.write(MJCacheEntry(key,entry._value))
                }catch(e:Exception){
                    throw CacheWriterException(e)
                }
                entry._value ?: throw EntryProcessorException("null value")

                map.put(key, entry._value)
            }
            MutableEntryOperation.LOAD -> {
                entry._value ?: throw EntryProcessorException("null value")
                map.put(key, entry._value)
            }
            MutableEntryOperation.UPDATE ->{
                try {
                    cacheWriter?.write(MJCacheEntry(key,entry._value))
                }catch(e:Exception){
                    throw CacheWriterException(e)
                }
                statistics.increaseCacheHits(1)
                entry._value ?: throw EntryProcessorException("null value")
                map.put(key, entry._value)
            }
            else ->{
                throw UnsupportedOperationException("Unknown operation: "+entry.operation())
            }

        }

        return ret
    }

    override fun <T : Any?> invokeAll(keys: MutableSet<out K>?, entryProcessor: EntryProcessor<K, V, T>?, vararg arguments: Any?): MutableMap<K, EntryProcessorResult<T>> {
        if(keys==null || entryProcessor==null)
            throw NullPointerException()
        ensureOpen()
        val ret = HashMap<K, EntryProcessorResult<T>>()
        for(key in keys!!){
            try {
                val res = invoke(key, entryProcessor, *arguments)
                if(res!=null)
                    ret.put(key, RIEntryProcessorResult<T>(res))
            }catch(e:Exception){
                ret.put(key, RIEntryProcessorResult<T>(e))
            }
        }
        return ret
    }

    override fun close() {

        if(db.isClosed())
            return

        //ensure that any further access to this Cache will raise an
        // IllegalStateException
        db.close()

        //ensure that the cache may no longer be accessed via the CacheManager
        cacheManager.releaseCache(name)

        //disable statistics and management
        setStatisticsEnabled(false)
        setManagementEnabled(false)

        //close the configured CacheLoader
        if (cacheLoader is Closeable) {
            try {
                cacheLoader.close()
            } catch (e: IOException) {
                Logger.getLogger(this.getName()).log(Level.WARNING, "Problem " + "closing CacheLoader " + cacheLoader.javaClass, e)
            }

        }

//        //close the configured CacheWriter
//        if (cacheWriter is Closeable) {
//            try {
//                (cacheWriter as Closeable).close()
//            } catch (e: IOException) {
//                Logger.getLogger(this.getName()).log(Level.WARNING, "Problem " + "closing CacheWriter " + cacheWriter.javaClass, e)
//            }
//
//        }
//
//        //close the configured ExpiryPolicy
//        if (expiryPolicy is Closeable) {
//            try {
//                (expiryPolicy as Closeable).close()
//            } catch (e: IOException) {
//                Logger.getLogger(this.getName()).log(Level.WARNING, "Problem " + "closing ExpiryPolicy " + cacheLoader.javaClass, e)
//            }
//
//        }
//
//        //close the configured CacheEntryListeners
//        for (registration in listenerRegistrations) {
//            if (registration.getCacheEntryListener() is Closeable) {
//                try {
//                    (registration as Closeable).close()
//                } catch (e: IOException) {
//                    Logger.getLogger(this.getName()).log(Level.WARNING, "Problem " + "closing listener " + cacheLoader.javaClass, e)
//                }
//
//            }
//        }
//
//        //attempt to shutdown (and wait for the cache to shutdown)
//        executorService.shutdown()
//        try {
//            executorService.awaitTermination(10, TimeUnit.SECONDS)
//        } catch (e: InterruptedException) {
//            throw CacheException(e)
//        }
//
//
//        //drop all entries from the cache
//        entries.clear()

    }


    override fun isClosed() = db.isClosed()

    override fun iterator(): MutableIterator<Cache.Entry<K, V>> {
        ensureOpen()

        val iter =  map.iterator()
        return object: MutableIterator<Cache.Entry<K, V>>{

            var lastKey:K? = null;

            override fun remove() {
                iter.remove()
                if(cacheWriter!=null && lastKey!=null){
                    try {
                        cacheWriter.delete(lastKey)
                    }catch(e:Exception){
                        throw CacheWriterException(e)
                    }
                    lastKey = null;
                }
            }

            override fun hasNext(): Boolean {
                return iter.hasNext()
            }

            override fun next(): Cache.Entry<K, V> {
                val e = iter.next()
                lastKey = e.key
                statistics.increaseCacheHits(1)
                return MJCacheEntry(e.key!!, e.value!!)
            }

        }
    }

    private fun ensureOpen(){
        //TODO if this cache is stateless, all is done in map, there is an race condition, HTreeMap should probably throw IllegalStateException as well
        if(isClosed)
            throw IllegalStateException("Cache closed");
    }


    override fun loadAll(keys: MutableSet<out K>?, replaceExistingValues: Boolean, completionListener: CompletionListener?) {
        ensureOpen();

        if(keys==null)
            throw NullPointerException("keys are null")

        if(cacheLoader == null) {
            completionListener?.onCompletion()
            return
        }

        for(k in keys)
            if(k==null)
                throw NullPointerException("keys contains a null")

        executor.submit {
            try{
                val keysToLoad = ArrayList<K>()
                for (key in keys) {
                    if (replaceExistingValues || !containsKey(key)) {
                        keysToLoad.add(key)
                    }
                }

                val loaded: MutableMap<out K, out V>
                try {
                    loaded = cacheLoader!!.loadAll(keysToLoad)
                } catch (e: Exception) {
                    if (e !is CacheLoaderException) {
                        throw CacheLoaderException("Exception in CacheLoader", e)
                    } else {
                        throw e
                    }
                }

                //remove nulls
                for (key in keysToLoad) {
                    if (loaded[key] == null) {
                        loaded.remove(key)
                    }
                }

                //put into map, TODO create single function?
                loaded.forEach { e ->
                    if(replaceExistingValues)
                        map.put(e.key,e.value)
                    else
                        map.putIfAbsent(e.key, e.value)
                }

                completionListener?.onCompletion()
            }catch(e:Exception){
                completionListener?.onException(e)
            }

        }

    }

    override fun put(key: K?, value: V?) {
        ensureOpen()
        try{
            cacheWriter?.write(MJCacheEntry(key,value))
        }catch(e:Exception){
            throw CacheWriterException(e)
        }
        map.put(key!!,value!!)
    }

    override fun putAll(map2: MutableMap<out K, out V>?) {
        ensureOpen()
        if (map2==null || map2.containsKey(null as K)) {
            throw NullPointerException("key")
        }

        map2.forEach { e->
            try {
                cacheWriter?.write(MJCacheEntry(e.key, e.value))
            }catch(e:Exception){
                throw CacheWriterException(e)
            }
            map.put(e.key, e.value)
        }
    }

    override fun putIfAbsent(key: K?, value: V?): Boolean {
        ensureOpen()
        val ret = map.putIfAbsentBoolean(key!!, value!!)

        if(ret && cacheWriter!=null)try{
            cacheWriter.write(MJCacheEntry(key, value))
        }catch(e:Exception){
            throw CacheWriterException(e)
        }

        if(ret) statistics.increaseCacheMisses(1)
        else statistics.increaseCacheHits(1)

        return ret
    }


    override fun remove(key: K?): Boolean {
        ensureOpen()
        try{
            cacheWriter?.delete(key)
        }catch(e:Exception){
            throw CacheWriterException(e)
        }

        return map.remove(key!!)!=null
    }

    override fun remove(key: K?, oldValue: V?): Boolean {
        ensureOpen()
        val ret =  map.remove(key!!, oldValue!!)
        if(ret)try{
            cacheWriter?.delete(key)
        }catch(e:Exception){
            throw CacheWriterException(e)
        }

        if(ret) statistics.increaseCacheHits(1)
        else statistics.increaseCacheMisses(1)

        return ret
    }

    override fun removeAll() {
        ensureOpen()
        if(map.isEmpty().not())
        try{
            cacheWriter?.deleteAll(map.keys)
        }catch(e:Exception){
            throw CacheWriterException(e)
        }

        map.clear()
    }

    override fun removeAll(keys: MutableSet<out K>?) {
        ensureOpen()
        for(key in keys!!) {
            try{
                cacheWriter?.delete(key)
            }catch(e:Exception){
                throw CacheWriterException(e)
            }
            val old = map.remove(key)

        }
    }

    override fun replace(key: K?, oldValue: V?, newValue: V?): Boolean {
        ensureOpen()
        if(map.containsKey(key))
            statistics.increaseCacheHits(1)
        else
            statistics.increaseCacheMisses(1)

        val ret =  map.replace(key!!, oldValue!!, newValue!!)
        if(ret) {
            try {
                cacheWriter?.write(MJCacheEntry(key, newValue))
            } catch(e: Exception) {
                throw CacheWriterException(e)
            }
        }

        return ret;
    }

    override fun replace(key: K?, value: V?): Boolean {
        ensureOpen()
        val ret =  map.replace(key!!, value)!=null
        if(ret) {
            statistics.increaseCacheHits(1)
            try {
                cacheWriter?.write(MJCacheEntry(key, value))
            } catch(e: Exception) {
                throw CacheWriterException(e)
            }
        }else{
            statistics.increaseCacheMisses(1)
        }

        return ret;
    }

    override fun <T : Any?> unwrap(clazz: Class<T>?): T {
        if(clazz==null)
            throw NullPointerException()
        if (clazz.isAssignableFrom(javaClass)) {
            return clazz.cast(this)
        }

        throw IllegalArgumentException("Unwapping to $clazz is not a supported by this implementation")
    }




    override fun registerCacheEntryListener(cacheEntryListenerConfiguration: CacheEntryListenerConfiguration<K, V>?) {
        cacheEntryListenerConfiguration!!
        configuration.addCacheEntryListenerConfiguration(cacheEntryListenerConfiguration)
        createAndAddListener(cacheEntryListenerConfiguration)
    }


    override fun deregisterCacheEntryListener(cacheEntryListenerConfiguration: CacheEntryListenerConfiguration<K, V>?) {
        removeListener(cacheEntryListenerConfiguration)
    }

    private fun createAndAddListener(listenerConfiguration: CacheEntryListenerConfiguration<K, V>) {
        val registration = RICacheEntryListenerRegistration(listenerConfiguration)
        listenerRegistrations.add(registration)
    }


    private fun removeListener(cacheEntryListenerConfiguration: CacheEntryListenerConfiguration<K, V>?) {

        if (cacheEntryListenerConfiguration == null) {
            throw NullPointerException("CacheEntryListenerConfiguration can't be " + "null")
        }

        for (listenerRegistration in listenerRegistrations) {
            if (cacheEntryListenerConfiguration == listenerRegistration.configuration) {
                listenerRegistrations.remove(listenerRegistration)
                configuration.removeCacheEntryListenerConfiguration(cacheEntryListenerConfiguration)
            }
        }
    }



    /**
     * Sets statistics
     */
    fun setStatisticsEnabled(enabled: Boolean) {
        if (enabled) {
            MBeanServerRegistrationUtility.registerCacheObject(this, MBeanServerRegistrationUtility.ObjectNameType.Statistics)
        } else {
            MBeanServerRegistrationUtility.unregisterCacheObject(this, MBeanServerRegistrationUtility.ObjectNameType.Statistics)
        }
        configuration.setStatisticsEnabled(enabled)
    }


    /**
     * Sets management enablement

     * @param enabled true if management should be enabled
     */
    fun setManagementEnabled(enabled: Boolean) {
        if (enabled) {
            MBeanServerRegistrationUtility.registerCacheObject(this, MBeanServerRegistrationUtility.ObjectNameType.Configuration)
        } else {
            MBeanServerRegistrationUtility.unregisterCacheObject(this, MBeanServerRegistrationUtility.ObjectNameType.Configuration)
        }
        configuration.setManagementEnabled(enabled)
    }
}
