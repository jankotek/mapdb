package org.mapdb.jcache

import com.google.common.cache.CacheLoader
import org.mapdb.DBMaker
import org.mapdb.Serializer
import java.net.URI
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Level
import java.util.logging.Logger
import javax.cache.Cache
import javax.cache.CacheException
import javax.cache.CacheManager
import javax.cache.configuration.CompleteConfiguration
import javax.cache.configuration.Configuration
import javax.cache.configuration.Factory
import javax.cache.configuration.MutableConfiguration
import javax.cache.spi.CachingProvider


/**
 * JCache wrapper for [DB]
 */
class MJCacheManager(
        private val cachingProvider:MJCachingProvider,
        private val uri:URI,
        private val classLoader:ClassLoader,
        private val properties:Properties
): CacheManager {

    companion object {
        private val LOG = Logger.getLogger(MJCacheManager::class.java.canonicalName)
    }

    private val caches:MutableMap<String, MJCache<Any,Any>> = ConcurrentHashMap()

    private val executor:ExecutorService = Executors.newCachedThreadPool()

    private val isClosed = AtomicBoolean()

    override fun <K : Any?, V : Any?, C : Configuration<K, V>?> createCache(
            cacheName: String?,
            configuration: C): Cache<K, V> {

        ensureNotClosed()
        if(cacheName==null)
            throw NullPointerException("cacheName null")

        if(configuration==null)
            throw NullPointerException("configuration null")

        if(caches.containsKey(cacheName))
            throw CacheException("A cache named $cacheName already exists.")


        val cache = MJCache(this, cacheName, configuration as Configuration<K, V>, executor)
        caches.put(cacheName, cache as MJCache<Any,Any>)
        return cache
    }

    override fun destroyCache(cacheName: String?) {
        ensureNotClosed()

        val cache = caches.remove(cacheName)
                ?: return

        cache.clear()
        cache.close()
    }

    override fun enableManagement(cacheName: String?, enabled: Boolean) {
        ensureNotClosed()
        if(cacheName==null)
            throw NullPointerException("cacheName null")

        val cache = caches[cacheName]
                ?: throw IllegalArgumentException("Cache $cacheName not found")

        cache.setManagementEnabled(enabled)
    }

    override fun enableStatistics(cacheName: String?, enabled: Boolean) {
        ensureNotClosed()

        if(cacheName==null)
            throw NullPointerException("cacheName null")

        val cache = caches[cacheName]
            ?: throw IllegalArgumentException("Cache $cacheName not found")

        cache.setStatisticsEnabled(enabled)
    }

    override fun <K : Any?, V : Any?> getCache(cacheName: String?): Cache<K, V>? {
        ensureNotClosed()
        if(cacheName==null)
            throw NullPointerException("cacheName null")

        val cache = caches[cacheName]
            ?:return null

        val configuration = cache.getConfiguration(CompleteConfiguration::class.java as Class<CompleteConfiguration<Any,Any>>)

        if (configuration.keyType != Any::class.java || configuration.valueType != Any::class.java) {
            throw IllegalArgumentException("Cache " + cacheName + " was " + "defined with specific types Cache<" +
                    configuration.keyType + ", " + configuration.valueType + "> " + "in which case CacheManager.getCache(String, Class, Class) must be used")
        }

        return cache as Cache<K,V>
    }

    private fun ensureNotClosed() {
        if(isClosed())
            throw IllegalStateException("CacheManager is closed");
    }

    override fun <K : Any?, V : Any?> getCache(cacheName: String?, keyType: Class<K>?, valueType: Class<V>?): Cache<K, V>? {
        ensureNotClosed()
        if(cacheName==null)
            throw NullPointerException("cacheName null")
        if(keyType==null)
            throw NullPointerException("keyType null")
        if(valueType==null)
            throw NullPointerException("valueType null")


        val cache = caches[cacheName]
            ?: return null;

        val configuration = cache.getConfiguration(CompleteConfiguration::class.java as Class<CompleteConfiguration<Any,Any>>)

        if (configuration.getKeyType() != null && configuration.getKeyType() == keyType) {

            if (configuration.getValueType() != null && configuration.getValueType() == valueType) {
                return cache as Cache<K, V>
            } else {
                throw ClassCastException("Incompatible cache value types specified, expected " + configuration.getValueType() + " but " + valueType + " was specified")
            }
        } else {
            throw ClassCastException("Incompatible cache key types specified, expected " + configuration.getKeyType() + " but " + keyType + " was specified")
        }

        return cache as Cache<K,V>;
//        val keySerializer = Serializer.serializerForClass(keyType)
//                ?: return null
//        val valueSerializer = Serializer.serializerForClass(valueType)
//                ?: return null
//
//        val db = DBMaker.memoryDB()
//
//        val map = db.hashMapCreate<K,V>("cache", keySerializer, valueSerializer)
//        val cache = MJCache(cacheName, db, map, MutableConfiguration<K,V>(), executor)
//        caches.put(cacheName, cache as MJCache<Any,Any>)
//        return cache
    }

    override fun getCacheNames(): MutableIterable<String>? {
        val list = ArrayList<String>()
        list.addAll(caches.keys)
        return Collections.unmodifiableList(list)
    }

    override fun getCachingProvider(): CachingProvider? {
        ensureNotClosed()

        return cachingProvider
    }

    override fun getClassLoader() = classLoader


    override fun getProperties(): Properties? {
        return properties
    }

    override fun getURI() = uri

    override fun isClosed() = isClosed.get()

    override fun close() {
        val closed = isClosed.getAndSet(true)
        if(closed)
            return
        //first releaseCacheManager the CacheManager from the CacheProvider so that
        //future requests for this CacheManager won't return this one
        cachingProvider.releaseCacheManager(getURI(), getClassLoader())

        executor.shutdown()
        executor.awaitTermination(1000, TimeUnit.DAYS)
        for(cache in caches.values){
            try {
                cache.close()
            } catch (e: Exception) {
                LOG.log(Level.WARNING, "Error stopping cache: " + cache, e)
            }

        }
        caches.clear()
    }


    override fun <T : Any?> unwrap(clazz: Class<T>?): T {
        if(clazz==null)
            throw NullPointerException()
        if (clazz.isAssignableFrom(javaClass)) {
            return clazz.cast(this)
        }

        throw IllegalArgumentException("Unwapping to $clazz is not a supported by this implementation")
    }

    /**
     * Releases the Cache with the specified name from being managed by
     * this CacheManager.

     * @param cacheName the name of the Cache to releaseCacheManager
     */
    internal fun releaseCache(cacheName: String?) {
        if (cacheName == null) {
            throw NullPointerException()
        }

        caches.remove(cacheName)
    }


}
