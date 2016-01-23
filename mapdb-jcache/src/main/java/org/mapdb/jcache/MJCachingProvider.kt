package org.mapdb.jcache

import java.net.URI
import java.net.URISyntaxException
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import javax.cache.CacheException
import javax.cache.CacheManager
import javax.cache.configuration.OptionalFeature
import javax.cache.spi.CachingProvider
import kotlin.concurrent.withLock

/**
 *
 */
class MJCachingProvider: CachingProvider {

    private val lock = ReentrantLock()


    /**
     * The CacheManagers scoped by ClassLoader and URI.
     */
    private var cacheManagersByClassLoader: WeakHashMap<ClassLoader, HashMap<URI, CacheManager>> = WeakHashMap()


    override fun getCacheManager(): CacheManager? {
        return getCacheManager(getDefaultURI(), getDefaultClassLoader(), getDefaultProperties())
    }

    override fun getCacheManager(uri: URI?, classLoader: ClassLoader?): CacheManager? {
        return getCacheManager(uri, classLoader, defaultProperties)
    }

    override fun getCacheManager(uri: URI?, classLoader: ClassLoader?, properties: Properties?): CacheManager? {
        lock.withLock {
            val managerURI = uri ?: defaultURI
            val managerClassLoader = classLoader ?: defaultClassLoader
            val managerProperties = properties ?: Properties()

            var cacheManagersByURI: HashMap<URI, CacheManager>? = cacheManagersByClassLoader[managerClassLoader]

            if (cacheManagersByURI == null) {
                cacheManagersByURI = HashMap<URI, CacheManager>()
            }

            var cacheManager: CacheManager? = cacheManagersByURI[managerURI]

            if (cacheManager == null) {
                cacheManager = MJCacheManager(this, managerURI, managerClassLoader, managerProperties)

                cacheManagersByURI.put(managerURI, cacheManager)
            }

            if (!cacheManagersByClassLoader.containsKey(managerClassLoader)) {
                cacheManagersByClassLoader.put(managerClassLoader, cacheManagersByURI)
            }

            return cacheManager
        }
    }


    override fun close() {
        lock.withLock {
            val managersByClassLoader = this.cacheManagersByClassLoader
            this.cacheManagersByClassLoader = WeakHashMap<ClassLoader, HashMap<URI, CacheManager>>()

            managersByClassLoader.forEach { classLoader, hashMap ->
                hashMap.forEach { uri, cacheManager ->
                    cacheManager.close()
                }
            }
        }
    }

    override fun close(classLoader: ClassLoader?) {
        lock.withLock {
            val cacheManagersByURI = cacheManagersByClassLoader.remove(classLoader)
            cacheManagersByURI?.forEach { uri, cacheManager ->
                cacheManager.close()
            }
        }
    }

    override fun close(uri: URI?, classLoader: ClassLoader?) {
        lock.withLock {
            val managerURI = uri ?: defaultURI
            val managerClassLoader = classLoader ?: defaultClassLoader

            val cacheManagersByURI = cacheManagersByClassLoader[managerClassLoader]
            if (cacheManagersByURI != null) {
                val cacheManager = cacheManagersByURI.remove(managerURI)

                cacheManager?.close()

                if (cacheManagersByURI.size == 0) {
                    cacheManagersByClassLoader.remove(managerClassLoader)
                }
            }
        }
    }

    override fun getDefaultClassLoader(): ClassLoader {
        return javaClass.classLoader
    }

    override fun getDefaultProperties(): Properties {
        return Properties()
    }

    override fun getDefaultURI(): URI {
        try {
            return URI(this.javaClass.name)
        } catch (e: URISyntaxException) {
            throw CacheException(
                    "Failed to create the default URI for the javax.cache Reference Implementation",
                    e)
        }

    }

    override fun isSupported(optionalFeature: OptionalFeature?): Boolean {
        return when(optionalFeature){
            OptionalFeature.STORE_BY_REFERENCE -> true
            else -> false
        }

    }

    /**
     * Releases the CacheManager with the specified URI and ClassLoader
     * from this CachingProvider.  This does not close the CacheManager.  It
     * simply releases it from being tracked by the CachingProvider.
     *
     *
     * This method does nothing if a CacheManager matching the specified
     * parameters is not being tracked.
     *
     * @param uri         the URI of the CacheManager
     * *
     * @param classLoader the ClassLoader of the CacheManager
     */
    fun releaseCacheManager(uri: URI?, classLoader: ClassLoader?) {
        lock.withLock {
            val managerURI = uri ?: defaultURI
            val managerClassLoader = classLoader ?: defaultClassLoader

            val cacheManagersByURI = cacheManagersByClassLoader[managerClassLoader]
            if (cacheManagersByURI != null) {
                cacheManagersByURI.remove(managerURI)

                if (cacheManagersByURI.size == 0) {
                    cacheManagersByClassLoader.remove(managerClassLoader)
                }
            }
        }
    }


}