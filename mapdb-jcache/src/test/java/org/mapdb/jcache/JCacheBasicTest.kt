package org.mapdb.jcache

import org.junit.Test
import org.junit.Assert.*
import javax.cache.Caching
import javax.cache.configuration.MutableConfiguration

class JCacheBasicTest{

    @Test fun start(){
        val prov = Caching.getCachingProvider()
        val manager = prov.cacheManager

        val config = MutableConfiguration<String, Object>()
        config.setStoreByValue(true).setTypes(String::class.java, Object::class.java)

        val cache = manager.createCache("cache", config)

        assertTrue(prov is MJCachingProvider)
        assertTrue(manager is MJCacheManager)
        assertTrue(cache is MJCache)
    }

}