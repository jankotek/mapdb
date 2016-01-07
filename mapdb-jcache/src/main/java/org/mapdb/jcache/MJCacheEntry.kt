package org.mapdb.jcache

import javax.cache.Cache

/**
 * Cache entry
 */
data class MJCacheEntry<K,V>(
        private val key:K,
        private val value:V
): Cache.Entry<K,V>{

    override fun getKey(): K {
        return key;
    }

    override fun getValue(): V {
        return value
    }

    override fun <T : Any?> unwrap(clazz: Class<T>?): T {
        if (clazz != null && clazz.isInstance(this)) {
            return this as T
        } else {
            throw IllegalArgumentException("Class $clazz is unknown to this implementation")
        }

    }

    override fun hashCode(): Int {
        return key!!.hashCode()
    }
}
