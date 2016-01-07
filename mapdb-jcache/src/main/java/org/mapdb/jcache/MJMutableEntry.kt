package org.mapdb.jcache

import org.jsr107.ri.processor.MutableEntryOperation
import javax.cache.integration.CacheLoaderException
import javax.cache.processor.MutableEntry

class MJMutableEntry<K,V>(
        private val key:K,
        private val cache:MJCache<K,V>
): MutableEntry<K,V> {

    internal var _value:V? = null
    private var operation = MutableEntryOperation.NONE

    override fun getKey() = key

    override fun getValue():V? {
        if(operation== MutableEntryOperation.NONE){
            //load entry
            if(_value==null && cache.map.containsKey(key)){ //containsKey is so valueCreator is not triggered by map.get()
                _value = cache.map[key]
                operation = MutableEntryOperation.ACCESS
            }
        }

        if(_value==null && cache.isReadThrough)try{
            // check for read-through
            _value = cache.cacheLoader!!.load(key)
            if(_value!=null)
                operation = MutableEntryOperation.LOAD
        }catch(e:Exception){
            throw CacheLoaderException(e)
        }

        return _value
    }

    override fun <T : Any?> unwrap(clazz: Class<T>?): T {
        if (clazz != null && clazz.isInstance(this)) {
            return this as T
        } else {
            throw IllegalArgumentException("Class $clazz is unknown to this implementation")
        }
    }



    override fun exists(): Boolean {
        return (operation == MutableEntryOperation.NONE && cache.map.containsKey(key)) || _value != null;
    }

    override fun remove() {
        _value = null;

        operation =
                if(operation == MutableEntryOperation.CREATE)
                        MutableEntryOperation.NONE
                else
                    MutableEntryOperation.REMOVE
    }
    override fun setValue(value: V) {
        operation =
                if(cache.map.containsKey(key))
                    MutableEntryOperation.UPDATE
                else
                    MutableEntryOperation.CREATE
        this._value = value
    }

    fun operation() = operation;

}