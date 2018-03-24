package org.mapdb.store

import org.mapdb.serializer.Serializer
import java.io.Closeable

/** basic unmodifiable store */
interface Store: Closeable{

    fun <K> get(recid:Long, serializer: Serializer<K>):K

    override fun close()
}


interface MutableStore{

    fun <K> put(record:K, serializer:Serializer<K>)

    fun <K> update(recid:Long, serializer: Serializer<K>, newRecord:K)

    fun <K> delete(recid:Long, serializer:Serializer<K>)


}