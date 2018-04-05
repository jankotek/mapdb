package org.mapdb.store

import org.mapdb.serializer.Serializer
import java.io.Closeable

/** Basic unmodifiable store */
interface Store: Closeable{

    /**
     * Get existing record
     *
     * @return record or null if record was not allocated yet, or was deleted
     **/
    fun <K> get(recid:Long, serializer: Serializer<K>):K?

    override fun close()

    /**
     * Iterates over all records in store.
     *
     * Function takes recid and binary data
     */
    fun getAll(consumer:(Long, ByteArray?)->Unit)
}

/** Modifiable store */
interface MutableStore:Store{

    /** allocates new null record, and returns its recid. It can be latter updated with `update()` or `cas` */
    fun preallocate():Long

    /** insert new record, returns recid under which record was stored */
    fun <K> put(record:K, serializer:Serializer<K>):Long

    /** update existing record with new value */
    fun <K> update(recid:Long, serializer: Serializer<K>, newRecord:K?)

    /** atomically compares and swap records
     * @return true if compare was sucessfull and record was swapped, else false
     */
    fun <K> compareAndUpdate(recid:Long, serializer:Serializer<K>, expectedOldRecord:K?, newRecord:K?):Boolean

    fun <K> compareAndDelete(recid:Long, serializer:Serializer<K>, expectedOldRecord:K?):Boolean

    /** delete existing record */
    fun <K> delete(recid:Long, serializer:Serializer<K>)

    //TODO move this method into Verifiable
    fun verify()

    //TODO in tx store?
    fun commit()

    //TODO move to threadAware
    val isThreadSafe: Boolean

    //TODO bg operations?
    fun compact()

}


object Recids{

    @JvmStatic val RECID_MAX_RESERVED = 63L
}
