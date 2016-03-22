package org.mapdb

import java.io.IOException

/**
 * Stores records
 */
interface StoreImmutable{

    fun <R> get(recid: Long, serializer: Serializer<R>): R?

    fun getAllRecids(): LongIterator
}

/**
 * Stores records, mutable version
 */
interface Store: StoreImmutable, Verifiable {

    fun preallocate():Long;

    fun <R> put(record: R?, serializer: Serializer<R>):Long
    fun <R> update(recid: Long, record: R?, serializer: Serializer<R>)
    fun <R> compareAndSwap(recid: Long,
                           expectedOldRecord: R?,
                           newRecord: R?,
                           serializer: Serializer<R>
                        ): Boolean

    fun <R> delete(recid: Long, serializer: Serializer<R>)

    fun commit();
    fun compact()

    fun close();
    val isClosed:Boolean;

    val isThreadSafe:Boolean;

    override fun verify()
}

/**
 * Stores records, transactional version
 */
interface StoreTx:Store{
    fun rollback();
}

interface StoreBinary:Store{

    fun getBinaryLong(recid:Long, f:StoreBinaryGetLong):Long
}
