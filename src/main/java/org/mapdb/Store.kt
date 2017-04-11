package org.mapdb


/**
 * Stores records
 */
interface StoreImmutable {

    fun <R> get(recid: Long, serializer: Serializer<R>): R?

    fun getAllRecids(): LongIterator

    fun getAllRecidsSize():Long {
        val iter = getAllRecids()
        var c:Long = 0L
        while(iter.hasNext()){
            iter.nextLong()
            c++
        }
        return c
    }


    fun getAllFiles(): Iterable<String>
}
/**
 * Stores records, mutable version
 */
interface Store: StoreImmutable, Verifiable,
        ConcurrencyAware { //TODO put assertions for underlying collections and Volumes

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

    override fun verify()

    val isReadOnly: Boolean

    fun fileLoad(): Boolean;
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
