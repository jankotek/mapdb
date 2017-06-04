package org.mapdb.store

import org.mapdb.*

/**
 * Wraps Store and throws `UnsupportedOperationException("Read-only")` on operations which would modify it
 */
class StoreReadOnlyWrapper(protected val store:Store):Store{

    override fun close() {
        store.close()
    }

    override fun commit() {
        throw UnsupportedOperationException("Read-only")
    }

    override fun compact() {
        throw UnsupportedOperationException("Read-only")
    }

    override fun <R> compareAndSwap(recid: Long, expectedOldRecord: R?, newRecord: R?, serializer: Serializer<R>): Boolean {
        throw UnsupportedOperationException("Read-only")
    }

    override fun <R> delete(recid: Long, serializer: Serializer<R>) {
        throw UnsupportedOperationException("Read-only")
    }

    override val isClosed: Boolean
        get() = store.isClosed

    override val isThreadSafe: Boolean
        get() = store.isThreadSafe

    override val isReadOnly = true

    override fun preallocate(): Long {
        throw UnsupportedOperationException("Read-only")
    }

    override fun <R> put(record: R?, serializer: Serializer<R>): Long {
        throw UnsupportedOperationException("Read-only")
    }

    override fun <R> update(recid: Long, record: R?, serializer: Serializer<R>) {
        throw UnsupportedOperationException("Read-only")
    }

    override fun verify() {
        store.verify()
    }

    override fun <R> get(recid: Long, serializer: Serializer<R>): R? {
        return store.get(recid, serializer)
    }

    override fun getAllRecids(): LongIterator {
        return store.getAllRecids()
    }

    override fun fileLoad() = store.fileLoad()

    override fun getAllFiles() = store.getAllFiles()

}