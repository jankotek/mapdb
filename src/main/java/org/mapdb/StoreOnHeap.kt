package org.mapdb

import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet
import org.eclipse.collections.impl.stack.mutable.primitive.LongArrayStack
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * Store which does not use serialization, but puts everything into on-heap Map.
 */
class StoreOnHeap(
        override val isThreadSafe:Boolean=true
    ) : Store{

    private val lock: ReentrantReadWriteLock? = if(isThreadSafe) ReentrantReadWriteLock() else null

    /** stack of deleted recids, those will be reused*/
    private val freeRecids = LongArrayStack();
    /** maximal allocated recid. All other recids should be in `freeRecid` stack or in `records`*/
    private val maxRecid = AtomicLong();

    /** Stores data */
    private val records = LongObjectHashMap<Any>();

    /** Represents null record, `records` map does not allow nulls*/
    companion object {
        private val NULL_RECORD = Object();
    }

    private fun <R> unwap(r:Any?, recid:Long):R?{
        if(NULL_RECORD === r)
            return null;
        if(null == r)
            throw DBException.GetVoid(recid)

        @Suppress("UNCHECKED_CAST")
        return r as R
    }

    override fun preallocate(): Long {
        Utils.lockWrite(lock) {
            val recid =
                    if (freeRecids.isEmpty)
                        maxRecid.incrementAndGet()
                    else
                        freeRecids.pop()

            if(records.containsKey(recid))
                throw DBException.DataCorruption("Old data were not null");
            records.put(recid, NULL_RECORD)
            return recid;
        }
    }

    override fun <R> put(record: R?, serializer: Serializer<R>): Long {
        Utils.lockWrite(lock) {
            val recid = preallocate();
            @Suppress("UNCHECKED_CAST")
            update(recid, record ?: NULL_RECORD as R?, serializer);
            return recid
        }
    }

    override fun <R> update(recid: Long, record: R?, serializer: Serializer<R>) {
        Utils.lockWrite(lock) {
            if(records.containsKey(recid).not())
                    throw DBException.GetVoid(recid);

            records.put(recid, record ?: NULL_RECORD)
        }
    }

    override fun <R> compareAndSwap(recid: Long, expectedOldRecord: R?, newRecord: R?, serializer: Serializer<R>): Boolean {
        //TODO use StampedLock here?
        Utils.lockWrite(lock) {
            val old2 = records.get(recid)
                    ?: throw DBException.GetVoid(recid);

            val old = unwap<R>(old2, recid);
            if (old != expectedOldRecord)
                return false;

            records.put(recid, newRecord ?: NULL_RECORD)
            return true;
        }
    }

    override fun <R> delete(recid: Long, serializer: Serializer<R>) {
        Utils.lockWrite(lock) {
            if(!records.containsKey(recid))
                throw DBException.GetVoid(recid);

            records.remove(recid)
            freeRecids.push(recid);
        }
    }

    override fun commit() {
        //nothing to commit
    }

    override fun compact() {
        //nothing to compact
    }


    override fun close() {
        if(CC.PARANOID) {
            Utils.lockWrite(lock) {
                val freeRecidsSet = LongHashSet();
                freeRecidsSet.addAll(freeRecids)
                for (recid in 1..maxRecid.get()) {
                    if (!freeRecidsSet.contains(recid) && !records.containsKey(recid))
                        throw AssertionError("Recid not used " + recid);
                }
            }
        }
    }

    override val isClosed = false

    override fun <R> get(recid: Long, serializer: Serializer<R>): R? {
        val record = Utils.lockRead(lock) {
            records.get(recid)
        }

        return unwap(record, recid)
    }


    override fun getAllRecids(): LongIterator {
        Utils.lockRead(lock){
            return records.keySet().toArray().iterator()
        }
    }

    override fun getAllRecidsSize(): Long {
        Utils.lockRead(lock){
            return records.size().toLong()
        }
    }

    override fun verify() {
    }

    override val isReadOnly = false

    override fun fileLoad() = false


    override fun getAllFiles(): Iterable<String> {
        return arrayListOf()
    }


}

