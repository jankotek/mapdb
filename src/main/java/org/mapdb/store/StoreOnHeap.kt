package org.mapdb.store

import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet
import org.eclipse.collections.impl.stack.mutable.primitive.LongArrayStack
import org.mapdb.*
import org.mapdb.serializer.Serializer
import org.mapdb.util.*
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
        lock.lockWrite{
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
        lock.lockWrite{
            val recid = preallocate();
            @Suppress("UNCHECKED_CAST")
            update(recid, record ?: NULL_RECORD as R?, serializer);
            return recid
        }
    }

    override fun <R> update(recid: Long, record: R?, serializer: Serializer<R>) {
        lock.lockWrite{
            if(records.containsKey(recid).not())
                    throw DBException.GetVoid(recid);

            records.put(recid, record ?: NULL_RECORD)
        }
    }

    override fun <R> compareAndSwap(recid: Long, expectedOldRecord: R?, newRecord: R?, serializer: Serializer<R>): Boolean {
        //TODO use StampedLock here?
        lock.lockWrite {
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
        lock.lockWrite{
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
        //try to minimize maxRecid, and release free recids
        lock.lockWrite{
            val maxRecordRecid = records.keySet().maxIfEmpty(0L)
            val fr = freeRecids.toArray()
            freeRecids.clear()
            for(recid in fr){
                if(recid<maxRecordRecid)
                    freeRecids.push(recid)
            }
            maxRecid.set(maxRecordRecid)
        }
    }


    override fun close() {
        if(CC.PARANOID) {
            lock.lockWrite{
                val freeRecidsSet = LongHashSet();
                freeRecidsSet.addAll(freeRecids)
                for (recid in 1..maxRecid.get()) {
                    if (!freeRecidsSet.contains(recid) && !records.containsKey(recid))
                        throw IllegalStateException("Recid not used " + recid);
                }
            }
        }
    }

    override val isClosed = false

    override fun <R> get(recid: Long, serializer: Serializer<R>): R? {
        val record = lock.lockRead{
            records.get(recid)
        }

        return unwap(record, recid)
    }


    override fun getAllRecids(): LongIterator {
        lock.lockRead{
            return records.keySet().toArray().iterator()
        }
    }

    override fun getAllRecidsSize(): Long {
        lock.lockRead{
            return records.size().toLong()
        }
    }

    override fun verify() {
        lock.lockRead{
            freeRecids.forEach { recid ->
                if ((records.containsKey(recid)))
                    throw AssertionError("free recid is present")
                if(recid>maxRecid.get())
                    throw AssertionError("max recid")
            }
            records.keySet().forEach{ recid ->
                if(recid>maxRecid.get())
                    throw AssertionError("max recid")
            }
        }
    }

    override val isReadOnly = false

    override fun fileLoad() = false


    override fun getAllFiles(): Iterable<String> {
        return arrayListOf()
    }


}

