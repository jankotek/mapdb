package org.mapdb.store

import org.eclipse.collections.api.list.primitive.LongList
import org.eclipse.collections.api.list.primitive.MutableLongList
import org.eclipse.collections.impl.factory.primitive.LongLists
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet
import org.mapdb.DBException
import org.mapdb.serializer.Serializer
import org.mapdb.util.lockRead
import org.mapdb.util.lockWrite
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

class StoreOnHeap:MutableStore{

    protected object NULL_RECORD:Object()


    protected val lock: ReadWriteLock? = ReentrantReadWriteLock()

    protected val records: LongObjectHashMap<Any> = LongObjectHashMap.newMap()

    protected val freeRecids = LongLists.mutable.empty()

    protected var maxRecid:Long = 0;


    protected fun check(value:Any?):Any?{
        return if(value===NULL_RECORD) null
        else value ?:throw DBException.RecidNotFound()
    }

    override fun <K> get(recid: Long, serializer: Serializer<K>): K? {
        lock.lockRead {
            return check(records.get(recid)) as K?
        }
    }

    protected fun preallocate2():Long{
        val recid =
                if(freeRecids.isEmpty) ++maxRecid
                else freeRecids.removeAtIndex(freeRecids.size()-1)

        records.put(recid, NULL_RECORD)
        return recid
    }
    override fun preallocate(): Long {
        lock.lockWrite {
            return preallocate2()
        }
    }

    override fun <K> put(record: K, serializer: Serializer<K>): Long {
        lock.lockWrite {
            val recid = preallocate2()
            records.put(recid, record)
            return recid
        }
    }

    override fun <K> update(recid: Long, serializer: Serializer<K>, newRecord: K?) {
        lock.lockWrite {
            if(!records.containsKey(recid))
                throw DBException.RecidNotFound()
            records.put(recid, newRecord?:NULL_RECORD)
        }

    }

    override fun <K> compareAndUpdate(recid: Long, serializer: Serializer<K>, expectedOldRecord: K?, newRecord: K?): Boolean {
        lock.lockWrite {
            val oldRec = check(records.get(recid))

            if(oldRec!=expectedOldRecord)
                return false
            records.put(recid, newRecord?:NULL_RECORD)
            return true
        }
    }

    override fun <K> compareAndDelete(recid: Long, serializer: Serializer<K>, expectedOldRecord: K?): Boolean {
        lock.lockWrite {
            val oldRec = check(records.get(recid))

            if(oldRec!=expectedOldRecord)
                return false
            records.remove(recid)
            freeRecids.add(recid)
            return true
        }
    }

    override fun <K> delete(recid: Long, serializer: Serializer<K>) {
        lock.lockWrite {
            records.remove(recid) ?: throw DBException.RecidNotFound()
            freeRecids.add(recid)
        }
    }

    override fun verify() {
    }

    override fun commit() {
    }

    override val isThreadSafe: Boolean = lock!=null

    override fun compact() {
    }


    override fun close() {
    }

}