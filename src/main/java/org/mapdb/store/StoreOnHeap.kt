package org.mapdb.store

import org.eclipse.collections.impl.factory.primitive.LongLists
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap
import org.mapdb.DBException
import org.mapdb.ser.Serializer
import org.mapdb.ser.Serializers
import org.mapdb.util.lockRead
import org.mapdb.util.lockWrite
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

class StoreOnHeap(
        override val isThreadSafe:Boolean=true
):MutableStore{

    protected val NULL_RECORD = Pair(null, null)


    protected val lock: ReadWriteLock? = if(isThreadSafe) ReentrantReadWriteLock() else null

    protected val records: LongObjectHashMap<Pair<Any?,Serializer<Any>?>> = LongObjectHashMap.newMap()

    protected val freeRecids = LongLists.mutable.empty()

    protected var maxRecid:Long = 0;


    protected fun check(value:Pair<Any?,Serializer<*>?>?):Any?{
        return if(value===NULL_RECORD) null
            else value?.first ?:throw DBException.RecidNotFound()
    }

    override fun <K> get(recid: Long, serializer: Serializer<K>): K? {
        lock.lockRead {
            return check(records.get(recid)) as K?
        }
    }



    override fun getAll(consumer: (Long, ByteArray?) -> Unit) {
        lock.lockRead {
            val recids = records.keySet().toSortedArray()
            for(recid in recids){
                val v = records.get(recid)!!
                val data = if(v === NULL_RECORD) null else Serializers.serializeToByteArray(v.first!!, v.second!!)
                consumer(recid, data )
            }
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

    override fun <K> put(record: K?, serializer: Serializer<K>): Long {
        lock.lockWrite {
            val recid = preallocate2()
            records.put(recid, Pair(record, serializer as Serializer<Any>))
            return recid
        }
    }

    override fun <K> update(recid: Long, serializer: Serializer<K>, newRecord: K?) {
        lock.lockWrite {
            if(!records.containsKey(recid))
                throw DBException.RecidNotFound()
            val newVal =
                    if(newRecord==null) NULL_RECORD
                    else Pair(newRecord,serializer as Serializer<Any>)
            records.put(recid, newVal)
        }
    }


    override fun <K> updateAtomic(recid: Long, serializer: Serializer<K>, m: (K?) -> K?) {
        lock.lockWrite {
            if(!records.containsKey(recid))
                throw DBException.RecidNotFound()
            val oldRec = check(records.get(recid)) as K?
            val newRecord = m(oldRec);

            val newVal =
                    if(newRecord==null) NULL_RECORD
                    else Pair(newRecord,serializer as Serializer<Any>)
            records.put(recid, newVal)
        }

    }

    override fun <K> compareAndUpdate(recid: Long, serializer: Serializer<K>, expectedOldRecord: K?, newRecord: K?): Boolean {
        lock.lockWrite {
            val oldRec = check(records.get(recid))

            if(!serializer.equals(oldRec as K,expectedOldRecord))
                return false
            val newVal =
                    if(newRecord==null)NULL_RECORD
                    else Pair(newRecord,serializer as Serializer<Any>)
            records.put(recid, newVal)
            return true
        }
    }

    override fun <K> compareAndDelete(recid: Long, serializer: Serializer<K>, expectedOldRecord: K?): Boolean {
        lock.lockWrite {
            val oldRec = check(records.get(recid))

            if(!serializer.equals(oldRec as K,expectedOldRecord))
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


    override fun <E> getAndDelete(recid: Long, serializer: Serializer<E>): E? {
        lock.lockWrite {
            //TODO optimize
            val ret = get(recid,serializer)
            delete(recid, serializer)
            return ret
        }
    }


    override fun verify() {
    }

    override fun commit() {
    }

    override fun compact() {
    }


    override fun close() {
    }

    override fun isEmpty(): Boolean {
        return maxRecid == 0L
    }

}