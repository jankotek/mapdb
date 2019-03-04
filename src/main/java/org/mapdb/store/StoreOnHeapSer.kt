package org.mapdb.store

import org.eclipse.collections.impl.factory.primitive.LongLists
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap
import org.mapdb.DBException
import org.mapdb.io.DataInput2ByteArray
import org.mapdb.ser.Serializer
import org.mapdb.ser.Serializers
import org.mapdb.util.dataAssert
import org.mapdb.util.lockRead
import org.mapdb.util.lockWrite
import java.io.*
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

class StoreOnHeapSer(
        val file: File? = null,
        override val isThreadSafe:Boolean=true
):MutableStore{

    protected val NULL_RECORD = byteArrayOf(1,2,3)


    protected val lock: ReadWriteLock? = if(isThreadSafe) ReentrantReadWriteLock() else null

    protected val records: LongObjectHashMap<ByteArray> = LongObjectHashMap.newMap()

    protected val freeRecids = LongLists.mutable.empty()

    protected var maxRecid:Long = 0;

    init{
        if(file!=null && file.exists()){
            //load from file
            DataInputStream(DataInputStream(BufferedInputStream(file.inputStream()))).use { s ->
                maxRecid = s.readLong()
                val freeRecidCount = s.readInt()
                val recCount = s.readInt()

                for(i in 0 until freeRecidCount){
                    val recid = s.readLong()
                    freeRecids.add(recid)
                }


                for(i in 0 until recCount){
                    val recid = s.readLong() //TODO use recid with parity
                     val size = s.readInt()
                    val ba = ByteArray(size)
                    s.readFully(ba)
                    val oldval = records.put(recid, ba)
                    dataAssert(oldval==null)
                }
            }

        }
    }

    protected fun <E> check(value:ByteArray?, serializer:Serializer<E>):E?{
        return when {
            value===NULL_RECORD -> null
            value==null -> throw DBException.RecidNotFound()
            else -> serializer.deserialize(DataInput2ByteArray(value))
        }
    }

    override fun <K> get(recid: Long, serializer: Serializer<K>): K? {
        lock.lockRead {
            return check(records.get(recid), serializer)
        }
    }



    override fun getAll(consumer: (Long, ByteArray?) -> Unit) {
        lock.lockRead {
            val recids = records.keySet().toSortedArray()
            for(recid in recids){
                val v = records.get(recid)!!
                val data = if(v === NULL_RECORD) null else v
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
        val data =
                if(record==null) NULL_RECORD
                else Serializers.serializeToByteArray(record, serializer)
        lock.lockWrite {
            val recid = preallocate2()
            records.put(recid, data)
            return recid
        }
    }

    override fun <K> update(recid: Long, serializer: Serializer<K>, newRecord: K?) {
        val newVal =
                if(newRecord==null) NULL_RECORD
                else Serializers.serializeToByteArray(newRecord, serializer)
        lock.lockWrite {
            if(!records.containsKey(recid))
                throw DBException.RecidNotFound()
            records.put(recid, newVal)
        }
    }


    override fun <K> updateAtomic(recid: Long, serializer: Serializer<K>, m: (K?) -> K?) {
        lock.lockWrite {
            if(!records.containsKey(recid))
                throw DBException.RecidNotFound()

            val oldRec = check(records.get(recid), serializer)
            val newRec = m(oldRec)
            val newVal =
                    if(newRec==null) NULL_RECORD
                    else Serializers.serializeToByteArray(newRec, serializer)

            records.put(recid, newVal)
        }
    }


    override fun <K> compareAndUpdate(recid: Long, serializer: Serializer<K>, expectedOldRecord: K?, newRecord: K?): Boolean {
        val newVal =
                if(newRecord==null) NULL_RECORD
                else Serializers.serializeToByteArray(newRecord, serializer)

        lock.lockWrite {
            val oldRec = check(records.get(recid), serializer)

            if(!serializer.equals(oldRec,expectedOldRecord))
                return false
            records.put(recid, newVal)
            return true
        }
    }

    override fun <K> compareAndDelete(recid: Long, serializer: Serializer<K>, expectedOldRecord: K?): Boolean {
        lock.lockWrite {
            val oldRec = check(records.get(recid), serializer)

            if(!serializer.equals(oldRec,expectedOldRecord))
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
        if(file==null)
            return
        lock.lockRead {
            DataOutputStream(BufferedOutputStream(file.outputStream())).use { s ->
                s.writeLong(maxRecid)
                s.writeInt(freeRecids.size())
                s.writeInt(records.size())

                freeRecids.forEach{recid->
                    s.writeLong(recid)
                }

                records.forEachKeyValue{recid,ba->
                    s.writeLong(recid)
                    s.writeInt(ba.size)
                    s.write(ba)
                }
            }
        }
    }

    override fun compact() {
    }


    override fun close() {
    }

    override fun isEmpty(): Boolean {
        return maxRecid == 0L
    }


}