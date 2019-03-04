package org.mapdb.store

import org.eclipse.collections.impl.factory.primitive.LongLists
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap
import org.mapdb.DBException
import org.mapdb.io.DataIO
import org.mapdb.io.DataInput2ByteArray
import org.mapdb.ser.Serializer
import org.mapdb.ser.Serializers
import org.mapdb.util.lockRead
import org.mapdb.util.lockWrite
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.*
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

class StoreAppend(
        val file: Path,
        override val isThreadSafe:Boolean=true
    ):MutableStore{


    protected val c = FileChannel.open(file,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE,
            StandardOpenOption.READ)


    protected val lock: ReadWriteLock? = if(isThreadSafe) ReentrantReadWriteLock() else null

    /** key is recid, value is offset in channel */
    protected val offsets = LongLongHashMap()

    protected val freeRecids = LongLists.mutable.empty()

    protected var maxRecid:Long = 0

    init{
        val csize = c.size()
        if(csize==0L) {
            DataIO.writeFully(c, ByteBuffer.allocate(8))
        }else{
            var pos = 8L
            //read record positions
            while(pos<csize){
                val offset = pos
                val (recid, size) = readRecidSize(offset)
                pos+=12 + Math.max(size,0)

                offsets.put(recid,offset)
                maxRecid = Math.max(recid, maxRecid)
            }
            //restore recids
            for(recid in 1 until maxRecid){
                if(!offsets.containsKey(recid))
                    freeRecids.add(recid)
            }
        }
    }

    protected fun readRecidSize(offset:Long):Pair<Long,Int>{
        val b = ByteBuffer.allocate(12)
        DataIO.readFully(c, b, offset)
        val recid = DataIO.getLong(b.array(),0)
        val size = DataIO.getInt(b.array(),8)
        return Pair(recid, size)
    }

    protected fun readRecord(offset:Long, size:Int):ByteArray{
        val b = ByteBuffer.allocate(size)
        DataIO.readFully(c,b, offset)
        return b.array()
    }

    protected fun append(recid:Long, serialized:ByteArray?):Long{
        val offset = c.position()

        appendRecidSize(recid, serialized?.size ?: -1)

        if(serialized!=null) {
            val b2 = ByteBuffer.wrap(serialized)
            DataIO.writeFully(c, b2)
            //TODO merge into single write
        }

        offsets.put(recid, if(serialized==null) 0L else offset)
        return offset
    }

    private fun appendRecidSize(recid: Long, size: Int) {
        //write size and data
        val b1 = ByteBuffer.allocate(12)
        DataIO.putLong(b1.array(), 0, recid)
        DataIO.putInt(b1.array(), 8, size )
        DataIO.writeFully(c, b1)
    }


    override fun <K> get(recid: Long, serializer: Serializer<K>): K? {
        lock.lockRead {
            return getNoLock(recid, serializer)
        }
    }

    private fun <K> getNoLock(recid: Long, serializer: Serializer<K>): K? {
        val offset = offsets.getIfAbsent(recid, Long.MIN_VALUE)
        if (offset == Long.MIN_VALUE)
            throw DBException.RecidNotFound()

        if (offset == 0L)
            return null

        val (recid2, size) = readRecidSize(offset)
        assert(recid == recid2)
        val b = readRecord(offset + 12, size)

        val input = DataInput2ByteArray(b)
        return serializer.deserialize(input)
    }


    override fun getAll(consumer: (Long, ByteArray?) -> Unit) {
        lock.lockRead {
            val recids = offsets.keySet().toSortedArray()
            for(recid in recids){
                val offset = offsets.getOrThrow(recid)
                if(offset==0L){
                    consumer(recid, null)
                    continue
                }

                val (recid2,size) = readRecidSize(offset)
                assert(recid == recid2)

                val b = readRecord(offset+12, size)
                consumer(recid, b)
            }
        }
    }


    override fun preallocate(): Long {
       lock.lockWrite {
        val recid = preallocate2()
        append(recid, null)
        return recid
       }
    }


    protected fun preallocate2():Long{
        val recid =
                if(freeRecids.isEmpty) ++maxRecid
                else freeRecids.removeAtIndex(freeRecids.size()-1)

        return recid
    }

    override fun <K> put(record: K?, serializer: Serializer<K>): Long {
        val serialized = Serializers.serializeToByteArrayNullable(record, serializer)
        lock.lockWrite {
            val recid = preallocate2()
            append(recid, serialized)
            return recid
        }
    }

    override fun <K> update(recid: Long, serializer: Serializer<K>, newRecord: K?) {
        val serialized = Serializers.serializeToByteArrayNullable(newRecord, serializer)

        lock.lockWrite {
            if(!offsets.containsKey(recid))
                throw DBException.RecidNotFound()
            append(recid, serialized)
        }
    }

    override fun <K> updateAtomic(recid: Long, serializer: Serializer<K>, m: (K?) -> K?) {
        lock.lockWrite {
            val oldRec = getNoLock(recid, serializer)!!
            val newRec = m(oldRec)
            val serialized = Serializers.serializeToByteArrayNullable(newRec, serializer)
            append(recid, serialized)
        }
    }

    override fun <K> compareAndUpdate(recid: Long, serializer: Serializer<K>, expectedOldRecord: K?, newRecord: K?): Boolean {
        val expected = Serializers.serializeToByteArrayNullable(expectedOldRecord, serializer)
        val newRecord = Serializers.serializeToByteArrayNullable(newRecord, serializer)

        lock.lockWrite {
            val offset = offsets.getIfAbsent(recid, Long.MIN_VALUE)
            if(offset==Long.MIN_VALUE)
                throw DBException.RecidNotFound()


            //null record
            if(offset==0L){
                if(expected!=null)
                    return false
                append(recid, newRecord)
                return true
            }

            if(expected==null)
                return false

            val (recid2,size) = readRecidSize(offset)
            assert(recid == recid2)

            val old = readRecord(offset+12, size)

            if(!Arrays.equals(expected, old))
                return false

            append(recid, newRecord)
            return true
        }
    }

    override fun <K> compareAndDelete(recid: Long, serializer: Serializer<K>, expectedOldRecord: K?): Boolean {
        val expected = Serializers.serializeToByteArrayNullable(expectedOldRecord, serializer)

        lock.lockWrite {
            val offset = offsets.getIfAbsent(recid, Long.MIN_VALUE)
            if(offset==Long.MIN_VALUE)
                throw DBException.RecidNotFound()


            //null record
            if(offset==0L){
                if(expected!=null)
                    return false
                appendRecidSize(recid,-2)
                offsets.removeKey(recid)
                freeRecids.add(recid)
                return true
            }

            if(expected==null)
                return false

            val (recid2,size) = readRecidSize(offset)
            assert(recid == recid2)

            val old = readRecord(offset+12, size)

            if(!Arrays.equals(expected, old))
                return false

            appendRecidSize(recid,-2)
            offsets.removeKey(recid)
            freeRecids.add(recid)
            return true
        }
    }

    override fun <K> delete(recid: Long, serializer: Serializer<K>) {
        lock.lockWrite {
            if(!offsets.containsKey(recid))
                throw DBException.RecidNotFound()
            appendRecidSize(recid,-2)
            offsets.removeKey(recid)
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


    override fun close() {
        c.close()
    }
    override fun verify() {
    }

    override fun commit() {
        c.force(true)
    }

    override fun compact() {

    }

    override fun isEmpty(): Boolean {
        return maxRecid == 0L
    }

}