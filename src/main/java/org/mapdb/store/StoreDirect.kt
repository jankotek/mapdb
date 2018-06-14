package org.mapdb.store

import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap
import org.mapdb.DBException
import org.mapdb.io.DataInput2ByteArray
import org.mapdb.io.DataOutput2ByteArray
import org.mapdb.serializer.Serializer
import org.mapdb.serializer.Serializers
import org.mapdb.util.*
import java.nio.ByteBuffer
import java.nio.MappedByteBuffer

class StoreDirect(
        private val b: ByteBuffer,
        override val isThreadSafe: Boolean = true
    ) :MutableStore{


    companion object {
        val blockSize:Long = 1024*1024
        val maskOffset =  0x0000FFFFFFFFFFFFL // 0x0000FFFFFFFFFFF0L
    }

    private val lock = newReadWriteLock(isThreadSafe)

    private val indexPages = LongLongHashMap()

    private var maxRecid:Long = 0L

    private var eof:Long = blockSize

    private val freeRecids = LongArrayList()

    /** release free space */
    private fun spaceRelease(offset:Long, size:Int){

    }


    init{
        //find max recid
        for(recid in 1L until blockSize/8){
            if(b.getLong(8*recid.toInt())!=0L)
                maxRecid = recid
        }
        //restore free recids
        for(recid in 1L .. maxRecid){
            if(b.getLong(8*recid.toInt())==0L)
                freeRecids.add(recid)
        }
    }

    /** allocate record of given size, returns offset */
    private fun spaceAllocate(size:Int):Long{
        lock.assertWriteLock()
        assert(size>=0)
        val ret = eof
        eof+=size
        return ret
    }


    private fun indexGet(recid:Long):Long{
        val indexValOffset = recid*8
        assert(indexValOffset< blockSize)
        return b.getLong(indexValOffset.toInt())
    }

    private fun indexUpdate(recid: Long, indexVal:Long){
        val indexValOffset = recid*8
        b.putLong(indexValOffset.toInt(), indexVal)
    }

    private fun indexVal(size: Int, offset: Long):Long{
        assert(size<256*256)
        assert(offset<Int.MAX_VALUE)
        return size.toLong().shl(48).or(offset)
    }

    private fun indexValToSize(indexVal:Long) = indexVal.ushr(48)

    private fun indexValToOffset(indexVal:Long) = indexVal.and(maskOffset)




    private fun recidAllocate(indexVal:Long):Long{
        lock.assertWriteLock()
        val recid =
                if(freeRecids.isEmpty) ++maxRecid
                else freeRecids.removeAtIndex(freeRecids.size()-1)

        assert(recid*8<blockSize)
        assert(b.getLong(8*recid.toInt())==0L)
        b.putLong(8*recid.toInt(), indexVal)
        return recid
    }

    private fun recidRelease(recid:Long){
        lock.assertWriteLock()
        assert(recid*8<blockSize)
        freeRecids.add(recid)
        b.putLong(8*recid.toInt(), 0L)
    }


    private fun read(size: Long, offset: Long): ByteArray {
        lock.assertReadLock()
        val bb = ByteArray(size.toInt())
        val b2 = b.duplicate()

        b2.position(offset.toInt())

        b2.get(bb)
        return bb
    }


    private fun write(offset: Long, bb: ByteArray?) {
        lock.assertWriteLock()
        val b2 = b.duplicate()
        b2.position(offset.toInt())
        b2.put(bb)
    }

    override fun <K> get(recid: Long, serializer: Serializer<K>): K? {
        lock.lockRead {
           if(recid>maxRecid)
               throw DBException.RecidNotFound()

            val indexVal = indexGet(recid)
            if(indexVal == 0L)
                throw DBException.RecidNotFound()
            if(indexVal==1L)
                return null
            val size = indexValToSize(indexVal)
            val offset = indexValToOffset(indexVal)

            val bb = read(size, offset)
            return serializer.deserialize(DataInput2ByteArray(bb))
        }
    }

    override fun getAll(consumer: (Long, ByteArray?) -> Unit) {
        //TODO exclude deleted
        lock.lockRead {
            for (recid in 1L..maxRecid) {
                val indexVal = indexGet(recid)
                if (indexVal == 0L) {
                    continue
                }
                if (indexVal == 1L) {
                    consumer(recid, null)
                    continue
                }

                val size = indexValToSize(indexVal)
                val offset = indexValToOffset(indexVal)
                val bb = read(size, offset)
                consumer(recid, bb)
            }
        }
    }


    override fun isEmpty(): Boolean {
        lock.lockRead {
            return maxRecid == 0L
        }
    }

    override fun <E> getAndDelete(recid: Long, serializer: Serializer<E>): E? {
        lock.lockWrite {
            val ret = get(recid, serializer)
            delete(recid, serializer)
            return ret
        }

    }


    override fun preallocate(): Long {
        lock.lockWrite {
            return recidAllocate(1L)
        }
    }

    override fun <K> put(record: K?, serializer: Serializer<K>): Long {
        if(record==null)
            return preallocate()

        val out = DataOutput2ByteArray()
        serializer.serialize(record, out)
        lock.lockWrite {
            val offset = spaceAllocate(out.pos)

            write(offset, out.buf)

            val recid = recidAllocate(indexVal(out.pos, offset))
            return recid
        }
    }

    override fun <K> update(recid: Long, serializer: Serializer<K>, newRecord: K?) {
        val bb = Serializers.serializeToByteArrayNullable(newRecord, serializer)

        lock.lockWrite {
            val indexVal = indexGet(recid)
            if(indexVal==0L)
                throw DBException.RecidNotFound()
            if(indexVal!=1L){
                //release old space
                spaceRelease(indexValToOffset(indexVal), indexValToSize(indexVal).toInt())
            }

            if(bb==null){
                //write null
                indexUpdate(recid, 1L)
            }else{
                //TODO handle empty records
                val offset = spaceAllocate(bb.size)

                write(offset, bb)
                indexUpdate(recid, indexVal(bb.size, offset))
            }
        }
    }


    override fun <K> updateAtomic(recid: Long, serializer: Serializer<K>, m: (K?) -> K?) {
        lock.lockWrite {
            updateWeak(recid, serializer, m)
        }
    }

    override fun <K> compareAndUpdate(recid: Long, serializer: Serializer<K>, expectedOldRecord: K?, newRecord: K?): Boolean {
        lock.lockWrite {
            val old = get(recid, serializer)
            if(old!=expectedOldRecord)
                return false
            update(recid, serializer, newRecord)
            return true
        }
    }

    override fun <K> compareAndDelete(recid: Long, serializer: Serializer<K>, expectedOldRecord: K?): Boolean {
        lock.lockWrite {
            val old = get(recid, serializer)
            if(old!=expectedOldRecord)
                return false
            delete(recid, serializer)
            return true
        }
    }

    override fun <K> delete(recid: Long, serializer: Serializer<K>) {
        lock.lockWrite {
            val indexVal = indexGet(recid)
            if(indexVal==0L)
                throw DBException.RecidNotFound()

            recidRelease(recid)

            if(indexVal==1L)
                return //null

            val offset = indexValToOffset(indexVal)
            val size = indexValToSize(indexVal)
            spaceRelease(offset, size.toInt())
        }
    }

    override fun verify() {
    }

    override fun commit() {
        if(b is MappedByteBuffer)
            b.force()
    }

    override fun compact() {
    }

    override fun close() {

    }
}