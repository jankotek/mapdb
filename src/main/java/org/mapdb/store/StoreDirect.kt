package org.mapdb.store

import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap
import org.mapdb.DBException
import org.mapdb.io.DataIO
import org.mapdb.io.DataInput2ByteArray
import org.mapdb.ser.Serializer
import org.mapdb.ser.Serializers
import org.mapdb.util.*
import java.nio.ByteBuffer
import java.nio.MappedByteBuffer
import java.util.*

class StoreDirect(
        private val b: ByteBuffer,
        override val isThreadSafe: Boolean = true
    ) :MutableStore{


    companion object {
        val blockSize:Long = 1024*1024
        val maskOffset =  0x0000FFFFFFFFFFF0L

        val recordTypeMask = 0xEL // 1110

        val recordTypeSmall = 0x0L
        val recordtTypeLarge = 0x2L
        val recordTypeIndex = 0x6L

        val indexRecordSizeMask= 0xF0L

        val indexValNull:Long = 8L.shl(4).or(recordTypeIndex)

        val maxSmallRecordSize = 0xFFFFL
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
        eof+=DataIO.roundUp(size, 16)

        assert(ret%16==0L)
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
        assert(offset%16==0L)
        return size.toLong().shl(48).or(offset).or(recordTypeSmall)
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

            return when(indexVal.and(recordTypeMask)){
                recordTypeSmall -> getRecordSmall(indexVal, serializer)
                recordTypeIndex -> getRecordIndex(indexVal, serializer)
                recordtTypeLarge -> getRecordLarge(indexVal, serializer)

                else -> throw DBException.DataAssert("Unknown record type")
            }

        }
    }

    private fun <K> getRecordIndex(indexVal: Long, serializer: Serializer<K>): K? {
        val size = indexVal.and(indexRecordSizeMask).ushr(4).toInt()
        if(size==8)
            return null

        val bb = ByteArray(8)
        DataIO.putLong(bb, 0, indexVal)

        val bb2 = Arrays.copyOf(bb,size)
        return serializer.deserialize(DataInput2ByteArray(bb2))
    }

    private fun <K> getRecordLarge(indexVal:Long, serializer: Serializer<K>):K?  {
        val size = indexValToSize(indexVal)
        val offset = indexValToOffset(indexVal)

        val linkCount = b.getChar(offset.toInt()).toInt()
        assert(linkCount>0)
        assert(offset+size>=linkCount*8+8)
        val links = (0 until linkCount).map{i->
            val linkOffset = offset + 2 + i*8
            b.getLong(linkOffset.toInt())
        }.toLongArray()

        val bbSize = links.map{it.ushr(48)}.sum()
        val bb = ByteArray(bbSize.toInt())

        var bbPos = 0L
        var b2 = b.duplicate()
        for(link in links){
            val size = link.ushr(48)
            assert(size>0)
            val offset = link.and(maskOffset)

            b2.position(offset.toInt())
            b2.get(bb, bbPos.toInt(), size.toInt())

            bbPos+=size
        }
        assert(bbSize == bbPos)

        return serializer.deserialize(DataInput2ByteArray(bb))
    }
    private fun <K> getRecordSmall(indexVal:Long, serializer: Serializer<K>):K?  {
        val size = indexValToSize(indexVal)
        val offset = indexValToOffset(indexVal)

        val bb = read(size, offset)
        return serializer.deserialize(DataInput2ByteArray(bb))
    }

    override fun getAll(consumer: (Long, ByteArray?) -> Unit) {
        lock.lockRead {
            for (recid in 1L..maxRecid) {
                val indexVal = indexGet(recid)
                if (indexVal == 0L) {
                    continue
                }

                val bb = get(recid, Serializers.BYTE_ARRAY_NOSIZE) //TODO hack
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
            return recidAllocate(indexValNull)
        }
    }

    override fun <K> put(record: K?, serializer: Serializer<K>): Long {
        lock.lockWrite {
            val recid = preallocate()
            update(recid, serializer, record)
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

            if(bb==null) {
                //write null
                indexUpdate(recid, indexValNull)
            }else if(bb.size<8){
                val bb8 = Arrays.copyOf(bb, 8)
                val indexVal = DataIO.getLong(bb8, 0)
                        .or(bb.size.toLong().shl(4))
                        .or(recordTypeIndex)
                indexUpdate(recid, indexVal)
            }else if(bb.size<= maxSmallRecordSize){
                //small record

                val offset = spaceAllocate(bb.size)

                write(offset, bb)
                indexUpdate(recid, indexVal(bb.size, offset))
            }else{
                updateRecordLarge(bb, recid)
            }
        }
    }

    private fun updateRecordLarge(bb: ByteArray, recid: Long) {
        //large record
        val rootSize = maxSmallRecordSize.toInt()
        val rootOffset = spaceAllocate(rootSize) //TODO size allocation

        var bbPos = 0
        var count = 0
        while (bbPos < bb.size) {
            val partSize = Math.min(maxSmallRecordSize.toInt(), bb.size - bbPos)
            val partOffset = spaceAllocate(partSize.toInt())
            val b2 = b.duplicate()
            b2.position(partOffset.toInt())
            b2.put(bb, bbPos, partSize.toInt())

            val linkVal = partSize.toLong().shl(48).or(partOffset)
            b.putLong(rootOffset.toInt()+2 + count * 8, linkVal)

            count++
            bbPos += partSize
        }
        assert(bbPos == bb.size)
        assert(count <= 0xFF)
        b.putChar(rootOffset.toInt(), count.toChar())

        val indexVal = rootSize.toLong().shl(48).or(rootOffset).or(recordtTypeLarge)
        indexUpdate(recid, indexVal)
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

            when(indexVal.and(recordTypeMask)){
                recordTypeSmall -> deleteRecordSmall(indexVal)
                recordTypeIndex -> {} //no space to release
                recordtTypeLarge -> deleteRecordLarge(indexVal)
                else -> throw DBException.DataAssert("unknown record type")
            }
        }
    }

    private fun deleteRecordSmall(indexVal: Long){
        lock.assertWriteLock()
        val offset = indexValToOffset(indexVal)
        val size = indexValToSize(indexVal)
        assert(size>0)
        spaceRelease(offset, size.toInt())
    }

    private fun deleteRecordLarge(indexVal:Long){
        lock.assertWriteLock()
        val offset = indexValToOffset(indexVal)
        val size = indexValToSize(indexVal)

        val linkCount = b.getChar(offset.toInt()).toInt()
        assert(offset+size>=linkCount*8+8)
        for(i in 0 until linkCount){
            val linkOffset = offset + 2 + i*8
            val linkVal = b.getLong(linkOffset.toInt())
            deleteRecordSmall(linkVal)
        }

        deleteRecordSmall(indexVal)
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