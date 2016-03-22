package org.mapdb

import org.eclipse.collections.api.list.primitive.MutableLongList
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.mapdb.volume.Volume
import org.mapdb.volume.VolumeFactory
import java.io.IOException
import java.util.concurrent.locks.ReadWriteLock
import org.mapdb.StoreDirectJava.*

/**
 * Common utils for StoreDirect, StoreWAL and StoreCached
 */
abstract class StoreDirectAbstract(
        val file:String?,
        val volumeFactory: VolumeFactory,
        override val isThreadSafe:Boolean,
        val concShift:Int
        ):Store{

    protected abstract val volume: Volume
    protected abstract val headVol: Volume

    protected val segmentCount = 1.shl(concShift)
    protected val segmentMask = 1L.shl(concShift)-1
    protected val locks:Array<ReadWriteLock?> = Array(segmentCount, {Utils.newReadWriteLock(isThreadSafe)})
    protected val structuralLock = Utils.newLock(isThreadSafe)

    protected val volumeExistsAtStart = volumeFactory.exists(file)

    //TODO writes are protected by structural lock, but should it be reads under locks?
    protected val indexPages = LongArrayList()

    protected fun recidToOffset(recid2:Long):Long{
        var recid = recid2-1; //normalize recid so it starts from zero
        if(recid< StoreDirectJava.RECIDS_PER_ZERO_INDEX_PAGE){
            //zero index page
            return StoreDirectJava.HEAD_END + 16 + recid*8
        }
        //strip zero index page
        recid -= StoreDirectJava.RECIDS_PER_ZERO_INDEX_PAGE
        val pageNum = recid/ StoreDirectJava.RECIDS_PER_INDEX_PAGE
        return indexPages.get(pageNum.toInt()) + 16 + ((recid)% StoreDirectJava.RECIDS_PER_INDEX_PAGE)*8
    }

    protected @Volatile var closed = false;

    override val isClosed:Boolean
        get() = closed

    protected fun assertNotClosed(){
        if(closed)
            throw IllegalAccessError("Store was closed");
    }


    /** end of last record */
    protected var dataTail: Long
        get() = DataIO.parity4Get(headVol.getLong(StoreDirectJava.DATA_TAIL_OFFSET))
        set(v:Long){
            if(CC.ASSERT && (v%16)!=0L)
                throw DBException.DataCorruption("unaligned data tail")
            if(CC.ASSERT)
                Utils.assertLocked(structuralLock)
            headVol.putLong(StoreDirectJava.DATA_TAIL_OFFSET, DataIO.parity4Set(v))
        }

    /** maximal allocated recid */
    protected var maxRecid: Long
        get() = DataIO.parity3Get(headVol.getLong(StoreDirectJava.INDEX_TAIL_OFFSET)).ushr(3)
        set(v:Long){
            if(CC.ASSERT)
                Utils.assertLocked(structuralLock)
            headVol.putLong(StoreDirectJava.INDEX_TAIL_OFFSET, DataIO.parity3Set(v.shl(3)))
        }

    /** end of file (last allocated page) */
    //TODO add fileSize into Store interface, make this var protected
    internal var fileTail: Long
        get() = DataIO.parity16Get(headVol.getLong(StoreDirectJava.FILE_TAIL_OFFSET))
        set(v:Long){
            if(CC.ASSERT)
                Utils.assertLocked(structuralLock)
            headVol.putLong(StoreDirectJava.FILE_TAIL_OFFSET, DataIO.parity16Set(v))
        }


    abstract protected fun getIndexVal(recid:Long):Long;

    abstract protected fun setIndexVal(recid:Long, value:Long)

    protected fun loadIndexPages(indexPages: MutableLongList){
        //load index pages
        var indexPagePointerOffset = StoreDirectJava.ZERO_PAGE_LINK;
        while (true) {
            val nextPage = DataIO.parity16Get(volume.getLong(indexPagePointerOffset))
            if (nextPage == 0L)
                break;
            if (CC.ASSERT && nextPage % CC.PAGE_SIZE != 0L)
                throw DBException.DataCorruption("wrong page pointer")
            indexPages.add(nextPage)
            indexPagePointerOffset = nextPage + 8
        }

    }

    protected fun indexValCompose(size:Long,
                                  offset:Long,
                                  linked:Int,
                                  unused:Int,
                                  archive:Int
            ):Long{

        if(CC.ASSERT && size<0 || size>0xFFFF)
            throw AssertionError()

        if(CC.ASSERT && (offset%16) != 0L)
            throw DBException.DataCorruption("unaligned offset")

        if(CC.ASSERT && (offset and StoreDirectJava.MOFFSET) != offset)
            throw DBException.DataCorruption("unaligned offset")


        if(CC.ASSERT && (linked in 0..1).not())
            throw AssertionError()
        if(CC.ASSERT && (archive in 0..1).not())
            throw AssertionError()
        if(CC.ASSERT && (unused in 0..1).not())
            throw AssertionError()

        return size.shl(48) + offset + linked* StoreDirectJava.MLINKED + unused* StoreDirectJava.MUNUSED + archive* StoreDirectJava.MARCHIVE
    }

    protected fun indexValFlagLinked(indexValue:Long):Boolean{
        return indexValue and StoreDirectJava.MLINKED != 0L
    }

    protected fun indexValFlagUnused(indexValue:Long):Boolean{
        return indexValue and StoreDirectJava.MUNUSED != 0L
    }

    protected fun indexValFlagArchive(indexValue:Long):Boolean{
        return indexValue and StoreDirectJava.MARCHIVE != 0L
    }


    protected fun recidToSegment(recid:Long):Int{
        return (recid and segmentMask).toInt()
    }

    protected fun <R> deserialize(serializer: Serializer<R>, di: DataInput2, size: Long): R? {
        try{
            val ret = serializer.deserialize(di, size.toInt());
            return ret
            //TODO assert number of bytes read
            //TODO wrap di, if untrusted serializer
        }catch(e: IOException){
            throw DBException.SerializationError(e)
        }
    }

    protected fun <R> serialize(record: R?, serializer:Serializer<R>):DataOutput2?{
        if(record == null)
            return null;
        try {
            val out = DataOutput2()
            serializer.serialize(out, record);
            return out;
        }catch(e: IOException){
            throw DBException.SerializationError(e)
        }
    }


    protected fun allocateRecid():Long{
        if(CC.ASSERT)
            Utils.assertLocked(structuralLock)

        val reusedRecid = longStackTake(RECID_LONG_STACK,false)
        if(reusedRecid!=0L){
            //TODO ensure old value is zero
            return reusedRecid
        }

        val maxRecid2 = maxRecid;

        val maxRecidOffset = recidToOffset(maxRecid2);

        // check if maxRecid is last on its index page
        if(maxRecidOffset % CC.PAGE_SIZE == CC.PAGE_SIZE-8){
            //yes, we can not increment recid without allocating new index page
            allocateNewIndexPage()
        }
        // increment maximal recid
        val ret = maxRecid2+1;
        maxRecid = ret;
        if(CC.ZEROS && volume.getLong(recidToOffset(ret))!=0L)
            throw AssertionError();
        return ret;
    }

    abstract protected fun allocateNewIndexPage():Long

    protected fun allocateData(size:Int, recursive:Boolean):Long{
        if(CC.ASSERT)
            Utils.assertLocked(structuralLock)

        if(CC.ASSERT && size> StoreDirectJava.MAX_RECORD_SIZE)
            throw AssertionError()
        if(CC.ASSERT && size<=0)
            throw AssertionError()
        if(CC.ASSERT && size%16!=0)
            throw AssertionError()


        val reusedDataOffset = if(recursive) 0L else
            longStackTake(longStackMasterLinkOffset(size.toLong()), recursive)
        if(reusedDataOffset!=0L){
            if(CC.ZEROS)
                volume.assertZeroes(reusedDataOffset, reusedDataOffset+size)
            if(CC.ASSERT && reusedDataOffset%16!=0L)
                throw DBException.DataCorruption("wrong offset")

            freeSizeIncrement(-size.toLong())
            return reusedDataOffset
        }

        val dataTail2 = dataTail;

        //no data were allocated yet
        if(dataTail2==0L){
            //create new page and return it
            val page = allocateNewPage();
            dataTail = page+size
            if(CC.ZEROS)
                volume.assertZeroes(page, page+size)
            if(CC.ASSERT && page%16!=0L)
                throw DBException.DataCorruption("wrong offset")
            return page;
        }

        //is there enough space on current page?
        if((dataTail2 % CC.PAGE_SIZE) + size <= CC.PAGE_SIZE) {
            //yes, so just increment data tail and return
            dataTail =
                    //check for case when page is completely filled
                    if((dataTail2+size)%CC.PAGE_SIZE==0L)
                        0L //in that case reset dataTail
                    else
                        dataTail2+size; //still space on current page, increment data tail

            if(CC.ZEROS)
                volume.assertZeroes(dataTail2, dataTail2+size)
            if(CC.ASSERT && dataTail2%16!=0L)
                throw DBException.DataCorruption("wrong offset")
            return dataTail2
        }

        // There is not enough space on current page to fit this record.
        // Must start new page
        // reset the dataTail, that will force new page creation
        dataTail = 0

        //and mark remaining space on old page as free
        val remSize = CC.PAGE_SIZE - (dataTail2 % CC.PAGE_SIZE)
        if(remSize!=0L){
            releaseData(remSize, dataTail2, recursive)
        }
        //now start new allocation on fresh page
        return allocateData(size, recursive);
    }


    protected fun releaseData(size:Long, offset:Long, recursive:Boolean){
        if(CC.ASSERT)
            Utils.assertLocked(structuralLock)

        if(CC.ASSERT && size%16!=0L)
            throw AssertionError()
        if(CC.ASSERT && size> StoreDirectJava.MAX_RECORD_SIZE)
            throw AssertionError()

        if(CC.ZEROS)
            volume.assertZeroes(offset, offset+size)

        freeSizeIncrement(size)

        longStackPut(longStackMasterLinkOffset(size), offset, recursive);
    }

    protected fun releaseRecid(recid:Long){
        longStackPut(StoreDirectJava.RECID_LONG_STACK, recid, false)
    }

    abstract protected fun freeSizeIncrement(increment: Long)

    abstract protected fun longStackPut(masterLinkOffset:Long, value:Long, recursive:Boolean)

    abstract protected fun longStackTake(masterLinkOffset:Long, recursive:Boolean):Long


    protected fun longStackMasterLinkOffset(size: Long): Long {
        if (CC.ASSERT && size % 16 != 0L)
            throw AssertionError()
        if(CC.ASSERT && size> StoreDirectJava.MAX_RECORD_SIZE)
            throw AssertionError()
        return size / 2 + StoreDirectJava.RECID_LONG_STACK // really is size*8/16
    }

    abstract protected fun allocateNewPage():Long

}
