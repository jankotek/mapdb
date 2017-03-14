package org.mapdb

import org.eclipse.collections.api.list.primitive.MutableLongList
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.mapdb.DataIO.parity1Get
import org.mapdb.DataIO.parity1Set
import org.mapdb.StoreDirectJava.RECID_LONG_STACK
import org.mapdb.volume.Volume
import org.mapdb.volume.VolumeFactory
import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Common utils for StoreDirect, StoreWAL and StoreCached
 */
abstract class StoreDirectAbstract(
        val file:String?,
        val volumeFactory: VolumeFactory,
        override val isThreadSafe:Boolean,
        val concShift:Int,
        val fileDeleteAfterClose:Boolean,
        val checksum:Boolean,
        val checksumHeader:Boolean,
        val checksumHeaderBypass:Boolean
        ):Store{

    protected abstract val volume: Volume
    protected abstract val headVol: Volume

    protected val segmentCount = 1.shl(concShift)
    protected val segmentMask = 1L.shl(concShift)-1
    protected val locks = Utils.newReadWriteSegmentedLock(isThreadSafe, segmentCount)
    protected val structuralLock = Utils.newLock(isThreadSafe)
    protected val compactionLock = Utils.newReadWriteLock(isThreadSafe)

    protected val volumeExistsAtStart = volumeFactory.exists(file)

    //TODO PERF indexPages are synchronized writes are protected by structural lock, but should it be read under locks?
    protected val indexPages = if(isThreadSafe) ThreadSafeLongArrayList() else LongArrayList()

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

    protected val closed = AtomicBoolean(false)

    override val isClosed:Boolean
        get() = closed.get()

    protected fun assertNotClosed(){
        if(closed.get())
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

    protected fun fileHeaderCheck(){
        val header = headVol.getLong(0)
        if(header.ushr(7*8)!=CC.FILE_HEADER){
            throw DBException.WrongFormat("Wrong file header, not MapDB file")
        }
        if(header.ushr(6*8) and 0xFF!=CC.FILE_TYPE_STOREDIRECT)
            throw DBException.WrongFormat("Wrong file header, not StoreDirect file")

        if(header.ushr(4*8) and 0xFFFF != 0L){
            throw DBException.NewMapDBFormat("Store was created with newer version of MapDB, some new features are not supported")
        }

        if(headVol.getInt(20)!=calculateHeaderChecksum()) {
            val msg = "Header checksum broken. Store was not closed correctly and might be corrupted."
            if(checksumHeaderBypass)
                Utils.LOG.warning{msg+". Check was bypassed with `DBMaker.checksumHeaderBypass()`. Recover your data!"}
            else
                throw DBException.BrokenHeaderChecksum(msg+" Use `DBMaker.checksumHeaderBypass()` to recover your data. Use clean shutdown or enable transactions to protect the store in the future.")
        }

        if(header.toInt().ushr(CC.FEAT_ENCRYPT_SHIFT) and CC.FEAT_ENCRYPT_MASK!=0)
            throw DBException.WrongConfiguration("Store is encrypted, but no encryption method was provided")

        //fails if checksum is enabled, but not in header
        val checksumFeature = header.toInt().ushr(CC.FEAT_CHECKSUM_SHIFT) and CC.FEAT_CHECKSUM_MASK
        if(checksumFeature==0 && checksum)
            throw DBException.WrongConfiguration("Store was created without checksum, but checksum is enabled in configuration")
        if(checksumFeature==1 && !checksum)
            throw DBException.WrongConfiguration("Store was created without checksum, but checksum is not enabled in configuration")
        if(checksumFeature>1){
            throw DBException.NewMapDBFormat("This version of MapDB does not support new checksum type used in store")
        }
        if(checksumFeature!=0 && this is StoreWAL)
            throw DBException.WrongConfiguration("StoreWAL does not support checksum")
        val checksumFromHeader = headVol.getLong(8)
        if(checksum){
            if(calculateChecksum()!=checksumFromHeader)
                throw DBException.DataCorruption("Wrong checksum in header")
        }else{
            if(1L!=checksumFromHeader)
                throw DBException.DataCorruption("Checksum is disabled, expected 1, got something else")
        }

        val featBits = headVol.getInt(4)
        if(featBits.ushr(3)!=0)
            throw DBException.NewMapDBFormat("Header indicates feature not supported in older version of MapDB")
        val storeFeatBits = headVol.getInt(16)
        if(storeFeatBits.ushr(1)!=0)
            throw DBException.NewMapDBFormat("Store header indicates feature not supported in older version of MapDB")

        if(storeFeatBits and 1 ==0 && checksumHeader)
            throw DBException.WrongConfiguration("Store header checksum, disabled in store, but enabled in configuration")

        if(storeFeatBits and 1 ==1 && !checksumHeader)
            throw DBException.WrongConfiguration("Store header checksum enabled in store, but disabled in configuration")
    }

    protected fun fileHeaderCompose():Long{
        val checksumFlag: Long = if(checksum)1L.shl(CC.FEAT_CHECKSUM_SHIFT) else 0
        return CC.FILE_HEADER.shl(7*8) + CC.FILE_TYPE_STOREDIRECT.shl(6*8) + checksumFlag
    }


    fun storeHeaderCompose(): Int {
        return 0 +
               if(checksumHeader) 1 else 0
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

    protected fun <R> deserialize(serializer: Serializer<R>, di: DataInput2, size: Long, recid:Long): R? {
        assert(serializer.isQuick() || di is DataInput2.ByteArray)
        assert(serializer.isQuick() || locks==null || recid<0 || !locks.isReadLockedByCurrentThread(recidToSegment(recid)))
        try{
            val ret = serializer.deserialize(di, size.toInt());
            return ret
            //TODO assert number of bytes read
            //TODO wrap di, if untrusted serializer
        }catch(e: IOException){
            throw DBException.SerializationError(e)
        }
    }

    protected fun <R> serialize(record: R?, serializer:Serializer<R>, recid:Long):DataOutput2?{
        if(record == null)
            return null;
        assert(serializer.isQuick() || locks==null || recid<0 || !locks.isReadLockedByCurrentThread(recidToSegment(recid)))
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
            return parity1Get(reusedRecid).ushr(1)
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
            val reusedDataOffset2 = parity1Get(reusedDataOffset).shl(3)
            if(CC.ZEROS)
                volume.assertZeroes(reusedDataOffset2, reusedDataOffset2+size)
            if(CC.ASSERT && reusedDataOffset2%16!=0L)
                throw DBException.DataCorruption("wrong offset")

            freeSizeIncrement(-size.toLong())
            return reusedDataOffset2
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

        //offset is multiple of 16, 4 bits are unnecessary, save 3 bits, use 1 bit for parity
        val offset2 = parity1Set(offset.ushr(3))
        longStackPut(longStackMasterLinkOffset(size), offset2, recursive);
    }

    protected fun releaseRecid(recid:Long){
        longStackPut(
                StoreDirectJava.RECID_LONG_STACK,
                parity1Set(recid.shl(1)),
                false)
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

    fun calculateChecksum():Long {
        var checksum = volume.getLong(0) + volume.hash(16, fileTail - 16, 0L)
        if(checksum==0L||checksum==1L)
            checksum=2
        return checksum
    }

    fun calculateHeaderChecksum():Int{
        if(checksumHeader.not())
            return 0
        var c = StoreDirectJava.HEAD_CHECKSUM_SEED
        for(offset in 24 until StoreDirectJava.HEAD_END step 4)
            c+=headVol.getInt(offset)
        return c
    }

}
