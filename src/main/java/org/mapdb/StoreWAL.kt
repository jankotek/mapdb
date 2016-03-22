package org.mapdb

import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap
import org.mapdb.volume.ReadOnlyVolume
import org.mapdb.volume.SingleByteArrayVol
import org.mapdb.volume.Volume
import org.mapdb.volume.VolumeFactory
import org.mapdb.DataIO.*
import org.mapdb.StoreDirectJava.*

/**
 * StoreDirect with write ahead log
 */
class StoreWAL(
        file:String?,
        volumeFactory: VolumeFactory,
        isThreadSafe:Boolean,
        concShift:Int,
        allocateStartSize:Long
):StoreDirectAbstract(
        file=file,
        volumeFactory=volumeFactory,
        isThreadSafe = isThreadSafe,
        concShift =  concShift
){

    companion object{
        @JvmStatic fun make(
                file:String?= null,
                volumeFactory: VolumeFactory = if(file==null) CC.DEFAULT_MEMORY_VOLUME_FACTORY else CC.DEFAULT_FILE_VOLUME_FACTORY,
                isThreadSafe:Boolean = true,
                concShift:Int = 4,
                allocateStartSize: Long = 0L
        )=StoreWAL(
                file = file,
                volumeFactory = volumeFactory,
                isThreadSafe = isThreadSafe,
                concShift = concShift,
                allocateStartSize = allocateStartSize
        )
    }

    protected val realVolume: Volume = {
        volumeFactory.makeVolume(file, false, false, CC.PAGE_SHIFT,
                DataIO.roundUp(allocateStartSize, CC.PAGE_SIZE), false)
    }()

    override protected val volume: Volume = if(CC.ASSERT) ReadOnlyVolume(realVolume) else realVolume

    /** header is stored in-memory, so it can be rolled back */
    protected val headBytes = ByteArray(StoreDirectJava.HEAD_END.toInt())

    override protected val headVol = SingleByteArrayVol(headBytes)

    /** stack pages, key is offset, value is content */
    protected val cacheStacks = LongObjectHashMap<ByteArray>()

    /** modified indexVals, key is offset, value is indexValue */
    protected val cacheIndexVals = Array(segmentCount, { LongLongHashMap() })
    /** modified records, key is offset, value is WAL ID */
    protected val cacheRecords = Array(segmentCount, { LongLongHashMap() })


    protected val wal = WriteAheadLog(file+".wal")

    /** backup for `indexPages`, restored on rollback */
    protected var indexPagesBackup = longArrayOf();

    protected val allocatedPages = LongArrayList();


    init{
        Utils.lock(structuralLock) {
            if (!volumeExistsAtStart) {
                realVolume.ensureAvailable(CC.PAGE_SIZE)
                //TODO crash resistance while file is being created
                //initialize values
                volume.ensureAvailable(CC.PAGE_SIZE)
                dataTail = 0L
                maxRecid = 0L
                fileTail = CC.PAGE_SIZE

                //initialize long stack master links
                for (offset in StoreDirectJava.RECID_LONG_STACK until StoreDirectJava.HEAD_END step 8) {
                    headVol.putLong(offset, parity4Set(0L))
                }
                //initialize zero link from first page
                //this is outside header
                realVolume.putLong(StoreDirectJava.ZERO_PAGE_LINK, parity16Set(0L))

                //and write down everything
                realVolume.putData(0L, headBytes,0, headBytes.size)
                realVolume.sync()
            } else {
                loadIndexPages(indexPages)
                indexPagesBackup = indexPages.toArray()
            }
        }
    }


    override fun getIndexVal(recid: Long): Long {
        val segment = recidToSegment(recid)
        if(CC.ASSERT)
            Utils.assertReadLock(locks[segment])

        val indexOffset = recidToOffset(recid)
        var ret = cacheIndexVals[segment].get(indexOffset)
        if(ret==0L)
            ret = volume.getLong(indexOffset)

        return DataIO.parity1Get(ret)
    }

    override fun setIndexVal(recid: Long, value: Long) {
        val segment = recidToSegment(recid)
        if(CC.ASSERT)
            Utils.assertReadLock(locks[segment])

        val indexOffset = recidToOffset(recid)
        cacheIndexVals[segment].put(indexOffset, parity1Set(value))
    }


    override fun <R> compareAndSwap(recid: Long, expectedOldRecord: R?, newRecord: R?, serializer: Serializer<R>): Boolean {
        throw UnsupportedOperationException()
    }

    override fun <R> delete(recid: Long, serializer: Serializer<R>) {
        throw UnsupportedOperationException()
    }

    override fun preallocate(): Long {
        throw UnsupportedOperationException()
    }

    override fun <R> put(record: R?, serializer: Serializer<R>): Long {
        val di = serialize(record, serializer)

        assertNotClosed()
        val recid = Utils.lock(structuralLock){
            allocateRecid()
        }
        val indexOffset = recidToOffset(recid)
        val segment = recidToSegment(recid)
        Utils.lockWrite(locks[segment]) {
            if (di != null) {
                //allocate space
                val volOffset = Utils.lock(structuralLock) {
                    allocateData(roundUp(di.pos,16), false)
                }
                val walId = wal.walPutRecord(recid, di.buf, 0, di.pos)
                //TODO linked record
                cacheRecords[segment].put(volOffset, walId)
                val indexVal = indexValCompose(size=di.pos.toLong(), offset = volOffset, archive = 1, linked = 0, unused = 0)
                cacheIndexVals[segment].put(indexOffset, indexVal)
            }else{
                //null record
                val indexVal = indexValCompose(size=NULL_RECORD_SIZE, offset = 0L, archive = 1, linked = 0, unused = 0)
                cacheIndexVals[segment].put(indexOffset, indexVal)
            }
        }

        return recid
    }

    override fun <R> update(recid: Long, record: R?, serializer: Serializer<R>) {
        throw UnsupportedOperationException()
    }

    override fun <R> get(recid: Long, serializer: Serializer<R>): R? {
        val segment = recidToSegment(recid)
        Utils.lockRead(locks[segment]){
            val indexVal = getIndexVal(recid)
            val size = indexValToSize(indexVal)
            if(size==NULL_RECORD_SIZE)
                return null

            val volOffset = indexValToOffset(indexVal)

            val walId = cacheRecords[segment].get(volOffset)
            val di = if(walId!=0L){
                    //try to get from WAL
                    DataInput2.ByteArray(wal.walGetRecord(walId,recid))
                }else {
                    //not in WAL, load from volume
                    volume.getDataInput(volOffset,size.toInt())
                }
            return deserialize(serializer, di, size)
        }
    }

    override fun getAllRecids(): LongIterator {
        throw UnsupportedOperationException()
    }

    override fun verify() {

    }
    override fun close() {

    }

    override fun commit() {
        throw UnsupportedOperationException()
    }

    override fun compact() {
        throw UnsupportedOperationException()
    }


    override protected fun allocateNewPage():Long{
        if(CC.ASSERT)
            Utils.assertLocked(structuralLock)

        val eof = fileTail
        val newEof = eof + CC.PAGE_SIZE
        allocatedPages.add(eof)
        fileTail = newEof
        return eof
    }

    override protected fun allocateNewIndexPage():Long{
        if(CC.ASSERT)
            Utils.assertLocked(structuralLock)

        val indexPage = allocateNewPage();

        //update pointer to previous page
        val pagePointerOffset =
                if(indexPages.isEmpty)
                    ZERO_PAGE_LINK
                else
                    indexPages[indexPages.size()-1] + 8

//        if(CC.ASSERT && parity16Get(volume.getLong(pagePointerOffset))!=0L)
//            throw DBException.DataCorruption("index pointer not empty")

        wal.walPutLong(pagePointerOffset, parity16Get(indexPage))
        //volume.putLong(pagePointerOffset, parity16Set(indexPage))

        //add this page to list of pages
        indexPages.add(indexPage)

        //zero out pointer to next page with valid parity
        wal.walPutLong(indexPage+8, parity16Set(0))
        //volume.putLong(indexPage+8, parity16Set(0))
        return indexPage;
    }

    override fun freeSizeIncrement(increment: Long) {
        //TODO free size ignored
    }

    override fun longStackPut(masterLinkOffset: Long, value: Long, recursive: Boolean) {
        //TODO
    }

    override fun longStackTake(masterLinkOffset: Long, recursive: Boolean): Long {
        //TODO
        return 0L
    }


}