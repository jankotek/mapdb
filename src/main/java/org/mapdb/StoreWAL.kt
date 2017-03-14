package org.mapdb

import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap
import org.mapdb.DataIO.*
import org.mapdb.StoreDirectJava.*
import org.mapdb.volume.ReadOnlyVolume
import org.mapdb.volume.SingleByteArrayVol
import org.mapdb.volume.Volume
import org.mapdb.volume.VolumeFactory
import java.io.File
import java.util.*

/**
 * StoreDirect with write ahead log
 */
class StoreWAL(
        file:String?,
        volumeFactory: VolumeFactory,
        fileLockWait:Long,
        isThreadSafe:Boolean,
        concShift:Int,
        allocateIncrement:Long,
        allocateStartSize:Long,
        fileDeleteAfterClose:Boolean,
        fileDeleteAfterOpen:Boolean,
        checksum:Boolean,
        checksumHeader:Boolean,
        checksumHeaderBypass:Boolean
):StoreDirectAbstract(
        file=file,
        volumeFactory=volumeFactory,
        isThreadSafe = isThreadSafe,
        concShift =  concShift,
        fileDeleteAfterClose = fileDeleteAfterClose,
        checksum = checksum,
        checksumHeader = checksumHeader,
        checksumHeaderBypass = checksumHeaderBypass
), StoreTx{

    companion object{
        @JvmStatic fun make(
                file:String?= null,
                volumeFactory: VolumeFactory = if(file==null) CC.DEFAULT_MEMORY_VOLUME_FACTORY else CC.DEFAULT_FILE_VOLUME_FACTORY,
                fileLockWait:Long = 0L,
                isThreadSafe:Boolean = true,
                concShift:Int = CC.STORE_DIRECT_CONC_SHIFT,
                allocateIncrement: Long = CC.PAGE_SIZE,
                allocateStartSize: Long = 0L,
                fileDeleteAfterClose:Boolean = false,
                fileDeleteAfterOpen:Boolean = false,
                checksum:Boolean = false,
                checksumHeader:Boolean = true,
                checksumHeaderBypass:Boolean = false
        )=StoreWAL(
                file = file,
                volumeFactory = volumeFactory,
                fileLockWait = fileLockWait,
                isThreadSafe = isThreadSafe,
                concShift = concShift,
                allocateIncrement = allocateIncrement,
                allocateStartSize = allocateStartSize,
                fileDeleteAfterClose = fileDeleteAfterClose,
                fileDeleteAfterOpen = fileDeleteAfterOpen,
                checksum = checksum,
                checksumHeader = checksumHeader,
                checksumHeaderBypass = checksumHeaderBypass
        )

        @JvmStatic protected val TOMB1 = -1L;
    }

    protected val realVolume: Volume = {
        volumeFactory.makeVolume(
                file,
                false,
                fileLockWait,
                Math.max(CC.PAGE_SHIFT, DataIO.shift(allocateIncrement.toInt())),
                DataIO.roundUp(allocateStartSize, CC.PAGE_SIZE),
                false
        )
    }()

    override protected val volume: Volume = if(CC.ASSERT) ReadOnlyVolume(realVolume) else realVolume

    /** header is stored in-memory, so it can be rolled back */
    protected val headBytes = ByteArray(StoreDirectJava.HEAD_END.toInt())

    override protected val headVol = SingleByteArrayVol(headBytes)

    /** stack pages, key is offset, value is content */
    protected val cacheStacks = LongObjectHashMap<ByteArray>()

    /** modified indexVals, key is offset, value is indexValue */
    protected val cacheIndexVals = Array(segmentCount, { LongLongHashMap() })
    protected val cacheIndexLinks = LongLongHashMap()
    /** modified records, key is offset, value is WAL ID */
    protected val cacheRecords = Array(segmentCount, { LongLongHashMap() })


    protected val wal = WriteAheadLog(
            file,
            volumeFactory, //TODO PERF choose best file factory, mmap might not be fastest option
            0L,
            fileDeleteAfterOpen
    )

    /** backup for `indexPages`, restored on rollback */
    protected var indexPagesBackup = longArrayOf();

    protected val allocatedPages = LongArrayList();

    override val isReadOnly = false


    init{
        if(checksum)
            throw DBException.WrongConfiguration("StoreWAL does not support checksum yet") //TODO StoreWAL checksums
        Utils.lock(structuralLock) {
            if (!volumeExistsAtStart) {
                realVolume.ensureAvailable(CC.PAGE_SIZE)
                //TODO crash resistance while file is being created
                headVol.putLong(0L, fileHeaderCompose())
                headVol.putLong(8L, 1L)
                dataTail = 0L
                maxRecid = 0L
                fileTail = CC.PAGE_SIZE

                //initialize long stack master links
                for (offset in StoreDirectJava.RECID_LONG_STACK until StoreDirectJava.HEAD_END step 8) {
                    headVol.putLong(offset, parity4Set(0L))
                }
                headVol.putInt(16, storeHeaderCompose())
                DataIO.putInt(headBytes,20, calculateHeaderChecksum())

                //initialize zero link from first page
                //this is outside header
                realVolume.putLong(StoreDirectJava.ZERO_PAGE_LINK, parity16Set(0L))

                //and write down everything
                realVolume.putData(0L, headBytes,0, headBytes.size)
                realVolume.sync()
            } else {
                if(volume.length()<=0)
                    throw DBException.DataCorruption("File is empty")
                volume.getData(0, headBytes, 0, headBytes.size)
                fileHeaderCheck()

                loadIndexPages(indexPages)
                indexPagesBackup = indexPages.toArray()
            }
            if(file!=null && fileDeleteAfterOpen)
                File(file).delete()
        }
    }


    override fun getIndexVal(recid: Long): Long {
        val segment = recidToSegment(recid)
        if(CC.ASSERT)
            locks?.checkReadLocked(segment)

        val indexOffset = recidToOffset(recid)
        var ret = cacheIndexVals[segment].get(indexOffset)
        if(ret==0L)
            ret = volume.getLong(indexOffset)
        if(ret == 0L)
            throw DBException.GetVoid(recid)

        return DataIO.parity1Get(ret)
    }

    override fun setIndexVal(recid: Long, value: Long) {
        val segment = recidToSegment(recid)
        if(CC.ASSERT)
            locks?.checkReadLocked(segment)

        val indexOffset = recidToOffset(recid)
        cacheIndexVals[segment].put(indexOffset, parity1Set(value))
    }


    override fun <R> compareAndSwap(recid: Long, expectedOldRecord: R?, newRecord: R?, serializer: Serializer<R>): Boolean {
        assertNotClosed()
        Utils.lockWrite(locks,recidToSegment(recid)) {
            //compare old value
            val old = getProtected(recid, serializer)

            if (old === null && expectedOldRecord !== null)
                return false;
            if (old !== null && expectedOldRecord === null)
                return false;

            if (old !== expectedOldRecord && !serializer.equals(old!!, expectedOldRecord!!))
                return false

            val di = serialize(newRecord, serializer, recid)

            updateProtected(recid, di)
            return true;
        }
    }


    override fun <R> delete(recid: Long, serializer: Serializer<R>) {
        assertNotClosed()
        val segment = recidToSegment(recid)

        Utils.lockWrite(locks,segment) {
            val oldIndexVal = getIndexVal(recid);
            val oldSize = indexValToSize(oldIndexVal);
            if (oldSize == DELETED_RECORD_SIZE)
                throw DBException.GetVoid(recid)

            if (oldSize != NULL_RECORD_SIZE) {
                Utils.lock(structuralLock) {
                    if (indexValFlagLinked(oldIndexVal)) {
                        linkedRecordDelete(oldIndexVal,recid)
                    } else if(oldSize>5){
                        val oldOffset = indexValToOffset(oldIndexVal);
                        val sizeUp = roundUp(oldSize, 16)
                        //TODO clear into WAL
//                        if(CC.ZEROS)
//                            volume.clear(oldOffset,oldOffset+sizeUp)
                        releaseData(sizeUp, oldOffset, false)
                        cacheRecords[segment].remove(indexValToOffset(oldIndexVal));
                    }
                    releaseRecid(recid)
                }
            }
            setIndexVal(recid, indexValCompose(size = DELETED_RECORD_SIZE, offset = 0L, linked = 0, unused = 0, archive = 1))
        }
    }

    override fun preallocate(): Long {
        assertNotClosed()
        val recid = Utils.lock(structuralLock){
            allocateRecid()
        }
        Utils.lockWrite(locks,recidToSegment(recid)) {
//            if (CC.ASSERT) {
//                val oldVal = volume.getLong(recidToOffset(recid))
//                if(oldVal!=0L && indexValToSize(oldVal)!=DELETED_RECORD_SIZE)
//                    throw DBException.DataCorruption("old recid is not empty")
//            }

            //set allocated flag
            setIndexVal(recid, indexValCompose(size = NULL_RECORD_SIZE, offset = 0, linked = 0, unused = 1, archive = 1))
            return recid
        }
    }


    protected fun linkedRecordGet(indexValue:Long, recid:Long):ByteArray{
        if(CC.ASSERT && !indexValFlagLinked(indexValue))
            throw AssertionError("not linked record")

        val segment = recidToSegment(recid);
        val cacheRec = cacheRecords[segment]
        var b = ByteArray(128*1024)
        var bpos = 0
        var pointer = indexValue
        chunks@ while(true) {
            val isLinked = indexValFlagLinked(pointer);
            val nextPointerSize = if(isLinked)8 else 0; //last (non linked) chunk does not have a pointer
            val size = indexValToSize(pointer).toInt() - nextPointerSize
            val offset = indexValToOffset(pointer)

            //grow b if needed
            if(bpos+size>=b.size)
                b = Arrays.copyOf(b,b.size*2)

            val walId = cacheRec.get(offset)

            if(walId!=0L){
                //load from wal
                val ba = wal.walGetRecord(walId,recid)
                System.arraycopy(ba,nextPointerSize,b,bpos,size)
                bpos += size;

                if (!isLinked)
                    break@chunks

                pointer = parity3Get(getLong(ba,0))

            }else{
                //load from volume
                volume.getData(offset + nextPointerSize, b, bpos, size)
                bpos += size;

                if (!isLinked)
                    break@chunks

                pointer = parity3Get(volume.getLong(offset))
            }
        }

        return Arrays.copyOf(b,bpos) //TODO PERF this copy can be avoided with boundary checking DataInput
    }

    protected fun linkedRecordDelete(indexValue:Long, recid:Long){
        if(CC.ASSERT && !indexValFlagLinked(indexValue))
            throw AssertionError("not linked record")

        val segment = recidToSegment(recid);
        val cacheRec = cacheRecords[segment]

        var pointer = indexValue
        chunks@ while(pointer!=0L) {
            val isLinked = indexValFlagLinked(pointer);
            val size = indexValToSize(pointer)
            val offset = indexValToOffset(pointer)

            //read next pointer
            pointer = if(isLinked) {
                val walId = cacheRec.get(offset)
                if(walId==0L) {
                    parity3Get(volume.getLong(offset))
                }else{
                    val ba = wal.walGetRecord(walId, recid)
                    parity3Get(getLong(ba,0))
                }
            }else
                0L
            val sizeUp = roundUp(size,16);
            //TODO data clear
//            if(CC.ZEROS)
//                volume.clear(offset,offset+sizeUp)
            releaseData(sizeUp, offset, false);
            cacheRec.remove(offset)
        }
    }

    protected fun linkedRecordPut(output:ByteArray, size:Int, recid:Long):Long{
        val segment = recidToSegment(recid);
        val cacheRec = cacheRecords[segment]

        var remSize = size.toLong();
        //insert first non linked record
        var chunkSize:Long = Math.min(MAX_RECORD_SIZE, remSize);
        var chunkOffset = Utils.lock(structuralLock){
            allocateData(roundUp(chunkSize.toInt(),16), false)
        }
        var walId = wal.walPutRecord(recid,output, (remSize-chunkSize).toInt(), chunkSize.toInt())
        cacheRec.put(chunkOffset,walId)
        //volume.putData(chunkOffset, output, (remSize-chunkSize).toInt(), chunkSize.toInt())
        remSize-=chunkSize
        var isLinked = 0L // holds linked flag, last set is not linked, so initialized with zero

        // iterate in reverse order (from tail and from end of record)
        while(remSize>0){
            val prevLink = parity3Set((chunkSize+isLinked).shl(48) + chunkOffset + isLinked)
            isLinked = MLINKED;

            //allocate stuff
            chunkSize = Math.min(MAX_RECORD_SIZE - 8, remSize);
            chunkOffset = Utils.lock(structuralLock){
                allocateData(roundUp(chunkSize+8,16).toInt(), false)
            }

            //write link
//            volume.putLong(chunkOffset, prevLink)
            //and write data
            remSize-=chunkSize
            val ba = ByteArray(chunkSize.toInt()+8)
            putLong(ba,0,prevLink)
            System.arraycopy(output,remSize.toInt(), ba, 8 , chunkSize.toInt())
            walId = wal.walPutRecord(recid, ba, 0, ba.size)
            cacheRec.put(chunkOffset,walId)
//            volume.putData(chunkOffset+8, output, remSize.toInt(), chunkSize.toInt())
        }
        if(CC.ASSERT && remSize!=0L)
            throw AssertionError();
        return (chunkSize+8).shl(48) + chunkOffset + isLinked + MARCHIVE
    }

    override fun <R> put(record: R?, serializer: Serializer<R>): Long {
        val di = serialize(record, serializer, -1)

        assertNotClosed()
        val recid = Utils.lock(structuralLock){
            allocateRecid()
        }
        val indexOffset = recidToOffset(recid)
        val segment = recidToSegment(recid)
        Utils.lockWrite(locks,segment) {
            if (di != null) {
                if(di.pos==0) {
                    val indexVal = indexValCompose(size = 0, offset = 0L, archive = 1, linked = 0, unused = 0)
                    setIndexVal(recid, indexVal)
                }else if(di.pos<6){
                    val offset = DataIO.getLong(di.buf,0).ushr((7-di.pos)*8)
                    val indexVal = indexValCompose(size=di.pos.toLong(), offset = offset, archive = 1, linked = 0, unused = 0)
                    setIndexVal(recid,indexVal)
                }else if(di.pos>MAX_RECORD_SIZE){
                    //linked record
                    val indexVal = linkedRecordPut(di.buf,di.pos,recid)
                    setIndexVal(recid,indexVal)
                }else{
                    //allocate space
                    val volOffset = Utils.lock(structuralLock) {
                        allocateData(roundUp(di.pos, 16), false)
                    }
                    val walId = wal.walPutRecord(recid, di.buf, 0, di.pos)
                    cacheRecords[segment].put(volOffset, walId)
                    val indexVal = indexValCompose(size = di.pos.toLong(), offset = volOffset, archive = 1, linked = 0, unused = 0)
                    setIndexVal(recid,indexVal)
                }
            }else{
                //null record
                val indexVal = indexValCompose(size=NULL_RECORD_SIZE, offset = 0L, archive = 1, linked = 0, unused = 0)
                cacheIndexVals[segment].put(indexOffset, indexVal)
            }
        }

        return recid
    }


    override fun <R> update(recid: Long, record: R?, serializer: Serializer<R>) {
        assertNotClosed()
        val di = serialize(record, serializer, recid)

        Utils.lockWrite(locks,recidToSegment(recid)) {
            updateProtected(recid, di)
        }
    }

    private fun updateProtected(recid: Long, di: DataOutput2?){
        if(CC.ASSERT)
            locks?.checkWriteLocked(recidToSegment(recid))

        val oldIndexVal = getIndexVal(recid);
        val oldLinked = indexValFlagLinked(oldIndexVal);
        val oldSize = indexValToSize(oldIndexVal);
        if (oldSize == DELETED_RECORD_SIZE)
            throw DBException.GetVoid(recid)
        fun roundSixDown(size:Long) = if(size<6) 0 else size
        val newUpSize: Long =
                if (di == null) -16L
                else roundUp(roundSixDown(di.pos.toLong()), 16)
        //try to reuse record if possible, if not possible, delete old record and allocate new
        if (oldLinked || (
                (newUpSize != roundUp(roundSixDown(oldSize), 16)) &&
                oldSize != NULL_RECORD_SIZE && oldSize > 5L )) {
            Utils.lock(structuralLock) {
                if (oldLinked) {
                    linkedRecordDelete(oldIndexVal,recid)
                } else {
                    val oldOffset = indexValToOffset(oldIndexVal);
                    val sizeUp = roundUp(oldSize, 16)
                    if (CC.ZEROS)
                        volume.clear(oldOffset, oldOffset + sizeUp)
                    releaseData(sizeUp, oldOffset, false)
                    cacheRecords[recidToSegment(recid)].remove(oldOffset);
                }
            }
        }

        if (di == null) {
            //null values
            setIndexVal(recid, indexValCompose(size = NULL_RECORD_SIZE, offset = 0L, linked = 0, unused = 0, archive = 1))
            return
        }

        if (di.pos > MAX_RECORD_SIZE) {
            //linked record
            val newIndexVal = linkedRecordPut(di.buf, di.pos, recid)
            setIndexVal(recid, newIndexVal);
            return
        }
        val size = di.pos;
        val offset =
                if(size!=0 && size<6 ){
                    DataIO.getLong(di.buf,0).ushr((7-size)*8)
                } else if (!oldLinked && newUpSize == roundUp(oldSize, 16) && oldSize>=6 ) {
                    //reuse existing offset
                    indexValToOffset(oldIndexVal)
                } else if (size == 0) {
                    0L
                } else {
                    Utils.lock(structuralLock) {
                        allocateData(roundUp(size, 16), false)
                    }
                }
        //volume.putData(offset, di.buf, 0, size)
        if(size>5) {
            val walId = wal.walPutRecord(recid, di.buf, 0, size)
            cacheRecords[recidToSegment(recid)].put(offset, walId)
        }
        setIndexVal(recid, indexValCompose(size = size.toLong(), offset = offset, linked = 0, unused = 0, archive = 1))
        return
    }

    override fun <R> get(recid: Long, serializer: Serializer<R>): R? {
        val segment = recidToSegment(recid)

        var di:DataInput2? = null
        var size:Long = -1
        var fromLong = Long.MIN_VALUE

        Utils.lockRead(locks,segment){

            val indexVal = getIndexVal(recid)
            size = indexValToSize(indexVal)
            if (size == NULL_RECORD_SIZE)
                return null
            if (size == DELETED_RECORD_SIZE)
                throw DBException.GetVoid(recid)

            if (indexValFlagLinked(indexVal)) {
                val ba = linkedRecordGet(indexVal, recid)
                di = DataInput2.ByteArray(ba)
                size = ba.size.toLong()
            }else {

                val offset = indexValToOffset(indexVal)

                if (size < 6) {
                    fromLong = offset
                } else {
                    val walId = cacheRecords[segment].get(offset)
                    if (walId != 0L) {
                        //record found in WAL, get it from there
                        di = DataInput2.ByteArray(wal.walGetRecord(walId, recid))

                    //not in WAL, load from volume
                    }else if(serializer.isQuick){
                        //serialize without making copy
                        val di2 = volume.getDataInput(offset, size.toInt())
                        return deserialize(serializer, di2, size, recid)
                    } else {
                        //copy data into byte[]
                        val b = ByteArray(size.toInt())
                        volume.getData(offset, b, 0, size.toInt())
                        di = DataInput2.ByteArray(b)
                    }

                }
            }
        }
        if(fromLong!=Long.MIN_VALUE) {
            return serializer.deserializeFromLong(fromLong.ushr(8), size.toInt())
        }else{
            return deserialize(serializer, di!!, size, recid)
        }
    }

    private fun <R> getProtected(recid: Long, serializer: Serializer<R>): R? {
        val segment = recidToSegment(recid)
        locks?.checkReadLocked(segment)

        val indexVal = getIndexVal(recid)
        val size = indexValToSize(indexVal)
        if (size == NULL_RECORD_SIZE)
            return null
        if (size == DELETED_RECORD_SIZE)
            throw DBException.GetVoid(recid)

        if (indexValFlagLinked(indexVal)) {
            val ba = linkedRecordGet(indexVal, recid)
            return deserialize(serializer, DataInput2.ByteArray(ba), ba.size.toLong(), recid)
        }

        val volOffset = indexValToOffset(indexVal)

        if (size < 6) {
            return serializer.deserializeFromLong(volOffset.ushr(8), size.toInt())
        }

        val walId = cacheRecords[segment].get(volOffset)
        val di = if (walId != 0L) {
            //try to get from WAL
            DataInput2.ByteArray(wal.walGetRecord(walId, recid))
        } else {
            //not in WAL, load from volume
            volume.getDataInput(volOffset, size.toInt())
        }
        return deserialize(serializer, di, size, recid)
    }

    override fun getAllRecids(): LongIterator {
        val ret = LongArrayList()

        Utils.lockReadAll(locks){
            val maxRecid = maxRecid
            for (recid in 1..maxRecid) {
                try {
                    val indexVal = getIndexVal(recid)
                    if (indexValFlagUnused(indexVal).not())
                        ret.add(recid)
                } catch(e: Exception) {
                    //TODO better way to check for parity errors, EOF etc
                }
            }
        }
        return ret.toArray().iterator()
    }


    override fun verify() {

    }
    override fun close() {
        Utils.lockWriteAll(locks){
            if (closed.compareAndSet(false, true).not())
                return

            volume.close()
            if (fileDeleteAfterClose && file != null) {
                File(file).delete()
                wal.destroyWalFiles()
            }
        }

    }

    override fun rollback() {
        Utils.lockWriteAll(locks){
            realVolume.getData(0,headBytes, 0, headBytes.size)
            cacheIndexLinks.clear()
            cacheIndexVals.forEach { it.clear() }
            cacheRecords.forEach { it.clear() }
            cacheStacks.clear()
            indexPages.clear()
            for(page in indexPagesBackup)
                indexPages.add(page)
            wal.rollback()
        }
    }

    override fun commit() {
        Utils.lockWriteAll(locks){
            DataIO.putInt(headBytes, 20, calculateHeaderChecksum())
            //write index page
            wal.walPutByteArray(0, headBytes, 0, headBytes.size)
            wal.commit()

            realVolume.putData(0, headBytes, 0, headBytes.size)

            realVolume.ensureAvailable(fileTail)

            //flush index values
            for (indexVals in cacheIndexVals) {
                indexVals.forEachKeyValue { indexOffset, indexVal ->
                    realVolume.putLong(indexOffset, indexVal)
                }
                indexVals.clear()
            }
            cacheIndexLinks.forEachKeyValue { indexOffset, indexVal ->
                realVolume.putLong(indexOffset, indexVal)
            }
            cacheIndexLinks.clear()

            //flush long stack pages
            cacheStacks.forEachKeyValue { offset, bytes ->
                realVolume.putData(offset, bytes, 0, bytes.size)
            }
            cacheStacks.clear()

            //move modified records from indexPages
            for (records in cacheRecords) {
                records.forEachKeyValue { offset, walId ->
                    val bytes = wal.walGetRecord(walId, 0)
                    realVolume.putData(offset, bytes, 0, bytes.size)
                }
                records.clear()
            }

            indexPagesBackup = indexPages.toArray()
            realVolume.sync()

            wal.destroyWalFiles()
            wal.close()
        }
    }

    override fun compact() {
        //TODO compaction
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

        wal.walPutLong(pagePointerOffset, parity16Set(indexPage))
        cacheIndexLinks.put(pagePointerOffset, parity16Set(indexPage))
        //volume.putLong(pagePointerOffset, parity16Set(indexPage))

        //add this page to list of pages
        indexPages.add(indexPage)

        //zero out pointer to next page with valid parity
        wal.walPutLong(indexPage+8, parity16Set(0))
        cacheIndexLinks.put(indexPage+8, parity16Set(0))
        //volume.putLong(indexPage+8, parity16Set(0))
        return indexPage;
    }

    override fun freeSizeIncrement(increment: Long) {
        //TODO free size ignored
    }



    override protected fun longStackPut(masterLinkOffset:Long, value:Long, recursive:Boolean){
        if(CC.ASSERT)
            Utils.assertLocked(structuralLock)
        if (CC.ASSERT && (masterLinkOffset <= 0 || masterLinkOffset > CC.PAGE_SIZE || masterLinkOffset % 8 != 0L))
            throw DBException.DataCorruption("wrong master link")
        if(CC.ASSERT && value.shr(48)!=0L)
            throw AssertionError()
        if(CC.ASSERT)
            parity1Get(value)

        /** size of value after it was packed */
        val valueSize:Long = DataIO.packLongSize(value).toLong()

        val masterLinkVal:Long = parity4Get(headVol.getLong(masterLinkOffset))
        if (masterLinkVal == 0L) {
            //empty stack, create new chunk
            longStackNewChunk(masterLinkOffset, 0L, value, valueSize, true)
            return
        }
        val chunkOffset = masterLinkVal and MOFFSET
        val currSize = masterLinkVal.ushr(48)
        var ba = longStackLoadChunk(chunkOffset)

        //is there enough space in current chunk?
        if (currSize + valueSize > ba.size) {
            //no there is not enough space
            //allocate new chunk
            longStackNewChunk(masterLinkOffset, chunkOffset, value, valueSize, true) //TODO recursive=true here is too paranoid, and could be improved
            return
        }
        //there is enough free space here, so put it there
        packLong(ba, currSize.toInt(), value)
        //volume.putPackedLong(chunkOffset+currSize, value)
        //and update master link with new size
        val newMasterLinkValue = (currSize+valueSize).shl(48) + chunkOffset
        headVol.putLong(masterLinkOffset, parity4Set(newMasterLinkValue))
    }

    private fun longStackLoadChunk(chunkOffset: Long): ByteArray {
        var ba = cacheStacks.get(chunkOffset)
        if(ba==null) {
            val prevLinkVal = parity4Get(volume.getLong(chunkOffset))
            val pageSize = prevLinkVal.ushr(48).toInt()
            //load from volume
            ba = ByteArray(pageSize)
            volume.getData(chunkOffset, ba, 0, pageSize)
            cacheStacks.put(chunkOffset,ba)
        }
        if(CC.ASSERT && ba.size>LONG_STACK_MAX_SIZE)
            throw AssertionError()
        return ba
    }

    protected fun longStackNewChunk(masterLinkOffset: Long, prevPageOffset: Long, value: Long, valueSize:Long, recursive: Boolean) {
        if(CC.ASSERT) {
            Utils.assertLocked(structuralLock)
        }
        if(CC.PARANOID){
            //ensure that this  longStackPut() method is not twice on stack trace
            val stack = Thread.currentThread().stackTrace
            if(stack.filter { it.methodName.startsWith("longStackPut")}.count()>1)
                throw AssertionError("longStackNewChunk called in recursion, longStackPut() is more then once on stack frame")
            if(stack.filter { it.methodName.startsWith("longStackTake")}.count()>1)
                throw AssertionError("longStackNewChunk called in recursion, longStackTake() is more then once on stack frame")
        }

        if (CC.ASSERT && (masterLinkOffset <= 0 || masterLinkOffset > CC.PAGE_SIZE || masterLinkOffset % 8 != 0L))
            throw DBException.DataCorruption("wrong master link")

        var newChunkSize:Long = -1L
        if(!recursive){
            // In this case do not allocate fixed size, but try to reuse existing free space.
            // That reduces fragmentation. But can not be used in recursion

            sizeLoop@ for(size in LONG_STACK_MAX_SIZE downTo  LONG_STACK_MIN_SIZE step 16){
                val masterLinkOffset2 = longStackMasterLinkOffset(size)
                if (masterLinkOffset == masterLinkOffset2) {
                    //we can not modify the same long stack, so skip
                    continue@sizeLoop
                }
                val indexVal = parity4Get(headVol.getLong(masterLinkOffset2))
                if (indexVal != 0L) {
                    newChunkSize = size
                    break@sizeLoop
                }
            }
        }

        val dataTail = dataTail
        val remainderSize = roundUp(dataTail, CC.PAGE_SIZE) - dataTail
        if(newChunkSize==-1L) {
            if (dataTail == 0L) {
                // will have to allocate new data page, plenty of size
                newChunkSize = LONG_STACK_PREF_SIZE
            }else{
                // Check space before end of data page.
                // Set size so it fully fits remainder of page

                newChunkSize =
                        if(remainderSize>LONG_STACK_MAX_SIZE || remainderSize<LONG_STACK_MIN_SIZE)
                            LONG_STACK_PREF_SIZE
                        else
                            remainderSize
            }
        }

        if(CC.ASSERT && newChunkSize % 16!=0L)
            throw AssertionError()

        //by now we should have determined size to take, so just take it
        val newChunkOffset:Long = allocateData(newChunkSize.toInt(), true)  //TODO recursive=true here is too paranoid, and could be improved
        val ba = ByteArray(newChunkSize.toInt())
        cacheStacks.put(newChunkOffset,ba)
        //write size of current chunk with link to prev chunk
        //volume.putLong(newChunkOffset, parity4Set((newChunkSize shl 48) + prevPageOffset))
        putLong(ba,0, parity4Set((newChunkSize shl 48) + prevPageOffset))
        //put value
        //volume.putPackedLong(newChunkOffset+8, value)
        packLong(ba, 8, value)
        //update master link
        val newSize:Long = 8+valueSize;
        val newMasterLinkValue = newSize.shl(48) + newChunkOffset
        headVol.putLong(masterLinkOffset, parity4Set(newMasterLinkValue))
    }

    override protected fun longStackTake(masterLinkOffset:Long, recursive:Boolean):Long {
        if(CC.ASSERT)
            Utils.assertLocked(structuralLock)

        if (CC.ASSERT && (masterLinkOffset <= 0 || masterLinkOffset > CC.PAGE_SIZE || masterLinkOffset % 8 != 0L))
            throw DBException.DataCorruption("wrong master link")

        val masterLinkVal = parity4Get(headVol.getLong(masterLinkOffset))
        if (masterLinkVal == 0L) {
            //empty stack
            return 0;
        }

        val offset = masterLinkVal and MOFFSET
        var ba = longStackLoadChunk(offset)

        //find position to read from
        var pos:Int = Math.max(masterLinkVal.ushr(48)-1, 8).toInt()
        //now decrease position to find ending byte of
        while(pos>8 && (ba[pos-1].toInt() and 0x80)==0){
            pos--
        }

        if(CC.ASSERT && pos<8L)
            throw DBException.DataCorruption("position too small")

        if(CC.ASSERT && getLong(ba, 0).ushr(48)<=pos)
            throw DBException.DataCorruption("position beyond chunk "+masterLinkOffset);

        //get value and zero it out
        val ret = unpackLong(ba,pos)
        for(i in pos until pos+packLongSize(ret)) {
            ba[i] = 0
            //volume.clear(offset+pos, offset+pos+ DataIO.packLongSize(ret))
        }

        //update size on master link
        if(pos>8L) {
            //there is enough space on current chunk, so just decrease its size
            headVol.putLong(masterLinkOffset, parity4Set(pos.toLong().shl(48) + offset))
            if(CC.ASSERT && ret.shr(48)!=0L)
                throw AssertionError()
            if(CC.ASSERT && ret!=0L)
                parity1Get(ret)

            return ret;
        }

        //current chunk become empty, so delete it
        val prevChunkValue = parity4Get(getLong(ba,0))
        putLong(ba,0,0)
        val currentSize = prevChunkValue.ushr(48)
        val prevChunkOffset = prevChunkValue and MOFFSET

        //does previous page exists?
        val masterLinkPos:Long = if (prevChunkOffset != 0L) {
            //TODO in this case baPrev might be unmodified. Use some sort of flag to indicate modified fields
            val baPrev = longStackLoadChunk(prevChunkOffset)
            //yes previous page exists, return its size, decreased by start
            val pos2 = parity4Get(getLong(baPrev,0)).ushr(48)
            longStackFindEnd(prevChunkOffset, pos2)
        }else{
            0L
        }

        //update master pointer
        headVol.putLong(masterLinkOffset, parity4Set(masterLinkPos.shl(48) + prevChunkOffset))

        //release old page
        //TODO clear
//        if(CC.ZEROS)
//            volume.clear(offset,offset+currentSize) //TODO incremental clear

        releaseData(currentSize, offset, true);

        if(CC.ASSERT && ret.shr(48)!=0L)
            throw AssertionError()
        if(CC.ASSERT && ret != 0L)
            parity1Get(ret)
        return ret;
    }

    protected fun longStackFindEnd(pageOffset:Long, pos:Long):Long{
        val ba = longStackLoadChunk(pageOffset)
        var pos2 = pos.toInt()
        while(pos2>8 && ba[pos2-1]==0.toByte()){
            pos2--
        }
        return pos2.toLong()
    }

    override fun fileLoad() = volume.fileLoad()

    override fun getAllFiles(): Iterable<String> {
        if(file==null)
            return Arrays.asList<String>()

        val ret = arrayListOf(file)
        ret.addAll(wal.getAllFiles())
        return ret.toList() //immutable copy
    }

}