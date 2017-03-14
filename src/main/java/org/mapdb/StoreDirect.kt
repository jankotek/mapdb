package org.mapdb

import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.mapdb.DataIO.*
import org.mapdb.StoreDirectJava.*
import org.mapdb.volume.Volume
import org.mapdb.volume.VolumeFactory
import java.io.File
import java.util.*
import java.util.concurrent.atomic.AtomicLong

/**
 * Store which uses binary storage (file, memory buffer...) and updates records on place.
 * It has memory allocator, so it reuses space freed by deletes and updates.
 */
class StoreDirect(
        file:String?,
        volumeFactory: VolumeFactory,
        override val isReadOnly:Boolean,
        fileLockWait:Long,
        isThreadSafe:Boolean,
        concShift:Int,
        allocateIncrement: Long,
        allocateStartSize:Long,
        fileDeleteAfterClose:Boolean,
        fileDeleteAfterOpen: Boolean,
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
),StoreBinary{


    companion object{
        fun make(
                file:String?= null,
                volumeFactory: VolumeFactory = if(file==null) CC.DEFAULT_MEMORY_VOLUME_FACTORY else CC.DEFAULT_FILE_VOLUME_FACTORY,
                fileLockWait:Long = 0L,
                isReadOnly:Boolean = false,
                isThreadSafe:Boolean = true,
                concShift:Int = CC.STORE_DIRECT_CONC_SHIFT,
                allocateIncrement:Long = CC.PAGE_SIZE,
                allocateStartSize: Long = 0L,
                fileDeleteAfterClose:Boolean = false,
                fileDeleteAfterOpen: Boolean = false,
                checksum:Boolean = false,
                checksumHeader:Boolean = true,
                checksumHeaderBypass:Boolean = false
        ) = StoreDirect(
            file = file,
            volumeFactory = volumeFactory,
            fileLockWait = fileLockWait,
            isReadOnly = isReadOnly,
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
    }

    protected val freeSize = AtomicLong(-1L)

    override protected val volume: Volume = {
        volumeFactory.makeVolume(
                file,
                isReadOnly,
                fileLockWait,
                Math.max(CC.PAGE_SHIFT, DataIO.shift(allocateIncrement.toInt())),
                roundUp(allocateStartSize, CC.PAGE_SIZE),
                false
        )
    }()

    override protected val headVol = volume

    init{
        Utils.lock(structuralLock) {
            if (!volumeExistsAtStart) {
                //TODO crash resistance while file is being created
                //initialize values
                volume.ensureAvailable(CC.PAGE_SIZE)
                volume.putLong(0L, fileHeaderCompose())
                volume.putLong(8L, 1L)
                dataTail = 0L
                maxRecid = 0L
                fileTail = CC.PAGE_SIZE

                volume.putInt(16, storeHeaderCompose())

                //initialize long stack master links
                for (offset in RECID_LONG_STACK until HEAD_END step 8) {
                    volume.putLong(offset, parity4Set(0L))
                }
                //initialize zero link from first page
                volume.putLong(ZERO_PAGE_LINK, parity16Set(0L))

                commit()
            } else {
                if(volume.length()<=0)
                    throw DBException.DataCorruption("File is empty")
                fileHeaderCheck()
                loadIndexPages(indexPages)
            }
            if(file!=null && fileDeleteAfterOpen)
                File(file).delete()
        }
    }

    override protected fun getIndexVal(recid:Long):Long{
        if(CC.PARANOID) //should be ASSERT, but this method is accessed way too often
            locks?.checkReadLocked(recidToSegment(recid))

        try {
            val offset = recidToOffset(recid)
            val indexVal = volume.getLong(offset);
            if(indexVal == 0L)
                throw DBException.GetVoid(recid)
            return parity1Get(indexVal);
        }catch (e:IndexOutOfBoundsException){
            throw DBException.GetVoid(recid);
        }
    }

    override protected fun setIndexVal(recid:Long, value:Long){
        if(CC.ASSERT)
            locks?.checkWriteLocked(recidToSegment(recid))

        val offset = recidToOffset(recid)
        volume.putLong(offset, parity1Set(value));
    }



    override protected fun allocateNewPage():Long{
        if(CC.ASSERT)
            Utils.assertLocked(structuralLock)

        val eof = fileTail
        val newEof = eof + CC.PAGE_SIZE
        volume.ensureAvailable(newEof)
        if(CC.ZEROS)
            volume.clear(eof, newEof) //TODO clear should be part of Volume.ensureAvail
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

        if(CC.ASSERT && parity16Get(volume.getLong(pagePointerOffset))!=0L)
            throw DBException.DataCorruption("index pointer not empty")

        volume.putLong(pagePointerOffset, parity16Set(indexPage))

        //add this page to list of pages
        indexPages.add(indexPage)

        //zero out pointer to next page with valid parity
        volume.putLong(indexPage+8, parity16Set(0))
        return indexPage;
    }

    protected fun linkedRecordGet(indexValue:Long):ByteArray{

        if(CC.ASSERT && !indexValFlagLinked(indexValue))
            throw AssertionError("not linked record")

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

            volume.getData(offset+nextPointerSize, b, bpos, size)
            bpos+=size;

            if(!isLinked)
                break@chunks

            pointer = parity3Get(volume.getLong(offset))
        }

        return Arrays.copyOf(b,bpos) //TODO PERF this copy can be avoided with boundary checking DataInput
    }

    protected fun linkedRecordDelete(indexValue:Long){
        if(CC.ASSERT && !indexValFlagLinked(indexValue))
            throw AssertionError("not linked record")

        var pointer = indexValue
        chunks@ while(pointer!=0L) {
            val isLinked = indexValFlagLinked(pointer);
            val size = indexValToSize(pointer)
            val offset = indexValToOffset(pointer)

            //read next pointer
            pointer = if(isLinked)
                    parity3Get(volume.getLong(offset))
                else
                    0L
            val sizeUp = roundUp(size,16);
            if(CC.ZEROS)
                volume.clear(offset,offset+sizeUp)
            releaseData(sizeUp, offset, false);
        }
    }

    protected fun linkedRecordPut(output:ByteArray, size:Int):Long{
        var remSize = size.toLong();
        //insert first non linked record
        var chunkSize:Long = Math.min(MAX_RECORD_SIZE, remSize);
        var chunkOffset = Utils.lock(structuralLock){
            allocateData(roundUp(chunkSize.toInt(),16), false)
        }
        volume.putData(chunkOffset, output, (remSize-chunkSize).toInt(), chunkSize.toInt())
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
            volume.putLong(chunkOffset, prevLink)
            //and write data
            remSize-=chunkSize
            volume.putData(chunkOffset+8, output, remSize.toInt(), chunkSize.toInt())
        }
        if(CC.ASSERT && remSize!=0L)
            throw AssertionError();
        return (chunkSize+8).shl(48) + chunkOffset + isLinked + MARCHIVE
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

        val masterLinkVal:Long = parity4Get(volume.getLong(masterLinkOffset))
        if (masterLinkVal == 0L) {
            //empty stack, create new chunk
            longStackNewChunk(masterLinkOffset, 0L, value, valueSize, true)
            return
        }
        val chunkOffset = masterLinkVal and MOFFSET
        val currSize = masterLinkVal.ushr(48)
        val prevLinkVal = parity4Get(volume.getLong(chunkOffset))
        val pageSize = prevLinkVal.ushr(48)

        //is there enough space in current chunk?
        if (currSize + valueSize > pageSize) {
            //no there is not enough space
            //allocate new chunk
            longStackNewChunk(masterLinkOffset, chunkOffset, value, valueSize, true) //TODO recursive=true here is too paranoid, and could be improved
            return
        }
        //there is enough free space here, so put it there
        volume.putPackedLong(chunkOffset+currSize, value)
        //and update master link with new size
        val newMasterLinkValue = (currSize+valueSize).shl(48) + chunkOffset
        volume.putLong(masterLinkOffset, parity4Set(newMasterLinkValue))
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
                val indexVal = parity4Get(volume.getLong(masterLinkOffset2))
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
        if(!CC.ZEROS)
            volume.clear(newChunkOffset, newChunkOffset+newChunkSize) //zeroes are used to determine end of stack page, so it must be zeroed out, even if allocateData does not clear out pages
        //write size of current chunk with link to prev chunk
        volume.putLong(newChunkOffset, parity4Set((newChunkSize shl 48) + prevPageOffset))
        //put value
        volume.putPackedLong(newChunkOffset+8, value)
        //update master link
        val newSize:Long = 8+valueSize;
        val newMasterLinkValue = newSize.shl(48) + newChunkOffset
        volume.putLong(masterLinkOffset, parity4Set(newMasterLinkValue))
    }

    override protected fun longStackTake(masterLinkOffset:Long, recursive:Boolean):Long {
        if(CC.ASSERT)
            Utils.assertLocked(structuralLock)

        if (CC.ASSERT && (masterLinkOffset <= 0 || masterLinkOffset > CC.PAGE_SIZE || masterLinkOffset % 8 != 0L))
            throw DBException.DataCorruption("wrong master link")

        val masterLinkVal = parity4Get(volume.getLong(masterLinkOffset))
        if (masterLinkVal == 0L) {
            //empty stack
            return 0;
        }

        val offset = masterLinkVal and MOFFSET

        //find position to read from
        var pos:Long = Math.max(masterLinkVal.ushr(48)-1, 8)
        //now decrease position to find ending byte of
        while(pos>8 && (volume.getUnsignedByte(offset+pos-1) and 0x80)==0){
            pos--
        }

        if(CC.ASSERT && pos<8L)
            throw DBException.DataCorruption("position too small")

        if(CC.ASSERT && volume.getLong(offset).ushr(48)<=pos)
            throw DBException.DataCorruption("position beyond chunk "+masterLinkOffset);

        //get value and zero it out
        var ret = volume.getPackedLong(offset+pos)
        volume.clear(offset+pos, offset+pos+ ret.ushr(60))
        ret = ret and DataIO.PACK_LONG_RESULT_MASK

        //update size on master link
        if(pos>8L) {
            //there is enough space on current chunk, so just decrease its size
            volume.putLong(masterLinkOffset, parity4Set(pos.shl(48) + offset))
            if(CC.ASSERT && ret.shr(48)!=0L)
                throw AssertionError()
            if(CC.ASSERT)
                parity1Get(ret)

            return ret;
        }

        //current chunk become empty, so delete it
        val prevChunkValue = parity4Get(volume.getLong(offset))
        volume.putLong(offset, 0L)
        val currentSize = prevChunkValue.ushr(48)
        val prevChunkOffset = prevChunkValue and MOFFSET

        //does previous page exists?
        val masterLinkPos:Long = if (prevChunkOffset != 0L) {
            //yes previous page exists, return its size, decreased by start
            val pos2 = parity4Get(volume.getLong(prevChunkOffset)).ushr(48)
            longStackFindEnd(prevChunkOffset, pos2)
        }else{
            0L
        }

        //update master pointer
        volume.putLong(masterLinkOffset, parity4Set(masterLinkPos.shl(48) + prevChunkOffset))

        //release old page
        if(CC.ZEROS)
            volume.clear(offset,offset+currentSize) //TODO incremental clear

        releaseData(currentSize, offset, true);

        if(CC.ASSERT && ret.shr(48)!=0L)
            throw AssertionError()
        if(CC.ASSERT && ret!=0L)
            parity1Get(ret)
        return ret;
    }

    protected fun longStackFindEnd(pageOffset:Long, pos:Long):Long{
        var pos2 = pos
        while(pos2>8 && volume.getUnsignedByte(pageOffset+pos2-1)==0){
            pos2--
        }
        return pos2
    }

    protected fun longStackForEach(masterLinkOffset: Long, body: (value: Long) -> Unit,
                                   setZeroes: Function2<Long,Long,Unit>? = null ) {

        // assert first page
        val linkVal = parity4Get(volume.getLong(masterLinkOffset))
        var endSize = indexValToSize(linkVal)
        var offset = indexValToOffset(linkVal)

        while (offset != 0L) {
            val currHead = parity4Get(volume.getLong(offset))
            val currSize = indexValToSize(currHead)

            setZeroes?.invoke(offset, offset+currSize)
            volume.assertZeroes(offset + endSize, offset + currSize)
            //iterate over values
            var pos = 8L
            while(pos< endSize) {
                var stackVal = volume.getPackedLong(offset + pos)
                pos+=stackVal.ushr(60)
                stackVal = stackVal and DataIO.PACK_LONG_RESULT_MASK

                if (stackVal.ushr(48) != 0L)
                    throw AssertionError()

                parity1Get(stackVal) //assert parity
                body(stackVal)
            }

            //set values for next page
            offset = indexValToOffset(currHead)
            if (offset != 0L) {
                endSize = indexValToSize(parity4Get(volume.getLong(offset)))
                endSize = longStackFindEnd(offset, endSize)
            }
        }
    }

    override fun preallocate(): Long {
        assertNotClosed()
        val recid = Utils.lock(structuralLock){
            allocateRecid()
        }

        Utils.lockWrite(locks,recidToSegment(recid)) {
            if (CC.ASSERT) {
                val oldVal = volume.getLong(recidToOffset(recid))
                if(oldVal!=0L && indexValToSize(oldVal)!=DELETED_RECORD_SIZE)
                    throw DBException.DataCorruption("old recid is not empty")
            }

            //set allocated flag
            setIndexVal(recid, indexValCompose(size = NULL_RECORD_SIZE, offset = 0, linked = 0, unused = 1, archive = 1))
            return recid
        }
    }

    override fun <R> get(recid: Long, serializer: Serializer<R>): R? {
        assertNotClosed()

        var di:DataInput2? = null
        var size:Long = -1
        var fromLong = Long.MIN_VALUE

        Utils.lockRead(locks,recidToSegment(recid)) {
            val indexVal = getIndexVal(recid)

            if (indexValFlagLinked(indexVal)) {
                //linked record is always read into byte[]
                val ba = linkedRecordGet(indexVal)
                di = DataInput2.ByteArray(ba)
                size = ba.size.toLong()
            }else {
                size = indexValToSize(indexVal);
                if (size == DELETED_RECORD_SIZE)
                    throw DBException.GetVoid(recid)

                if (size == NULL_RECORD_SIZE)
                    return null

                val offset = indexValToOffset(indexVal)

                if (size < 6) {
                     fromLong = offset
                }else if(serializer.isQuick){
                    //serialize without making copy
                    val di2 = volume.getDataInput(offset, size.toInt())
                    return deserialize(serializer, di2, size, recid)
                }else{
                    //copy data into byte[]
                    val b = ByteArray(size.toInt())
                    volume.getData(offset, b, 0, size.toInt())
                    di = DataInput2.ByteArray(b)
                }
            }
        }

        if(fromLong!=Long.MIN_VALUE) {
            return serializer.deserializeFromLong(fromLong.ushr(8), size.toInt())
        }else{
            return deserialize(serializer, di!!, size, recid)
        }
    }

    protected fun <R> getProtected(recid: Long, serializer: Serializer<R>): R? {
        locks?.checkReadLocked(recidToSegment(recid))

        val indexVal = getIndexVal(recid);

        if (indexValFlagLinked(indexVal)) {
            val di = linkedRecordGet(indexVal)
            return deserialize(serializer, DataInput2.ByteArray(di), di.size.toLong(), recid)
        }

        var size = indexValToSize(indexVal);
        if (size == DELETED_RECORD_SIZE)
            throw DBException.GetVoid(recid)

        if (size == NULL_RECORD_SIZE)
            return null;

        val offset = indexValToOffset(indexVal);

        if (size < 6) {
            return serializer.deserializeFromLong(offset.ushr(8), size.toInt())
        }

        val di = volume.getDataInput(offset, size.toInt())
        return deserialize(serializer, di, size, recid)
    }



    override fun getBinaryLong(recid:Long, f: StoreBinaryGetLong): Long {
        assertNotClosed()

        Utils.lockRead(locks, recidToSegment(recid)) {
            val indexVal = getIndexVal(recid);

            if (indexValFlagLinked(indexVal)) {
                val di = linkedRecordGet(indexVal)
                return f.get(DataInput2.ByteArray(di), di.size)
            }


            val size = indexValToSize(indexVal);
            if (size == DELETED_RECORD_SIZE)
                throw DBException.GetVoid(recid)

            if (size == NULL_RECORD_SIZE)
                return Long.MIN_VALUE;

            val offset = indexValToOffset(indexVal);
            val sizeInt = size.toInt()
            val di =
                if(size>=6)
                    volume.getDataInput(offset, sizeInt)
                else{
                    val buf = ByteArray(sizeInt)
                    DataIO.putLong(buf, 0, offset.ushr(8), sizeInt)
                    DataInput2.ByteArray(buf)
                }

            return f.get(di,sizeInt)
        }
    }

    override fun <R> put(record: R?, serializer: Serializer<R>): Long {
        assertNotClosed()

        val di = serialize(record, serializer, -1);

        Utils.lockRead(compactionLock) {

            val recid = Utils.lock(structuralLock) {
                allocateRecid()
            }

            Utils.lockWrite(locks, recidToSegment(recid)) {
                if (di == null) {
                    setIndexVal(recid, indexValCompose(size = NULL_RECORD_SIZE, offset = 0, linked = 0, unused = 0, archive = 1))
                    return recid
                }

                if (di.pos > MAX_RECORD_SIZE) {
                    //save as linked record
                    val indexVal = linkedRecordPut(di.buf, di.pos)
                    setIndexVal(recid, indexVal);
                    return recid
                }
                val size = di.pos.toLong()
                var offset: Long
                //allocate space for data
                if (di.pos == 0) {
                    offset = 0L
                } else if (di.pos < 6) {
                    //store inside offset at index table
                    offset = DataIO.getLong(di.buf, 0).ushr((7 - di.pos) * 8)
                } else {
                    offset = Utils.lock(structuralLock) {
                        allocateData(roundUp(di.pos, 16), false)
                    }
                    volume.putData(offset, di.buf, 0, di.pos)
                }

                setIndexVal(recid, indexValCompose(size = size, offset = offset, linked = 0, unused = 0, archive = 1))
                return recid;
            }
        }
    }

    override fun <R> update(recid: Long, record: R?, serializer: Serializer<R>) {
        assertNotClosed()
        val di = serialize(record, serializer, recid);

        Utils.lockWrite(locks, recidToSegment(recid)) {
            updateProtected(recid, di)
        }
    }

    private fun updateProtected(recid: Long, di: DataOutput2?) {
        if (CC.ASSERT)
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
                    linkedRecordDelete(oldIndexVal)
                } else {
                    val oldOffset = indexValToOffset(oldIndexVal);
                    val sizeUp = roundUp(oldSize, 16)
                    if (CC.ZEROS)
                        volume.clear(oldOffset, oldOffset + sizeUp)
                    releaseData(sizeUp, oldOffset, false)
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
            val newIndexVal = linkedRecordPut(di.buf, di.pos)
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
        if(size>5)
            volume.putData(offset, di.buf, 0, size)
        setIndexVal(recid, indexValCompose(size = size.toLong(), offset = offset, linked = 0, unused = 0, archive = 1))
        return
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

            val di = serialize(newRecord, serializer, recid);

            updateProtected(recid, di)
            return true;
        }
    }

    override fun <R> delete(recid: Long, serializer: Serializer<R>) {
        assertNotClosed()

        Utils.lockWrite(locks, recidToSegment(recid)) {
            val oldIndexVal = getIndexVal(recid);
            val oldSize = indexValToSize(oldIndexVal);
            if (oldSize == DELETED_RECORD_SIZE)
                throw DBException.GetVoid(recid)

            if (oldSize != NULL_RECORD_SIZE) {
                Utils.lock(structuralLock) {
                    if (indexValFlagLinked(oldIndexVal)) {
                        linkedRecordDelete(oldIndexVal)
                    } else if(oldSize>5){
                        val oldOffset = indexValToOffset(oldIndexVal);
                        val sizeUp = roundUp(oldSize, 16)

                        if(CC.ZEROS)
                            volume.clear(oldOffset,oldOffset+sizeUp)
                        releaseData(sizeUp, oldOffset, false)
                    }
                    releaseRecid(recid)
                }
            }

            setIndexVal(recid, indexValCompose(size = DELETED_RECORD_SIZE, offset = 0L, linked = 0, unused = 0, archive = 1))
        }
    }

    override fun compact() {
        Utils.lockWrite(compactionLock) {
            Utils.lockWriteAll(locks) {
                Utils.lock(structuralLock) {
                    //TODO use file for compaction, if store is file based
                    val store2 = StoreDirect.make(isThreadSafe = false, concShift = 0)

                    //first allocate enough index pages, so they are at beginning of store
                    for (i in 0 until indexPages.size())
                        store2.allocateNewIndexPage()

                    if (CC.ASSERT && store2.indexPages.size() != indexPages.size())
                        throw AssertionError();

                    //now iterate over all recids
                    val maxRecid = maxRecid
                    for (recid in 1..maxRecid) {
                        var data: ByteArray? = null;
                        var exist:Boolean;
                        try {
                            data = getProtected(recid, Serializer.BYTE_ARRAY_NOSIZE)
                            exist = true
                        } catch(e: Exception) {
                            //TODO better way to check for parity errors, EOF etc
                            exist = false
                        }

                        if (!exist) {
                            //recid does not exist, mark it as deleted in other store
                            store2.releaseRecid(recid)
                            store2.setIndexVal(recid, store2.indexValCompose(
                                    size = DELETED_RECORD_SIZE, offset = 0L, linked = 0, unused = 0, archive = 1))
                        } else {
                            store2.putCompact(recid, data)
                        }
                    }

                    //finished, update some variables
                    store2.maxRecid = maxRecid

                    // copy content of volume
                    //TODO it would be faster to just swap volumes or rename file, but that is concurrency issue
                    val fileTail = store2.fileTail;
                    volume.truncate(fileTail)

                    for (page in 0 until fileTail step CC.PAGE_SIZE) {
                        store2.volume.copyTo(page, volume, page, CC.PAGE_SIZE)
                    }

                    //take index pages from second store
                    indexPages.clear()
                    indexPages.addAll(store2.indexPages)
                    //and update statistics
                    freeSize.set(store2.freeSize.get());

                    store2.close()
                }
            }
        }
    }

    /** only called from compaction, it inserts new record under given recid */
    private fun putCompact(recid: Long, data: ByteArray?) {
        if(CC.ASSERT && isThreadSafe) //compaction is done on second store, which sis always thread unsafe
            throw AssertionError();
        if (data == null) {
            setIndexVal(recid, indexValCompose(size = NULL_RECORD_SIZE, offset = 0, linked = 0, unused = 0, archive = 1))
            return
        }

        if (data.size > MAX_RECORD_SIZE) {
            //save as linked record
            val indexVal = linkedRecordPut(data, data.size)
            setIndexVal(recid, indexVal);
            return
        }

        //allocate space for data
        val offset =
                if(data.size==0) {
                    0L
                }else if (data.size<6){
                    //expand to full size
                    val data2 = Arrays.copyOf(data, 8)
                    //store inside offset at index table
                    DataIO.getLong(data2, 0).ushr((7 - data.size) * 8)
                }else {
                    val offset = allocateData(roundUp(data.size, 16), false)
                    volume.putData(offset, data, 0, data.size)
                    offset
                }

        setIndexVal(recid, indexValCompose(size = data.size.toLong(), offset = offset, linked = 0, unused = 0, archive = 1))
    }

    override fun commit() {
        assertNotClosed()
        //update checksum
        if(isReadOnly)
            return

        volume.putInt(20, calculateHeaderChecksum())
        if(checksum) {
            volume.putLong(8, calculateChecksum())
        }

        volume.sync()
    }

    override fun close() {
        Utils.lockWriteAll(locks){
            if(closed.compareAndSet(false,true).not())
                return

            //update checksum
            if(!isReadOnly) {
                volume.putInt(20, calculateHeaderChecksum())
                if (checksum) {
                    volume.putLong(8, calculateChecksum())
                }
            }

            volume.close()
            if(fileDeleteAfterClose && file!=null) {
                File(file).delete()
            }
        }
    }

    override fun getAllRecids(): LongIterator {
        val ret = LongArrayList()

        Utils.lockReadAll(locks){
            val maxRecid = maxRecid

            for (recid in 1..maxRecid) {
                val offset = recidToOffset(recid)
                try {
                    val indexVal = parity1Get(volume.getLong(offset))
                    if (indexValFlagUnused(indexVal).not())
                        ret.add(recid)
                } catch(e: Exception) {
                    //TODO better way to check for parity errors, EOF etc
                }
            }
        }
        return ret.toArray().iterator()
    }


    override fun verify(){

        /// TODO move this section back under lock, once Kotlin compiler issue is resolved in 1.1.2
        val bit = BitSet()
        val max = fileTail

        fun set(start: Long, end: Long, expectZeros: Boolean) {
            if (start > max)
                throw AssertionError("start too high")
            if (end > max)
                throw AssertionError("end too high")

            if (CC.ZEROS && expectZeros)
                volume.assertZeroes(start, end)

            val start0 = start.toInt()
            val end0 = end.toInt()

            for (index in start0 until end0) {
                if (bit.get(index)) {
                    throw AssertionError("already set $index - ${index % CC.PAGE_SIZE}")
                }
            }

            bit.set(start0, end0)
        }

        Utils.lockReadAll(locks){
            Utils.lock(structuralLock){


                set(0, HEAD_END, false)

                if (dataTail % CC.PAGE_SIZE != 0L) {
                    set(dataTail, roundUp(dataTail, CC.PAGE_SIZE), true)
                }

                fun iterateOverIndexValues(indexPage: Long, end: Long) {
                    for (indexOffset in indexPage + 16 until end step 8) {
                        //TODO preallocated versus deleted recids
                        set(indexOffset, indexOffset + 8, false)
                        var indexVal = parity1Get(volume.getLong(indexOffset))

                        if ((indexVal and MLINKED) == 0L && indexValToSize(indexVal) < 6)
                            continue;

                        while (indexVal and MLINKED != 0L) {
                            //iterate over linked
                            val offset = indexValToOffset(indexVal)
                            val size = roundUp(indexValToSize(indexVal), 16)
                            set(offset, offset + size, false)
                            indexVal = parity3Get(volume.getLong(offset))
                        }
                        val offset = indexValToOffset(indexVal)
                        val size = roundUp(indexValToSize(indexVal), 16)
                        if (size <= MAX_RECORD_SIZE)
                            set(offset, offset + size, false)
                    }
                }

                //analyze zero index page
                val zeroIndexPageEnd = Math.min(CC.PAGE_SIZE, recidToOffset(maxRecid) + 8)
                set(HEAD_END, HEAD_END + 16, false)
                iterateOverIndexValues(HEAD_END, zeroIndexPageEnd);
                if (zeroIndexPageEnd < CC.PAGE_SIZE) {
                    //expect zero at unused part
                    set(zeroIndexPageEnd, CC.PAGE_SIZE, true)
                }

                //iterate over index pages and mark their head
                indexPages.forEach { indexPage ->
                    set(indexPage, indexPage + 16, false)
                    val end = Math.min(indexPage + CC.PAGE_SIZE, recidToOffset(maxRecid) + 8)
                    iterateOverIndexValues(indexPage, end)
                    //if last index page, expect zeroes for unused part
                    if (end < indexPage + CC.PAGE_SIZE) {
                        set(end, indexPage + CC.PAGE_SIZE, true)
                    }
                }

                val setZeroes = { start: Long, end: Long ->
                    set(start, end, false)
                }
                longStackForEach(masterLinkOffset = RECID_LONG_STACK, setZeroes = setZeroes, body = { _ ->
                    //deleted recids should be marked separately
                })

                //iterate over free data
                for (size in 16..MAX_RECORD_SIZE step 16) {
                    val masterLinkOffset = longStackMasterLinkOffset(size)
                    longStackForEach(masterLinkOffset = masterLinkOffset, setZeroes = setZeroes, body = { freeOffset ->
                        val freeOffset2 = parity1Get(freeOffset).shl(3)
                        set(freeOffset2, freeOffset2 + size, true)
                    })
                }

                //ensure all data are set
                for (index in 0 until max) {
                    if (bit.get(index.toInt()).not()) {
                        var len = 0;
                        while (bit.get(index.toInt() + len).not()) {
                            len++;
                        }
                        throw AssertionError("not set at $index, for length $len - ${index % CC.PAGE_SIZE} - $dataTail - $fileTail")
                    }
                }
            }
        }
    }


    override protected fun freeSizeIncrement(increment: Long) {
        if(CC.ASSERT && increment%16!=0L)
            throw AssertionError()
        while (true) {
            val v = freeSize.get()
            if (v == -1L || freeSize.compareAndSet(v, v + increment))
                return
        }
    }


    fun getFreeSize(): Long {
        var ret = freeSize.get()
        if (ret != -1L)
            return ret
        Utils.lock(structuralLock){
            //try one more time under lock
            ret = freeSize.get()
            if (ret != -1L)
                return ret
            ret = calculateFreeSize()

            freeSize.set(ret)

            return ret
        }
    }

    private fun calculateFreeSize(): Long {
        Utils.assertLocked(structuralLock)
        //traverse list of records
        var ret1 = 0L
        val fileTail = fileTail
        for (size in 16..MAX_RECORD_SIZE step 16) {
            val masterLinkOffset = longStackMasterLinkOffset(size)
            longStackForEach(masterLinkOffset, { v ->
                val v2 = parity1Get(v).shl(3)
                if(CC.ASSERT && v2==0L)
                    throw AssertionError()
                if(CC.ASSERT && v2>fileTail)
                    throw AssertionError()

                ret1 += size
            })
        }
          //TODO Free size should include rest of data page, but that make stats unreliable for some reason
//        //set rest of data page
//        val dataTail = dataTail
//        println("ASAA $dataTail - ${dataTail % CC.PAGE_SIZE}")
//        if (dataTail % CC.PAGE_SIZE != 0L) {
//            ret1 += CC.PAGE_SIZE - dataTail % CC.PAGE_SIZE
//        }
        return ret1
    }

    fun getTotalSize():Long = fileTail

    override fun fileLoad() = volume.fileLoad()

    override fun getAllFiles(): Iterable<String> {
        if(file==null) return Arrays.asList<String>()
        else return Arrays.asList(file)
    }

}