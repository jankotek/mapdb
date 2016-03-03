package org.mapdb

import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.mapdb.StoreDirectJava.*
import org.mapdb.DBUtil.*
import org.mapdb.volume.Volume
import org.mapdb.volume.VolumeFactory
import java.io.IOException
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReadWriteLock

/**
 * Store which uses binary storage (file, memory buffer...) and updates records on place.
 * It has memory allocator, so it reuses space freed by deletes and updates.
 */
class StoreDirect(
        val file:String?,
        val volumeFactory: VolumeFactory,
        val readOnly:Boolean,
        override val isThreadSafe:Boolean,
        val concShift:Int,
        allocateStartSize:Long

):Store, StoreBinary{


    companion object{
        fun make(
                file:String?= null,
                volumeFactory: VolumeFactory = if(file==null) CC.DEFAULT_MEMORY_VOLUME_FACTORY else CC.DEFAULT_FILE_VOLUME_FACTORY,
                readOnly:Boolean = false,
                isThreadSafe:Boolean = true,
                concShift:Int = 4,
                allocateStartSize: Long = 0L
        ) = StoreDirect(
            file = file,
            volumeFactory = volumeFactory,
            readOnly = readOnly,
            isThreadSafe = isThreadSafe,
            concShift = concShift,
            allocateStartSize = allocateStartSize
        )
    }

    internal val freeSize = AtomicLong(-1L)

    private val segmentCount = 1.shl(concShift)
    private val segmentMask = 1L.shl(concShift)-1
    internal val locks:Array<ReadWriteLock?> = Array(segmentCount, {Utils.newReadWriteLock(isThreadSafe)})
    internal val structuralLock = Utils.newLock(isThreadSafe)

    private val volumeExistsAtStart = volumeFactory.exists(file)
    val volume: Volume = {
        volumeFactory.makeVolume(file, readOnly, false, CC.PAGE_SHIFT,
                roundUp(allocateStartSize, CC.PAGE_SIZE), false)
    }()

    internal @Volatile var closed = false;

    internal fun recidToSegment(recid:Long):Int{
        return (recid and segmentMask).toInt()
    }

    /** end of last record */
    internal var dataTail: Long
        get() = parity4Get(volume.getLong(DATA_TAIL_OFFSET))
        set(v:Long){
            if(CC.ASSERT && (v%16)!=0L)
                throw DBException.DataCorruption("unaligned data tail")
            if(CC.ASSERT)
                Utils.assertLocked(structuralLock)
            volume.putLong(DATA_TAIL_OFFSET, parity4Set(v))
        }

    /** maximal allocated recid */
    internal var maxRecid: Long
        get() = parity3Get(volume.getLong(INDEX_TAIL_OFFSET)).ushr(3)
        set(v:Long){
            if(CC.ASSERT)
                Utils.assertLocked(structuralLock)
            volume.putLong(INDEX_TAIL_OFFSET, parity3Set(v.shl(3)))
        }

    /** end of file (last allocated page) */
    internal var fileTail: Long
        get() = parity16Get(volume.getLong(FILE_TAIL_OFFSET))
        set(v:Long){
            if(CC.ASSERT)
                Utils.assertLocked(structuralLock)
            volume.putLong(FILE_TAIL_OFFSET, parity16Set(v))
        }

    internal val indexPages = LongArrayList()


    init{
        Utils.lock(structuralLock) {
            if (!volumeExistsAtStart) {
                //initialize values
                volume.ensureAvailable(CC.PAGE_SIZE)
                dataTail = 0L
                maxRecid = 0L
                fileTail = CC.PAGE_SIZE

                volume.putLong(FIRST_INDEX_PAGE_POINTER_OFFSET, parity16Set(0L))

                //initialize long stack master links
                for (offset in LONG_STACK_UNUSED1 until HEAD_END step 8) {
                    volume.putLong(offset, parity4Set(0L))
                }
                commit()
            } else {
                //load index pages
                var indexPagePointerOffset = FIRST_INDEX_PAGE_POINTER_OFFSET;
                while (true) {
                    val nextPage = parity16Get(volume.getLong(indexPagePointerOffset))
                    if (nextPage == 0L)
                        break;
                    if (CC.ASSERT && nextPage % CC.PAGE_SIZE != 0L)
                        throw DBException.DataCorruption("wrong page pointer")
                    indexPages.add(nextPage)
                    indexPagePointerOffset = nextPage + 8
                }
            }
        }

    }

    internal fun recidToOffset(recid2:Long):Long{
        val recid = recid2-1; //normalize recid so it starts from zero
        val pageNum = recid/RECIDS_PER_INDEX_PAGE
        return indexPages.get(pageNum.toInt()) + 16 + ((recid)%RECIDS_PER_INDEX_PAGE)*8
    }



    internal fun getIndexVal(recid:Long):Long{
        if(CC.PARANOID) //should be ASSERT, but this method is accessed way too often
            Utils.assertReadLock(locks[recidToSegment(recid)])

        try {
            val offset = recidToOffset(recid)
            return parity1Get(volume.getLong(offset));
        }catch (e:IndexOutOfBoundsException){
            throw DBException.GetVoid(recid);
        }
    }

    internal fun setIndexVal(recid:Long, value:Long){
        if(CC.ASSERT)
            Utils.assertWriteLock(locks[recidToSegment(recid)])

        val offset = recidToOffset(recid)
        volume.putLong(offset, parity1Set(value));
    }
    internal fun indexValCompose(size:Long,
                                  offset:Long,
                                  linked:Int,
                                  unused:Int,
                                  archive:Int
                                  ):Long{

        if(CC.ASSERT && size<0 || size>0xFFFF)
            throw AssertionError()

        if(CC.ASSERT && (offset%16) != 0L)
            throw DBException.DataCorruption("unaligned offset")

        if(CC.ASSERT && (offset and MOFFSET) != offset)
            throw DBException.DataCorruption("unaligned offset")


        if(CC.ASSERT && (linked in 0..1).not())
            throw AssertionError()
        if(CC.ASSERT && (archive in 0..1).not())
            throw AssertionError()
        if(CC.ASSERT && (unused in 0..1).not())
            throw AssertionError()

        return size.shl(48) + offset + linked*MLINKED + unused*MUNUSED + archive*MARCHIVE
    }


    internal fun <R> deserialize(serializer: Serializer<R>, di: DataInput2, size: Long): R? {
        try{
            val ret = serializer.deserialize(di, size.toInt());
            return ret
            //TODO assert number of bytes read
            //TODO wrap di, if untrusted serializer
        }catch(e: IOException){
            throw DBException.SerializationError(e)
        }
    }

    internal fun <R> serialize(record: R, serializer:Serializer<R>):DataOutput2{
        try {
            val out = DataOutput2()
            serializer.serialize(out, record);
            return out;
        }catch(e:IOException){
            throw DBException.SerializationError(e)
        }
    }

    internal fun allocateNewPage():Long{
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

    internal fun allocateNewIndexPage():Long{
        if(CC.ASSERT)
            Utils.assertLocked(structuralLock)


        val indexPage = allocateNewPage();

        //update pointer to previous page
        val pagePointerOffset =
                if(indexPages.isEmpty)
                    FIRST_INDEX_PAGE_POINTER_OFFSET
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

    internal fun allocateRecid():Long{
        if(CC.ASSERT)
            Utils.assertLocked(structuralLock)

        val reusedRecid = longStackTake(RECID_LONG_STACK,false)
        if(reusedRecid!=0L){
            //TODO ensure old value is zero
            return reusedRecid
        }

        val maxRecid2 = maxRecid;
        if(maxRecid2==0L) {
            allocateNewIndexPage()
            maxRecid = 1;
            return 1;
        }

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

    internal fun allocateData(size:Int, recursive:Boolean):Long{
        if(CC.ASSERT)
            Utils.assertLocked(structuralLock)

        if(CC.ASSERT && size>MAX_RECORD_SIZE)
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

    internal fun releaseData(size:Long, offset:Long, recursive:Boolean){
        if(CC.ASSERT)
            Utils.assertLocked(structuralLock)

        if(CC.ASSERT && size%16!=0L)
            throw AssertionError()
        if(CC.ASSERT && size>MAX_RECORD_SIZE)
            throw AssertionError()

        if(CC.ZEROS)
            volume.assertZeroes(offset, offset+size)

        freeSizeIncrement(size)

        longStackPut(longStackMasterLinkOffset(size), offset, recursive);
    }

    internal fun releaseRecid(recid:Long){
        longStackPut(RECID_LONG_STACK, recid, false)
    }

    internal fun indexValFlagLinked(indexValue:Long):Boolean{
        return indexValue and MLINKED != 0L
    }

    internal fun indexValFlagUnused(indexValue:Long):Boolean{
        return indexValue and MUNUSED != 0L
    }

    internal fun indexValFlagArchive(indexValue:Long):Boolean{
        return indexValue and MARCHIVE != 0L
    }


    internal fun linkedRecordGet(indexValue:Long):ByteArray{

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

    internal fun linkedRecordDelete(indexValue:Long){
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

    internal fun linkedRecordPut(output:ByteArray, size:Int):Long{
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


    internal fun longStackMasterLinkOffset(size: Long): Long {
        if (CC.ASSERT && size % 16 != 0L)
            throw AssertionError()
        if(CC.ASSERT && size>MAX_RECORD_SIZE)
            throw AssertionError()
        return size / 2 + RECID_LONG_STACK // really is size*8/16
    }


    internal fun longStackPut(masterLinkOffset:Long, value:Long, recursive:Boolean){
        if(CC.ASSERT)
            Utils.assertLocked(structuralLock)
        if (CC.ASSERT && (masterLinkOffset <= 0 || masterLinkOffset > CC.PAGE_SIZE || masterLinkOffset % 8 != 0L))
            throw DBException.DataCorruption("wrong master link")
        if(CC.ASSERT && value.shr(48)!=0L)
            throw AssertionError()
        if(CC.ASSERT && masterLinkOffset!=RECID_LONG_STACK && value % 16L !=0L)
            throw AssertionError()


        val masterLinkVal = parity4Get(volume.getLong(masterLinkOffset))
        if (masterLinkVal == 0L) {
            //empty stack, create new chunk
            longStackNewChunk(masterLinkOffset, 0L, value, true)
            return
        }
        val chunkOffset = masterLinkVal and MOFFSET
        val currSize = masterLinkVal.ushr(48)
        val prevLinkVal = parity4Get(volume.getLong(chunkOffset))
        val pageSize = prevLinkVal.ushr(48)

        //is there enough space in current chunk?
        if (currSize + 8 > pageSize) {
            //no there is not enough space
            //allocate new chunk
            longStackNewChunk(masterLinkOffset, chunkOffset, value, true) //TODO recursive=true here is too paranoid, and could be improved
            return
        }
        //there is enough free space here, so put it there
        volume.putLong(chunkOffset+currSize, value)
        //and update master link with new size
        val newMasterLinkValue = (currSize+8).shl(48) + chunkOffset
        volume.putLong(masterLinkOffset, parity4Set(newMasterLinkValue))
    }

    internal fun longStackNewChunk(masterLinkOffset: Long, prevPageOffset: Long, value: Long, recursive: Boolean) {
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
            val dataTail = dataTail
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
        //write size of current chunk with link to prev chunk
        volume.putLong(newChunkOffset, parity4Set((newChunkSize shl 48) + prevPageOffset))
        //put value
        volume.putLong(newChunkOffset+8, value)
        //update master link
        val newSize:Long = 8+8;
        val newMasterLinkValue = newSize.shl(48) + newChunkOffset
        volume.putLong(masterLinkOffset, parity4Set(newMasterLinkValue))
    }

    internal fun longStackTake(masterLinkOffset:Long, recursive:Boolean):Long {
        if(CC.ASSERT)
            Utils.assertLocked(structuralLock)

        if (CC.ASSERT && (masterLinkOffset <= 0 || masterLinkOffset > CC.PAGE_SIZE || masterLinkOffset % 8 != 0L))
            throw DBException.DataCorruption("wrong master link")

        val masterLinkVal = parity4Get(volume.getLong(masterLinkOffset))
        if (masterLinkVal == 0L) {
            //empty stack
            return 0;
        }

        val pos:Long = masterLinkVal.ushr(48)-8
        val offset = masterLinkVal and MOFFSET

        if(CC.ASSERT && pos<8L)
            throw DBException.DataCorruption("position too small")

        if(CC.ASSERT && volume.getLong(offset).ushr(48)<=pos)
            throw DBException.DataCorruption("position beyond chunk "+masterLinkOffset);

        //get value and zero it out
        val ret = volume.getLong(offset+pos)
        volume.putLong(offset+pos, 0L)

        //update size on master link
        if(pos>8L) {
            //there is enough space on current chunk, so just decrease its size
            volume.putLong(masterLinkOffset, parity4Set(pos.shl(48) + offset))
            if(CC.ASSERT && ret.shr(48)!=0L)
                throw AssertionError()
            if(CC.ASSERT && masterLinkOffset!= RECID_LONG_STACK && ret % 16 !=0L)
                throw AssertionError()

            return ret;
        }

        //current chunk become empty, so delete it
        val prevChunkValue = parity4Get(volume.getLong(offset))
        volume.putLong(offset, 0L)
        val currentSize = prevChunkValue.ushr(48)
        val prevChunkOffset = prevChunkValue and MOFFSET

        //does previous page exists?
        val masterLinkPos:Long = if (prevChunkOffset != 0L) {
            //yes previous page exists, return its size
            parity4Get(volume.getLong(prevChunkOffset)).ushr(48)
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
        if(CC.ASSERT && masterLinkOffset!=RECID_LONG_STACK && ret and 7 !=0L)
            throw AssertionError()
        return ret;
    }


    internal fun longStackForEach(masterLinkOffset: Long, body: (value: Long) -> Unit) {

        // assert first page
        val linkVal = parity4Get(volume.getLong(masterLinkOffset))
        var endSize = indexValToSize(linkVal)
        var offset = indexValToOffset(linkVal)


        while (offset != 0L) {
            var currHead = parity4Get(volume.getLong(offset))
            val currSize = indexValToSize(currHead)

            //iterate over values
            for (pos in 8 until endSize step 8) {
                val stackVal = volume.getLong(offset + pos)
                if (stackVal.ushr(48) != 0L)
                    throw AssertionError()
                if (masterLinkOffset!=RECID_LONG_STACK && stackVal % 16L != 0L)
                    throw AssertionError()
                body(stackVal)
            }

            //set values for next page
            offset = indexValToOffset(currHead)
            if (offset != 0L)
                endSize = indexValToSize(parity4Get(volume.getLong(offset)))
        }
    }

    override fun preallocate(): Long {
        assertNotClosed()
        val recid = Utils.lock(structuralLock){
            allocateRecid()
        }

        Utils.lockWrite(locks[recidToSegment(recid)]) {
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

        Utils.lockRead(locks[recidToSegment(recid)]) {
            val indexVal = getIndexVal(recid);

            if (indexValFlagLinked(indexVal)) {
                val di = linkedRecordGet(indexVal)
                return deserialize(serializer, DataInput2.ByteArray(di), di.size.toLong())
            }


            val size = indexValToSize(indexVal);
            if (size == DELETED_RECORD_SIZE)
                throw DBException.GetVoid(recid)

            if (size == NULL_RECORD_SIZE)
                return null;

            val offset = indexValToOffset(indexVal);

            val di =
                    if(size==0L) DataInput2.ByteArray(ByteArray(0))
                    else volume.getDataInput(offset, size.toInt())
            return deserialize(serializer, di, size)
        }
    }


    override fun getBinaryLong(recid:Long, f: StoreBinaryGetLong): Long {
        assertNotClosed()

        Utils.lockRead(locks[recidToSegment(recid)]) {
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

            val di = volume.getDataInput(offset, size.toInt())
            return f.get(di,size.toInt())
        }
    }

    override fun <R> put(record: R?, serializer: Serializer<R>): Long {
        assertNotClosed()

        val di =
                if(record==null) null
                else serialize(record, serializer);

        val recid = Utils.lock(structuralLock) {
            allocateRecid()
        }

        Utils.lockWrite(locks[recidToSegment(recid)]) {
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

            //allocate space for data
            val offset = if(di.pos==0) 0L
                else{
                Utils.lock(structuralLock) {
                    allocateData(roundUp(di.pos, 16), false)
                }
            }
            //and write data
            if(offset!=0L)
                volume.putData(offset, di.buf, 0, di.pos)

            setIndexVal(recid, indexValCompose(size = di.pos.toLong(), offset = offset, linked = 0, unused = 0, archive = 1))
            return recid;
        }
    }

    override fun <R> update(recid: Long, record: R?, serializer: Serializer<R>) {
        assertNotClosed()
        val di =
                if(record==null) null
                else serialize(record, serializer);

        Utils.lockWrite(locks[recidToSegment(recid)]) {
            updateInternal(recid, di)
        }
    }

    private fun updateInternal(recid: Long, di: DataOutput2?){
        if(CC.ASSERT)
            Utils.assertWriteLock(locks[recidToSegment(recid)])

        val oldIndexVal = getIndexVal(recid);
        val oldLinked = indexValFlagLinked(oldIndexVal);
        val oldSize = indexValToSize(oldIndexVal);
        if (oldSize == DELETED_RECORD_SIZE)
            throw DBException.GetVoid(recid)
        val newUpSize: Long = if (di == null) -16L else roundUp(di.pos.toLong(), 16)
        //try to reuse record if possible, if not possible, delete old record and allocate new
        if ((oldLinked || newUpSize != roundUp(oldSize, 16)) &&
                oldSize != NULL_RECORD_SIZE && oldSize != 0L ) {
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
                if (!oldLinked && newUpSize == roundUp(oldSize, 16) ) {
                    //reuse existing offset
                    indexValToOffset(oldIndexVal)
                } else if (size == 0) {
                    0L
                } else {
                    Utils.lock(structuralLock) {
                        allocateData(roundUp(size, 16), false)
                    }
                }
        volume.putData(offset, di.buf, 0, size)
        setIndexVal(recid, indexValCompose(size = size.toLong(), offset = offset, linked = 0, unused = 0, archive = 1))
        return
    }

    override fun <R> compareAndSwap(recid: Long, expectedOldRecord: R?, newRecord: R?, serializer: Serializer<R>): Boolean {
        assertNotClosed()
        Utils.lockWrite(locks[recidToSegment(recid)]) {
            //compare old value
            val old = get(recid, serializer)

            if (old === null && expectedOldRecord !== null)
                return false;
            if (old !== null && expectedOldRecord === null)
                return false;

            if (old !== expectedOldRecord && !serializer.equals(old!!, expectedOldRecord!!))
                return false

            val di =
                    if(newRecord==null) null
                    else serialize(newRecord, serializer);

            updateInternal(recid, di)
            return true;
        }
    }

    override fun <R> delete(recid: Long, serializer: Serializer<R>) {
        assertNotClosed()

        Utils.lockWrite(locks[recidToSegment(recid)]) {
            val oldIndexVal = getIndexVal(recid);
            val oldSize = indexValToSize(oldIndexVal);
            if (oldSize == DELETED_RECORD_SIZE)
                throw DBException.GetVoid(recid)

            if (oldSize != NULL_RECORD_SIZE) {
                Utils.lock(structuralLock) {
                    if (indexValFlagLinked(oldIndexVal)) {
                        linkedRecordDelete(oldIndexVal)
                    } else if(oldSize!=0L){
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
        Utils.lockWriteAll(locks)
        try{
            Utils.lock(structuralLock){
                //TODO use file for compaction, if store is file based
                val store2 = StoreDirect.make(isThreadSafe=false, concShift = 0)

                //first allocate enough index pages, so they are at beginning of store
                for(i in 0 until indexPages.size())
                    store2.allocateNewIndexPage()

                if(CC.ASSERT && store2.indexPages.size()!=indexPages.size())
                    throw AssertionError();

                //now iterate over all recids
                val maxRecid = maxRecid
                for (recid in 1..maxRecid) {
                    var data:ByteArray? = null;
                    var exist = true;
                    try{
                        data = get(recid, Serializer.BYTE_ARRAY_NOSIZE)
                        exist = true
                    } catch(e: Exception) {
                        //TODO better way to check for parity errors, EOF etc
                        exist = false
                    }

                    if(!exist) {
                        //recid does not exist, mark it as deleted in other store
                        store2.releaseRecid(recid)
                        store2.setIndexVal(recid, store2.indexValCompose(
                                size = DELETED_RECORD_SIZE, offset = 0L, linked = 0, unused = 0, archive = 1))
                    }else{
                        store2.putCompact(recid, data)
                    }
                }

                //finished, update some variables
                store2.maxRecid = maxRecid

                // copy content of volume
                //TODO it would be faster to just swap volumes or rename file, but that is concurrency issue
                val fileTail = store2.fileTail;
                volume.truncate(fileTail)

                for(page in 0 until fileTail step CC.PAGE_SIZE){
                    store2.volume.transferInto(page, volume, page, CC.PAGE_SIZE)
                }

                //take index pages from second store
                indexPages.clear()
                indexPages.addAll(store2.indexPages)
                //and update statistics
                freeSize.set(store2.freeSize.get());

                store2.close()
            }
        }finally{
            Utils.unlockWriteAll(locks)
        }
    }

    /** only called from compaction, it inserts new record under given recid */
    private fun putCompact(recid: Long, data: ByteArray?) {
        if(CC.ASSERT && isThreadSafe) //compaction is always thread unsafe
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
        val offset = if(data.size==0) 0L
        else{
            allocateData(roundUp(data.size, 16), false)
        }
        //and write data
        if(offset!=0L)
            volume.putData(offset, data, 0, data.size)

        setIndexVal(recid, indexValCompose(size = data.size.toLong(), offset = offset, linked = 0, unused = 0, archive = 1))
    }

    override fun commit() {
        assertNotClosed()
        volume.sync()
    }

    override fun close() {
        if(closed)
            return

        closed = true;
        volume.close()
    }

    override fun isClosed() = closed

    protected fun assertNotClosed(){
        if(closed)
            throw IllegalAccessError("Store was closed");
    }

    override fun getAllRecids(): LongIterator {
        val ret = LongArrayList()

        Utils.lockReadAll(locks)
        try {
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
        }finally{
            Utils.unlockReadAll(locks)
        }
        return ret.toArray().iterator()
    }


    override fun verify(){

        locks.forEach { it?.readLock()?.lock() }
        structuralLock?.lock()
        try {
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

            set(0, StoreDirectJava.HEAD_END, false)
            //TODO this section should be used by index page
            set(StoreDirectJava.HEAD_END, CC.PAGE_SIZE, true)

            if (dataTail % CC.PAGE_SIZE != 0L) {
                set(dataTail, roundUp(dataTail, CC.PAGE_SIZE), true)
            }


            //iterate over index pages and mark their head
            indexPages.forEach { indexPage ->
                set(indexPage, indexPage + 16, false)
                val end = Math.min(indexPage + CC.PAGE_SIZE, recidToOffset(maxRecid) + 8)
                for (indexOffset in indexPage + 16 until end step 8) {
                    //TODO preallocated versus deleted recids
                    set(indexOffset, indexOffset + 8, false)
                    var indexVal = parity1Get(volume.getLong(indexOffset))

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
                //if last index page, expect zeroes for unused part
                if (end < indexPage + CC.PAGE_SIZE) {
                    set(end, indexPage + CC.PAGE_SIZE, true)
                }
            }

            fun longStackForEach(masterLinkOffset: Long, body: (value: Long) -> Unit) {

                // assert first page
                val linkVal = parity4Get(volume.getLong(masterLinkOffset))
                var endSize = indexValToSize(linkVal)
                var offset = indexValToOffset(linkVal)


                while (offset != 0L) {
                    var currHead = parity4Get(volume.getLong(offset))
                    val currSize = indexValToSize(currHead)

                    //mark as used
                    set(offset, offset + currSize, false)
                    volume.assertZeroes(offset + endSize, offset + currSize)

                    //iterate over values
                    for (pos in 8 until endSize step 8) {
                        val stackVal = volume.getLong(offset + pos)
                        if (stackVal.ushr(48) != 0L)
                            throw AssertionError()
                        if (masterLinkOffset!=RECID_LONG_STACK && stackVal % 16L != 0L)
                            throw AssertionError()
                        body(stackVal)
                    }

                    //set values for next page
                    offset = indexValToOffset(currHead)
                    if (offset != 0L)
                        endSize = indexValToSize(parity4Get(volume.getLong(offset)))
                }
            }

            longStackForEach(RECID_LONG_STACK) { freeRecid ->
                //deleted recids should be marked separately

            }

            //iterate over free data
            for (size in 16..MAX_RECORD_SIZE step 16) {
                val masterLinkOffset = longStackMasterLinkOffset(size)
                longStackForEach(masterLinkOffset) { freeOffset ->
                    set(freeOffset, freeOffset + size, true)
                }
            }

            //ensure all data are set

            for (index in 0 until max) {
                if (bit.get(index.toInt()).not()) {
                    var len = 0;
                    while(bit.get(index.toInt()+len).not()){
                        len++;
                    }
                    throw AssertionError("not set at $index, for length $len - ${index % CC.PAGE_SIZE} - $dataTail - $fileTail")
                }
            }
        }finally{
            structuralLock?.unlock()
            locks.reversedArray().forEach { it?.readLock()?.unlock() }
        }

    }



    protected fun freeSizeIncrement(increment: Long) {
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

    internal fun calculateFreeSize(): Long {
        Utils.assertLocked(structuralLock)

        //traverse list of records
        var ret1 = 0L
        for (size in 16..MAX_RECORD_SIZE step 16) {
            val masterLinkOffset = longStackMasterLinkOffset(size)
            longStackForEach(masterLinkOffset) { v ->
                if(CC.ASSERT && v==0L)
                    throw AssertionError()

                ret1 += size
            }
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

}