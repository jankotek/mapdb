package net.kotek.jdbm;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * StorageDirect which provides transactions.
 * Index file data are stored in memory+trans log, phys file data are stored only in transaction log.
 *
 */
public class StorageTrans extends Storage implements RecordManager{

    protected static final long WRITE_INDEX_LONG = 1L <<48;
    protected static final long WRITE_INDEX_LONG_ZERO = 2L <<48;
    protected static final long WRITE_PHYS_LONG = 3L <<48;
    protected static final long WRITE_PHYS_BYTE = 4L <<48;
    protected static final long WRITE_PHYS_ARRAY = 5L <<48;
    protected static final long WRITE_SEAL = 111L <<48;
    public static final String TRANS_LOG_FILE_EXT = ".t";


    protected ByteBuffer2 transLog;
    protected long transLogOffset;


    protected long indexSize;
    protected long physSize;
    protected final LongHashMap<Long> recordLogRefs = new LongHashMap<Long>();
    protected final LongHashMap<Long> recordIndexVals = new LongHashMap<Long>();
    protected final long[] longStackCurrentPage = new long[INDEX_OFFSET_START];
    protected final int[] longStackCurrentPageSize = new int[INDEX_OFFSET_START];
    protected final long[][] longStackAdded = new long[INDEX_OFFSET_START][];
    protected final int[] longStackAddedSize = new int[INDEX_OFFSET_START];

    public StorageTrans(File indexFile, boolean enableLocks, boolean deleteFilesAfterClose, boolean readOnly) {
        super(indexFile,  enableLocks, deleteFilesAfterClose, readOnly);
        try{
            writeLock_lock();
            reloadIndexFile();
            replayLogFile();
        }finally{
            writeLock_unlock();
        }
    }


    protected void reloadIndexFile() {
        transLogOffset = 0;
        writeLock_checkLocked();
        recordLogRefs.clear();
        recordIndexVals.clear();
        indexSize = index.getLong(RECID_CURRENT_INDEX_FILE_SIZE *8);
        physSize = index.getLong(RECID_CURRENT_PHYS_FILE_SIZE*8);
        writeLock_checkLocked();
        for(int i = RECID_FREE_INDEX_SLOTS;i<INDEX_OFFSET_START; i++){
            longStackCurrentPage[i] = index.getLong(i*8) & PHYS_OFFSET_MASK;
            longStackCurrentPageSize[i] = phys.getUnsignedByte(longStackCurrentPage[i] );
            longStackAdded[i] = null;
            longStackAddedSize[i] = 0;
        }
    }

    @Override
    protected long longStackTake(long listRecid) {
        writeLock_checkLocked();
        final int r = (int) listRecid;
        if(longStackAddedSize[r]!=0){
            int offset = --longStackAddedSize[r];
            final long ret = longStackAdded[r][offset];
            if(offset!=0)
                longStackAdded[r][offset]=0;
            else
                longStackAdded[r]=null;
            return ret;
        }else if(longStackCurrentPage[r]!=0){
            long pageOffset = longStackCurrentPage[r]&PHYS_OFFSET_MASK;
            long ret = phys.getLong(pageOffset + 8 * (longStackCurrentPageSize[r]--));
            if(longStackCurrentPageSize[r]==0){
                //LongStack page is empty, so delete it and move to next
                final long indexValue = longStackCurrentPage[r] | (LONG_STACK_PAGE_SIZE <<48);
                long nextPage = phys.getLong(pageOffset)&PHYS_OFFSET_MASK;
                freePhysRecPut(indexValue);

                longStackCurrentPage[r] = nextPage==0?-1:nextPage;
                longStackCurrentPageSize[r] = (nextPage==0)? 0 :
                        phys.getUnsignedByte(nextPage);
            }
            return ret;
        }
        return 0;
    }

    @Override
    protected void longStackPut(long listRecid, long offset) {
        writeLock_checkLocked();
        final int r = (int) listRecid;
        //add to in-memory stack
        if(longStackAdded[r]==null){
            longStackAdded[r] = new long[]{offset,0,0,0};
            longStackAddedSize[r]=1;
            return;
        }
        if(longStackAdded[r].length==longStackAddedSize[r]){
            //grow
            longStackAdded[r] = Arrays.copyOf(longStackAdded[r], longStackAdded[r].length*2);
        }
        longStackAdded[r][longStackAddedSize[r]++] = offset;
    }


    protected long freePhysRecTake(final int requiredSize){
        writeLock_checkLocked();

        if(CC.ASSERT && requiredSize<=0) throw new InternalError();

        long freePhysRec = findFreePhysSlot(requiredSize);
        if(freePhysRec!=0)
            return freePhysRec;


            //No free records found, so lets increase the file size.
            //We need to take case of growing ByteBuffers.
            // Also max size of ByteBuffer is 2GB, so we need to use multiple ones


            if(CC.ASSERT && physSize <=0) throw new InternalError("illegal file size:"+physSize);

            //check if new record would be overflowing BUF_SIZE
            if(physSize%ByteBuffer2.BUF_SIZE+requiredSize<=ByteBuffer2.BUF_SIZE){
                //no, so just increase file size
                long oldPhysSize = physSize;
                physSize +=requiredSize;
                //and return this
                return (((long)requiredSize)<<48) | oldPhysSize;
            }else{
                //new size is overlapping 2GB ByteBuffer size
                //so we need to create empty record for 'padding' size to 2GB

                final long  freeSizeToCreate = ByteBuffer2.BUF_SIZE -  physSize%ByteBuffer2.BUF_SIZE;
                if(CC.ASSERT && freeSizeToCreate == 0) throw new InternalError();

                final long nextBufferStartOffset = physSize + freeSizeToCreate;
                if(CC.ASSERT && nextBufferStartOffset%ByteBuffer2.BUF_SIZE!=0) throw new InternalError();

                //increase the disk size

                //mark 'padding' free record
                freePhysRecPut(freeSizeToCreate<<48|physSize);
                physSize +=requiredSize+freeSizeToCreate;
                //and finally return position at beginning of new buffer
                return (((long)requiredSize)<<48) | nextBufferStartOffset;
            }

    }

    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        try{
            DataOutput2 out = new DataOutput2();
            serializer.serialize(out,value);
            if(CC.ASSERT && out.pos>1<<16) throw new InternalError("Record bigger then 64KB");

            try{
                writeLock_lock();
                //update index file, find free recid
                long recid = longStackTake(RECID_FREE_INDEX_SLOTS);
                if(recid == 0){
                    //could not reuse recid, so create new one
                    if(CC.ASSERT && indexSize%8!=0) throw new InternalError();
                    recid = indexSize/8;
                    indexSize+=8;
                }

                //get physical record
                // first 16 bites is record size, remaining 48 bytes is record offset in phys file
                final long indexValue = out.pos!=0?
                        freePhysRecTake(out.pos):0L;
                writeIndexValToTransLog(recid, indexValue);

                //write new phys data into trans log
                writeOutToTransLog(out, recid, indexValue);

                return recid;
            }finally {
                writeLock_unlock();
            }
        }catch(IOException e){
            throw new IOError(e);
        }
    }

    protected void writeIndexValToTransLog(long recid, long indexValue) throws IOException {
        //write new index value into transaction log
        transLog.ensureAvailable(transLogOffset+16);
        transLog.putLong(transLogOffset, WRITE_INDEX_LONG | (recid * 8));
        transLogOffset+=8;
        transLog.putLong(transLogOffset, indexValue);
        transLogOffset+=8;
        recordIndexVals.put(recid,indexValue);
    }

    protected void writeOutToTransLog(DataOutput2 out, long recid, long indexValue) throws IOException {
        transLog.ensureAvailable(transLogOffset+10+out.pos);
        transLog.putLong(transLogOffset, WRITE_PHYS_ARRAY|(indexValue&PHYS_OFFSET_MASK));
        transLogOffset+=8;
        transLog.putUnsignedShort(transLogOffset, out.pos);
        transLogOffset+=2;
        final Long transLogReference = (((long)out.pos)<<48)|transLogOffset;
        recordLogRefs.put(recid, transLogReference); //store reference to transaction log, so we can load data quickly
        transLog.putData(transLogOffset,out);
        transLogOffset+=out.pos;
    }


    @Override
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        try{
            readLock_lock();

            Long indexVal = recordLogRefs.get(recid);
            if(indexVal!=null){
                if(indexVal.longValue() == Long.MIN_VALUE)
                    return null; //was deleted
                //record is in transaction log
                return recordGet2(indexVal, transLog, serializer);
            }else{
                //not in transaction log, read from file
                final long indexValue = index.getLong(recid*8) ;
                return recordGet2(indexValue, phys, serializer);
            }
        }catch(IOException e){
            throw new IOError(e);
        }finally{
            readLock_unlock();
        }
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        try{
            DataOutput2 out = new DataOutput2();
            serializer.serialize(out,value);

            if(CC.ASSERT && out.pos>1<<16) throw new InternalError("Record bigger then 64KB");
            try{
                writeLock_lock();

                //check if size has changed
                Long oldIndexVal = recordIndexVals.get(recid);
                if(oldIndexVal==null)
                    oldIndexVal = index.getLong(recid * 8);

                long oldSize = oldIndexVal>>>48;

                if(oldSize == 0 && out.pos==0){
                    //do nothing
                } else if(oldSize == out.pos ){
                    //size is the same, so just write new data
                    writeOutToTransLog(out, recid, oldIndexVal);
                }else if(oldSize != 0 && out.pos==0){
                    //new record has zero size, just delete old phys one
                    freePhysRecPut(oldIndexVal);
                    writeIndexValToTransLog(recid, 0L);
                }else{
                    //size has changed, so write into new location
                    final long newIndexValue = freePhysRecTake(out.pos);

                    writeOutToTransLog(out, recid, newIndexValue);
                    //update index file with new location
                    writeIndexValToTransLog(recid, newIndexValue);

                    //and set old phys record as free
                    if(oldSize!=0)
                        freePhysRecPut(oldIndexVal);
                }
            }finally {
                writeLock_unlock();
            }
        }catch(IOException e){
            throw new IOError(e);
        }

    }

    @Override
    public void recordDelete(long recid){
        try{
            writeLock_lock();
            transLog.ensureAvailable(transLogOffset+8);
            transLog.putLong(transLogOffset, WRITE_INDEX_LONG_ZERO | (recid*8));
            transLogOffset+=8;
            longStackPut(RECID_FREE_INDEX_SLOTS,recid);
            recordLogRefs.put(recid, Long.MIN_VALUE);
            //check if is in transaction
            Long transIndexVal = recordIndexVals.get(recid);
            if(transIndexVal!=null){
                //is in transaction, so remove from transaction and wipe data
                recordIndexVals.put(recid,0L);

                if(transIndexVal.longValue()!=0)
                    freePhysRecPut(transIndexVal);
                return;
            }else{
                long oldIndexVal = index.getLong(recid * 8);
                if(oldIndexVal!=0)
                    freePhysRecPut(oldIndexVal);
            }
        }catch(IOException e){
            throw new IOError(e);
        }finally {
            writeLock_unlock();
        }
    }


    @Override
    public void close() {
        super.close();

        try{
            transLog.close();
            transLog = null;
            if(deleteFilesOnExit && indexFile!=null){
                new File(indexFile.getPath()+TRANS_LOG_FILE_EXT).delete();
            }

        }catch(IOException e){
            throw new IOError(e);
        }

        //delete log?
    }

    @Override
    public void commit() {
        try{
            writeLock_lock();

            //update LongStack sizes and page addresses
            for(int recid=RECID_FREE_INDEX_SLOTS; recid<longStackAddedSize.length; recid++){
                //compare and update if needed
                long newPage = longStackCurrentPage[recid];
                if(newPage == 0) continue;
                long realPage = index.getLong(recid*8);
                if(realPage !=newPage){
                    if(newPage==-1)
                        writeIndexValToTransLog(recid, 0);
                    else
                        writeIndexValToTransLog(recid, newPage | (1L*LONG_STACK_PAGE_SIZE)<<48);
                }

                if(newPage!=-1){
                    int realPageCount = phys.getUnsignedByte(newPage);
                    if(realPageCount!= longStackCurrentPageSize[recid]){
                        transLog.ensureAvailable(transLogOffset+9);
                        transLog.putLong(transLogOffset,WRITE_PHYS_BYTE | newPage);
                        transLogOffset+=8;
                        transLog.putUnsignedByte(transLogOffset, (byte) realPageCount);
                        transLogOffset+=1;
                    }
                }

            }



            //count number of pages to preallocate
            int pagesNeeded = 0;
            int oldFreePhysLongStackAddedSize = longStackAddedSize[RECID_FREE_PHYS_RECORDS_START + LONG_STACK_PAGE_SIZE];
            for(int recid = RECID_FREE_INDEX_SLOTS;recid<longStackAdded.length;recid++ ){
                int addedCount = longStackAddedSize[recid];
                if(addedCount!=0){
                    pagesNeeded += 1 + (addedCount-1)/LONG_STACK_NUM_OF_RECORDS_PER_PAGE;
                }
            }
            if(oldFreePhysLongStackAddedSize != longStackAddedSize[RECID_FREE_PHYS_RECORDS_START + LONG_STACK_PAGE_SIZE]){
                throw new InternalError();
            }


            //preallocate pages
            long[] preallocPages = new long[pagesNeeded];
            for(int i=0; i<pagesNeeded;i++){
                preallocPages[i] = freePhysRecTake(LONG_STACK_PAGE_SIZE) & PHYS_OFFSET_MASK;
            }

            int preallocPagesPos = 0;


            //now write values added into long stacks
            for(int recid=RECID_FREE_INDEX_SLOTS; recid<longStackAdded.length; recid++){
                if(longStackAddedSize[recid]==0) continue;
                long page = longStackCurrentPage[recid];
                int pageSize = longStackCurrentPageSize[recid];
                if(page==0){
                    //use preallocated page
                    page = preallocPages[preallocPagesPos++];
                    writeIndexValToTransLog(recid, (((long)LONG_STACK_PAGE_SIZE)<<48) | page);
                    pageSize=0;
                }

                for(int pos=0; pos<longStackAddedSize[recid]; pos++){
                    if(pageSize == LONG_STACK_NUM_OF_RECORDS_PER_PAGE){
                        //overflow to next page
                        long oldPage = page;
                        //get new page and write reference to old one
                        page = preallocPages[preallocPagesPos++];
                        transLog.ensureAvailable(transLogOffset+16);
                        transLog.putLong(transLogOffset, WRITE_PHYS_LONG | page);
                        transLogOffset+=8;
                        transLog.putLong(transLogOffset, oldPage);
                        transLogOffset+=8;
                        //update index reference to this new page
                        writeIndexValToTransLog(recid, page | (((long)LONG_STACK_PAGE_SIZE)<<48));
                        //reset counter
                        pageSize = 0;
                    }

                    //write new page size
                    transLog.ensureAvailable(transLogOffset+9);
                    transLog.putLong(transLogOffset,WRITE_PHYS_BYTE | page);
                    transLogOffset+=8;
                    transLog.putUnsignedByte(transLogOffset, (byte) (++pageSize));
                    transLogOffset+=1;

                    //write long value
                    long value = longStackAdded[recid][pos];
                    transLog.putLong(transLogOffset, WRITE_PHYS_LONG | (page+ (pageSize)*8));
                    transLogOffset+=8;
                    transLog.putLong(transLogOffset, value);
                    transLogOffset+=8;

                }
            }


            //update physical and logical filesize
            writeIndexValToTransLog(RECID_CURRENT_PHYS_FILE_SIZE, physSize);
            writeIndexValToTransLog(RECID_CURRENT_INDEX_FILE_SIZE, indexSize);


            //seal log file
            transLog.ensureAvailable(transLogOffset+8);
            transLog.putLong(transLogOffset, WRITE_SEAL);
            transLogOffset+=8;

            replayLogFile();
            reloadIndexFile();

        }catch(IOException e){
            throw new IOError(e);
        }finally{
            writeLock_unlock();
        }
    }

    protected void replayLogFile() {
        try {
            writeLock_checkLocked();
            transLogOffset = 0;

            if(transLog==null){
                if(inMemory){
                    transLog = new ByteBuffer2(true, null, readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE,"trans");
                    return;
                }else{
                    RandomAccessFile r =  new RandomAccessFile(new File(indexFile.getPath()+TRANS_LOG_FILE_EXT),"rw");
                    transLog = new ByteBuffer2(false,r.getChannel(), readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE,"trans");
                    if(r.length()==0)
                        return;
                }
            }

            long ins = transLog.getLong(transLogOffset);
            transLogOffset+=8;

            while(ins!=WRITE_SEAL && ins!=0){

                final long offset = ins&PHYS_OFFSET_MASK;
                ins -=offset;

                if(ins == WRITE_INDEX_LONG_ZERO){
                    index.ensureAvailable(offset+8);
                    index.putLong(offset, 0L);
                }else if(ins == WRITE_INDEX_LONG){
                    final long value = transLog.getLong(transLogOffset);
                    transLogOffset+=8;
                    index.ensureAvailable(offset+8);
                    index.putLong(offset, value);
                }else if(ins == WRITE_PHYS_LONG){
                    final long value = transLog.getLong(transLogOffset);
                    transLogOffset+=8;
                    phys.ensureAvailable(offset+8);
                    phys.putLong(offset, value);
                }else if(ins == WRITE_PHYS_BYTE){
                    final int value = transLog.getUnsignedByte(transLogOffset);
                    transLogOffset+=1;
                    phys.ensureAvailable(offset+1);
                    phys.putUnsignedByte(offset, (byte) value);
                }else if(ins == WRITE_PHYS_ARRAY){
                    final int size = transLog.getUnsignedShort(transLogOffset);
                    transLogOffset+=2;

                    //transfer byte[] directly from log file without copying into memory
                    final ByteBuffer blog = transLog.internalByteBuffer(transLogOffset);
                    int pos = (int) (transLogOffset% ByteBuffer2.BUF_SIZE);
                    blog.position(pos);
                    blog.limit(pos+size);
                    phys.ensureAvailable(offset+size);
                    final ByteBuffer bphys = phys.internalByteBuffer(offset);
                    bphys.position((int) (offset% ByteBuffer2.BUF_SIZE));
                    bphys.put(blog);
                    transLogOffset+=size;
                    blog.clear();
                    bphys.clear();

                }else{
                    throw new InternalError("unknown trans log instruction: "+(ins>>>48));
                }

                ins = transLog.getLong(transLogOffset);
                transLogOffset+=8;
            }
            transLogOffset=0;
        } catch (IOException e) {
            throw new IOError(e);
        }

    }


    @Override
    public void rollback() {
        reloadIndexFile();
    }
}
