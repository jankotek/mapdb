package net.kotek.jdbm;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.Arrays;

/**
 * RecordStore which provides transactions.
 * Index file data are stored in memory+trans log, phys file data are stored only in transaction log.
 *
 */
public class RecordStoreTrans extends RecordStoreAbstract implements RecordManager{

    protected static final long WRITE_INDEX_LONG = 1 <<48;
    protected static final long WRITE_INDEX_LONG_ZERO = 2 <<48;
    protected static final long WRITE_PHYS_LONG = 3 <<48;
    protected static final long WRITE_PHYS_BYTE = 4 <<48;
    protected static final long WRITE_PHYS_ARRAY = 5 <<48;


    protected ByteBuffer2 transLog = new ByteBuffer2(true, null,null);
    protected long transLogOffset = 0;


    protected long indexSize;
    protected long physSize;
    protected final LongHashMap<Long> recordLogRefs = new LongHashMap<Long>();
    protected final LongHashMap<Long> recordIndexVals = new LongHashMap<Long>();
    protected final long[] longStackCurrentPage = new long[INDEX_OFFSET_START];
    protected final int[] longStackCurrentPageSize = new int[INDEX_OFFSET_START];
    protected final long[][] longStackAdded = new long[INDEX_OFFSET_START][];
    protected final int[] longStackAddedSize = new int[INDEX_OFFSET_START];

    public RecordStoreTrans(File indexFile, boolean enableLocks) {
        super(indexFile,  enableLocks);
        reloadIndexFile();
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
            longStackCurrentPage[i] = index.getLong(i*8);
            longStackCurrentPageSize[i] = phys.getUnsignedByte(longStackCurrentPage[i] & PHYS_OFFSET_MASK);
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

                longStackCurrentPage[r] = nextPage;
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

    protected void writeIndexValToTransLog(long recid, long indexValue) {
        //write new index value into transaction log
        transLog.putLong(transLogOffset, WRITE_INDEX_LONG | (recid * 8));
        transLogOffset+=8;
        transLog.putLong(transLogOffset, indexValue);
        transLogOffset+=8;
        recordIndexVals.put(recid,indexValue);
    }

    protected void writeOutToTransLog(DataOutput2 out, long recid, long indexValue) {
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

            //TODO special handling for zero size records
            if(CC.ASSERT && out.pos>1<<16) throw new InternalError("Record bigger then 64KB");
            try{
                writeLock_lock();

                //check if size has changed
                Long oldIndexVal = recordIndexVals.get(recid);
                if(oldIndexVal==null)
                    oldIndexVal = index.getLong(recid * 8);

                if(oldIndexVal >>>48 == out.pos ){
                    //size is the same, so just write new data
                    writeOutToTransLog(out, recid, oldIndexVal);

                }else{
                    //size has changed, so write into new location
                    final long newIndexValue = freePhysRecTake(out.pos);

                    writeOutToTransLog(out, recid, newIndexValue);
                    //update index file with new location
                    writeIndexValToTransLog(recid, newIndexValue);

                    //and set old phys record as free
                    if(oldIndexVal!=0)
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
        }finally {
            writeLock_unlock();
        }
    }


    @Override
    public void close() {
        super.close();

        //delete log?
    }

    @Override
    public void commit() {
        //TODO commit
    }

    @Override
    public void rollback() {
        reloadIndexFile();
    }
}
