/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.mapdb;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * StorageDirect which provides transaction and journal.
 * Index file data are stored in memory+trans journal, phys file data are stored only in transaction journal.
 *
 * @author Jan Kotek
 */
public class StorageJournaled extends StorageDirect implements Engine {

    protected static final long WRITE_INDEX_LONG = 1L <<48;
    protected static final long WRITE_INDEX_LONG_ZERO = 2L <<48;
    protected static final long WRITE_PHYS_LONG = 3L <<48;
    protected static final long WRITE_PHYS_ARRAY = 4L <<48;

    protected static final long WRITE_SKIP_BUFFER = 444L <<48;
    /** last instruction in log file */
    protected static final long WRITE_SEAL = 111L <<48;
    /** added to offset 8 into log file, indicates that write was successful*/
    protected static final long LOG_SEAL = 4566556446554645L;
    public static final String TRANS_LOG_FILE_EXT = ".t";


    protected Volume transLog;
    protected final Volume.Factory volFac;
    protected long transLogOffset;


    protected long indexSize;
    protected long physSize;
    protected final LongMap<long[]> recordLogRefs = new LongHashMap<long[]>();
    protected final LongMap<Long> recordIndexVals = new LongHashMap<Long>();
    protected final LongMap<long[]> longStackPages = new LongHashMap<long[]>();
    protected final LongMap<ArrayList<Long>> transLinkedPhysRecods = new LongHashMap<ArrayList<Long>>();


    public StorageJournaled(Volume.Factory volFac){
        this(volFac, false, false, false, false);
    }

    public StorageJournaled(Volume.Factory volFac, boolean appendOnly,
                            boolean deleteFilesOnExit, boolean failOnWrongHeader, boolean readOnly) {
        super(volFac,  appendOnly, deleteFilesOnExit, failOnWrongHeader, readOnly);
        lock.writeLock().lock();
        try{
            this.volFac = volFac;
            this.transLog = volFac.createTransLogVolume();
            reloadIndexFile();
            replayLogFile();
            transLog = null;
        }finally{
            lock.writeLock().unlock();
        }
    }


    protected void reloadIndexFile() {
        transLogOffset = 0;
        writeLock_checkLocked();
        recordLogRefs.clear();
        recordIndexVals.clear();
        longStackPages.clear();
        transLinkedPhysRecods.clear();
        indexSize = index.getLong(RECID_CURRENT_INDEX_FILE_SIZE *8);
        physSize = index.getLong(RECID_CURRENT_PHYS_FILE_SIZE*8);
        writeLock_checkLocked();
    }

    protected void openLogIfNeeded(){
       if(transLog!=null) return;
       transLog = volFac.createTransLogVolume();
       transLog.ensureAvailable(16);
       transLog.putLong(0, HEADER);
       transLog.putLong(8, 0L);
       transLogOffset = 16;
    }





    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        if(value==null||serializer==null) throw new NullPointerException();
        try{
            DataOutput2 out = new DataOutput2();
            serializer.serialize(out,value);

            try{
                lock.writeLock().lock();
                //update index file, find free recid
                long recid = longStackTake(RECID_FREE_INDEX_SLOTS);
                if(recid == 0){
                    //could not reuse recid, so create new one
                    if(indexSize%8!=0) throw new InternalError();
                    recid = indexSize/8;
                    indexSize+=8;
                }

                if(out.pos<MAX_RECORD_SIZE){
                    //get physical record
                    // first 16 bites is record size, remaining 48 bytes is record offset in phys file
                    final long indexValue = out.pos!=0?
                        freePhysRecTake(out.pos):0L;
                    writeIndexValToTransLog(recid, indexValue);

                    //write new phys data into trans log
                    writeOutToTransLog(out, recid, indexValue);
                    checkBufferRounding();
                }else{
                    putLargeLinkedRecord(out, recid);
                }



                return recid-INDEX_OFFSET_START;
            }finally {
                lock.writeLock().unlock();
            }
        }catch(IOException e){
            throw new IOError(e);
        }
    }

    private void putLargeLinkedRecord(DataOutput2 out, long recid) throws IOException {
        openLogIfNeeded();
        //large size, needs to link multiple records together
        //start splitting from end, so we can build up linked list
        final int chunkSize = MAX_RECORD_SIZE-8;
        int lastArrayPos = out.pos;
        int arrayPos = out.pos - out.pos%chunkSize;
        long lastChunkPhysId = 0;
        ArrayList<Long> journalRefs = new ArrayList<Long>();
        ArrayList<Long> physRecords = new ArrayList<Long>();
        while(arrayPos>=0){
            final int currentChunkSize = lastArrayPos-arrayPos;
            byte[] b = new byte[currentChunkSize+8]; //TODO reuse byte[]
            //append reference to prev physId
            ByteBuffer.wrap(b).putLong(0, lastChunkPhysId);
            //copy chunk
            System.arraycopy(out.buf, arrayPos, b, 8, currentChunkSize);
            //and write current chunk
            lastChunkPhysId = freePhysRecTake(currentChunkSize+8);
            physRecords.add(lastChunkPhysId);
            //phys.putData(lastChunkPhysId&PHYS_OFFSET_MASK, b, b.length);

            transLog.ensureAvailable(transLogOffset+10+currentChunkSize+8);
            transLog.putLong(transLogOffset, WRITE_PHYS_ARRAY|(lastChunkPhysId&PHYS_OFFSET_MASK));
            transLogOffset+=8;
            transLog.putUnsignedShort(transLogOffset, currentChunkSize+8);
            transLogOffset+=2;
            final Long transLogReference = (((long)currentChunkSize)<<48)|(transLogOffset+8);
            journalRefs.add(transLogReference);
            transLog.putData(transLogOffset,b, b.length);
            transLogOffset+=b.length;

            checkBufferRounding();

            lastArrayPos = arrayPos;
            arrayPos-=chunkSize;
        }
        transLinkedPhysRecods.put(recid,physRecords);
        writeIndexValToTransLog(recid, lastChunkPhysId);
        long[] journalRefs2 = new long[journalRefs.size()];
        for(int i=0;i<journalRefs2.length;i++){
            journalRefs2[i] = journalRefs.get(i);
        }
        recordLogRefs.put(recid, journalRefs2);
    }

    protected void checkBufferRounding() throws IOException {
        if(transLogOffset%Volume.BUF_SIZE > Volume.BUF_SIZE - MAX_RECORD_SIZE*2){
            //position is to close to end of ByteBuffers (1GB)
            //so start writing into new buffer
            transLog.ensureAvailable(transLogOffset+8);
            transLog.putLong(transLogOffset,WRITE_SKIP_BUFFER);
            transLogOffset += Volume.BUF_SIZE-transLogOffset%Volume.BUF_SIZE;
        }
    }

    protected void writeIndexValToTransLog(long recid, long indexValue) throws IOException {
        //write new index value into transaction log
        openLogIfNeeded();
        transLog.ensureAvailable(transLogOffset+16);
        transLog.putLong(transLogOffset, WRITE_INDEX_LONG | (recid * 8));
        transLogOffset+=8;
        transLog.putLong(transLogOffset, indexValue);
        transLogOffset+=8;
        recordIndexVals.put(recid,indexValue);
    }

    protected void writeOutToTransLog(DataOutput2 out, long recid, long indexValue) throws IOException {
        openLogIfNeeded();
        transLog.ensureAvailable(transLogOffset+10+out.pos);
        transLog.putLong(transLogOffset, WRITE_PHYS_ARRAY|(indexValue&PHYS_OFFSET_MASK));
        transLogOffset+=8;
        transLog.putUnsignedShort(transLogOffset, out.pos);
        transLogOffset+=2;
        final long transLogReference = (((long)out.pos)<<48)|transLogOffset;
        recordLogRefs.put(recid, new long[]{transLogReference}); //store reference to transaction log, so we can load data quickly
        transLog.putData(transLogOffset,out.buf, out.pos);
        transLogOffset+=out.pos;
    }


    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        if(serializer==null)throw new NullPointerException();
        if(recid<=0) throw new IllegalArgumentException("recid");
        recid+=INDEX_OFFSET_START;

        try{
            lock.readLock().lock();

            long[] indexVals = recordLogRefs.get(recid);
            if(indexVals!=null){
                if(indexVals.length==1){
                    //single record
                    if(indexVals[0] == Long.MIN_VALUE)
                        return null; //was deleted
                    //record is in transaction log
                    return recordGet2(indexVals[0], transLog, serializer);
                }else{
                    //read linked record from journal
                    //first calculate total size
                    int size = 0;
                    for(long physId:indexVals) size+= physId>>>48;
                    byte[] b = new byte[size];
                    //now load it in chunks
                    int pos = 0;
                    for(long physId:indexVals){
                        int curChunkSize = (int) (physId>>>48);
                        long offset = physId&PHYS_OFFSET_MASK;
                        DataInput2 in = transLog.getDataInput(offset, curChunkSize);
                        in.readFully(b,pos,curChunkSize);
                        pos+=curChunkSize;
                    }
                    if(size!=pos) throw new InternalError();
                    //now deserialize
                    DataInput2 in = new DataInput2(b);
                    A ret = serializer.deserialize(in, size);
                    if(in.pos!=size) throw new InternalError("Data were not fully read");
                    return ret;
                }
            }else{
                //not in transaction log, read from file
                final long indexValue = index.getLong(recid*8) ;
                 return recordGet2(indexValue, phys, serializer);
            }
        }catch(IOException e){
            throw new IOError(e);
        }finally{
            lock.readLock().unlock();
        }
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        if(value==null||serializer==null) throw new NullPointerException();
        if(recid<=0) throw new IllegalArgumentException("recid");
        recid+=INDEX_OFFSET_START;

        try{
            DataOutput2 out = new DataOutput2();
            serializer.serialize(out,value);

            try{
                lock.writeLock().lock();

                //check if size has changed
                long oldIndexVal = getIndexLong(recid);
                long oldSize = oldIndexVal>>>48;

                //check if we need to split new records into multiple one
                if(out.pos<MAX_RECORD_SIZE){
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
                        unlinkPhysRecord(oldIndexVal,recid);
                    }
                }else{
                    unlinkPhysRecord(oldIndexVal,recid); //unlink must be first to release currently used space
                    putLargeLinkedRecord(out, recid);
                }


                checkBufferRounding();
            }finally {
                lock.writeLock().unlock();
            }
        }catch(IOException e){
            throw new IOError(e);
        }

    }

    private long getIndexLong(long recid) {
        Long v = recordIndexVals.get(recid);
        return (v!=null) ? v :
             index.getLong(recid * 8);
    }

    @Override
    public <A> void delete(long recid, Serializer<A>  serializer){
        if(serializer==null) throw new NullPointerException();
        if(recid<=0) throw new IllegalArgumentException("recid");
        recid+=INDEX_OFFSET_START;

        try{
            lock.writeLock().lock();
            openLogIfNeeded();

            transLog.ensureAvailable(transLogOffset+8);
            transLog.putLong(transLogOffset, WRITE_INDEX_LONG_ZERO | (recid*8));
            transLogOffset+=8;
            longStackPut(RECID_FREE_INDEX_SLOTS,recid);
            recordLogRefs.put(recid, new long[]{Long.MIN_VALUE});
            //check if is in transaction
            long oldIndexVal = getIndexLong(recid);
            recordIndexVals.put(recid,0L);
            unlinkPhysRecord(oldIndexVal,recid);


            checkBufferRounding();

        }catch(IOException e){
            throw new IOError(e);
        }finally {
            lock.writeLock().unlock();
        }
    }


    @Override
    public void close() {
        super.close();

        if(transLog!=null){
             transLog.sync();
             transLog.close();
             if(deleteFilesOnExit){
                transLog.deleteFile();
            }
        }

        transLog = null;
        //TODO delete trans log logic
    }

    @Override
    public void commit() {
        try{
            lock.writeLock().lock();

            //dump long stack pages
            LongMap.LongMapIterator<long[]> iter = longStackPages.longMapIterator();
            while(iter.moveToNext()){
                transLog.ensureAvailable(transLogOffset+8+2+LONG_STACK_PAGE_SIZE);
                transLog.putLong(transLogOffset, WRITE_PHYS_ARRAY|iter.key());
                transLogOffset+=8;
                transLog.putUnsignedShort(transLogOffset, LONG_STACK_PAGE_SIZE);
                transLogOffset+=2;
                for(long l:iter.value()){
                    transLog.putLong(transLogOffset, l);
                    transLogOffset+=8;
                }
                checkBufferRounding();
            }

            //update physical and logical filesize
            writeIndexValToTransLog(RECID_CURRENT_PHYS_FILE_SIZE, physSize);
            writeIndexValToTransLog(RECID_CURRENT_INDEX_FILE_SIZE, indexSize);


            //seal log file
            transLog.ensureAvailable(transLogOffset+8);
            transLog.putLong(transLogOffset, WRITE_SEAL);
            transLogOffset+=8;
            //flush log file
            transLog.sync();
            //and write mark it was sealed
            transLog.putLong(8, LOG_SEAL);
            transLog.sync();

            replayLogFile();
            reloadIndexFile();

        }catch(IOException e){
            throw new IOError(e);
        }finally{
            lock.writeLock().unlock();
        }
    }

    protected void replayLogFile(){

            writeLock_checkLocked();
            transLogOffset = 0;

            if(transLog!=null){
                transLog.sync();
            }


            //read headers
            if(transLog.isEmpty() || transLog.getLong(0)!=HEADER || transLog.getLong(8) !=LOG_SEAL){
                //wrong headers, discard log
                transLog.close();
                transLog.deleteFile();
                transLog = null;
                return;
            }


            //all good, start replay
            transLogOffset=16;
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
                }else if(ins == WRITE_PHYS_ARRAY){
                    final int size = transLog.getUnsignedShort(transLogOffset);
                    transLogOffset+=2;
                    //transfer byte[] directly from log file without copying into memory
                    DataInput2 input = transLog.getDataInput(transLogOffset, size);
                    synchronized (input.buf){
                        input.buf.position(input.pos);
                        input.buf.limit(input.pos+size);
                        phys.ensureAvailable(offset+size);
                        phys.putData(offset, input.buf);
                        input.buf.clear();
                    }
                    transLogOffset+=size;
                }else if(ins == WRITE_SKIP_BUFFER){
                    transLogOffset += Volume.BUF_SIZE-transLogOffset%Volume.BUF_SIZE;
                }else{
                    throw new InternalError("unknown trans log instruction: "+(ins>>>48));
                }

                ins = transLog.getLong(transLogOffset);
                transLogOffset+=8;
            }
            transLogOffset=0;

            //flush dbs
            phys.sync();
            index.sync();
            //and discard log
            transLog.putLong(0, 0);
            transLog.putLong(8, 0); //destroy seal to prevent log file from being replayed
            transLog.close();
            transLog.deleteFile();
            transLog = null;
    }


    @Override
    public void rollback() {
        lock.writeLock().lock();
        try{
        //discard trans log
        if(transLog!=null){
            transLog.close();
            transLog.deleteFile();
            transLog = null;
        }

        reloadIndexFile();
        }finally{
            lock.writeLock().unlock();
        }

    }

    @Override
    public void compact() {
        lock.writeLock().lock();
        try{
            if(transLog!=null && !transLog.isEmpty())
                throw new IllegalAccessError("Journal not empty; commit first, than compact");
            super.compact();
        }finally {
            lock.writeLock().unlock();
        }
    }


    private long[] getLongStackPage(final long physOffset, boolean read){
        long[] buf = longStackPages.get(physOffset);
        if(buf == null){
            buf = new long[LONG_STACK_NUM_OF_RECORDS_PER_PAGE+1];
            if(read)
                for(int i=0;i<buf.length;i++){
                    buf[i] = phys.getLong(physOffset+i*8);
                }
            longStackPages.put(physOffset,buf);
        }
        return buf;
    }

    @Override
    protected long longStackTake(final long listRecid) throws IOException {
        final long dataOffset = getIndexLong(listRecid) & PHYS_OFFSET_MASK;
        if(dataOffset == 0)
            return 0; //there is no such list, so just return 0

        writeLock_checkLocked();

        long[] buf = getLongStackPage(dataOffset,true);

        final int numberOfRecordsInPage = (int) (buf[0]>>>(8*7));


        if(numberOfRecordsInPage<=0)
            throw new InternalError();
        if(numberOfRecordsInPage>LONG_STACK_NUM_OF_RECORDS_PER_PAGE) throw new InternalError();

        final long ret = buf[numberOfRecordsInPage];

        final long previousListPhysid = buf[0] & PHYS_OFFSET_MASK;

        //was it only record at that page?
        if(numberOfRecordsInPage == 1){
            //yes, delete this page
            long value = previousListPhysid !=0 ?
                    previousListPhysid | (((long) LONG_STACK_PAGE_SIZE) << 48) :
                    0L;
            //update index so it points to previous (or none)
            writeIndexValToTransLog(listRecid, value);

            //put space used by this page into free list
            longStackPages.remove(dataOffset); //TODO write zeroes to phys file
            freePhysRecPut(dataOffset | (((long)LONG_STACK_PAGE_SIZE)<<48));
        }else{
            //no, it was not last record at this page, so just decrement the counter
            buf[0] = previousListPhysid | ((1L*numberOfRecordsInPage-1L)<<(8*7));
        }
        return ret;

    }

    @Override
    protected void longStackPut(final long listRecid, final long offset) throws IOException {
        writeLock_checkLocked();

        //index position was cleared, put into free index list
        final long listPhysid2 =getIndexLong(listRecid) & PHYS_OFFSET_MASK;

        if(listPhysid2 == 0){ //empty list?
            //yes empty, create new page and fill it with values
            final long listPhysid = freePhysRecTake(LONG_STACK_PAGE_SIZE) &PHYS_OFFSET_MASK;
            long[] buf = getLongStackPage(listPhysid,false);
            if(listPhysid == 0) throw new InternalError();
            //set number of free records in this page to 1
            buf[0] = 1L<<(8*7);
            //set  record
            buf[1] = offset;
            //and update index file with new page location
            writeIndexValToTransLog(listRecid, (((long) LONG_STACK_PAGE_SIZE) << 48) | listPhysid);
        }else{
            long[] buf = getLongStackPage(listPhysid2,true);
            final int numberOfRecordsInPage = (int) (buf[0]>>>(8*7));
            if(numberOfRecordsInPage == LONG_STACK_NUM_OF_RECORDS_PER_PAGE){ //is current page full?
                //yes it is full, so we need to allocate new page and write our number there
                final long listPhysid = freePhysRecTake(LONG_STACK_PAGE_SIZE) &PHYS_OFFSET_MASK;
                long[] bufNew = getLongStackPage(listPhysid,false);
                if(listPhysid == 0) throw new InternalError();
                //final ByteBuffers dataBuf = dataBufs[((int) (listPhysid / BUF_SIZE))];
                //set location to previous page
                //set number of free records in this page to 1
                bufNew[0] = listPhysid2 | (1L<<(8*7));
                //set free record
                bufNew[1] = offset;
                //and update index file with new page location
                writeIndexValToTransLog(listRecid,(((long) LONG_STACK_PAGE_SIZE) << 48) | listPhysid);
            }else{
                //there is space on page, so just write released recid and increase the counter
                buf[1+numberOfRecordsInPage] = offset;
                buf[0] = (buf[0]&PHYS_OFFSET_MASK) | ((1L*numberOfRecordsInPage+1L)<<(8*7));
            }
        }
    }



    @Override
	protected long freePhysRecTake(final int requiredSize) throws IOException {
        writeLock_checkLocked();

        if(requiredSize<=0) throw new InternalError();

        long freePhysRec = appendOnly? 0L:
                findFreePhysSlot(requiredSize);
        if(freePhysRec!=0){
            return freePhysRec;
        }

        //No free records found, so lets increase the file size.
        //We need to take case of growing ByteBuffers.
        // Also max size of ByteBuffers is 2GB, so we need to use multiple ones

        final long oldFileSize = physSize;
        if(oldFileSize <=0) throw new InternalError("illegal file size:"+oldFileSize);

        //check if new record would be overflowing BUF_SIZE
        if(oldFileSize%Volume.BUF_SIZE+requiredSize<=Volume.BUF_SIZE){
            //no, so just increase file size
            physSize+=requiredSize;
            //so just increase buffer size

            //and return this
            return (((long)requiredSize)<<48) | oldFileSize;
        }else{
            //new size is overlapping 2GB ByteBuffers size
            //so we need to create empty record for 'padding' size to 2GB

            final long  freeSizeToCreate = Volume.BUF_SIZE -  oldFileSize%Volume.BUF_SIZE;
            if(freeSizeToCreate == 0) throw new InternalError();

            final long nextBufferStartOffset = oldFileSize + freeSizeToCreate;
            if(nextBufferStartOffset%Volume.BUF_SIZE!=0) throw new InternalError();

            //increase the disk size
            physSize += freeSizeToCreate + requiredSize;

            //mark 'padding' free record
            freePhysRecPut((freeSizeToCreate<<48)|oldFileSize);

            //and finally return position at beginning of new buffer
            return (((long)requiredSize)<<48) | nextBufferStartOffset;
        }

    }

    @Override
    protected void unlinkPhysRecord(long indexVal, long recid) throws IOException {
        if(indexVal == 0) return;

        ArrayList<Long> linkedInTrans = transLinkedPhysRecods.remove(recid);
        if(linkedInTrans!=null){
            for(Long l:linkedInTrans){
                freePhysRecPut(l);
            }
            return;
        }

        if((indexVal>>>48)<MAX_RECORD_SIZE){  //check size
            //single record
            freePhysRecPut(indexVal);
            return;
        }

        while(indexVal!=0){
            freePhysRecPut(indexVal);
            final long offset = indexVal & PHYS_OFFSET_MASK;
            indexVal = phys.getLong(offset); //read next value
        }
    }

}
