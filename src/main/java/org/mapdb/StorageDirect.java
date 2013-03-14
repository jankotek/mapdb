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

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Storage Engine which saves record directly into file.
 * Is used when transaction journal is disabled.
 *
 * @author Jan Kotek
 */
public class StorageDirect  implements Engine {




    static final long PHYS_OFFSET_MASK = 0x0000FFFFFFFFFFFFL;


    /** File header. First 4 bytes are 'JDBM', last two bytes are store format version */
    static final long HEADER = 5646556656456456L;


    static final int RECID_CURRENT_PHYS_FILE_SIZE = 1;
    static final int RECID_CURRENT_INDEX_FILE_SIZE = 2;


    /** offset in index file which points to FREEINDEX list (free slots in index file) */
    static final int RECID_FREE_INDEX_SLOTS = 3;


    //TODO slots 5 to 18 are currently unused



    static final int RECID_FREE_PHYS_RECORDS_START = 20;

    static final int NUMBER_OF_PHYS_FREE_SLOT =1000 + 1535;
    static final int MAX_RECORD_SIZE = 65535;

    /** must be smaller then 127 */
    static final byte LONG_STACK_NUM_OF_RECORDS_PER_PAGE = 100;

    static final int LONG_STACK_PAGE_SIZE =   8 + LONG_STACK_NUM_OF_RECORDS_PER_PAGE * 8;

    /** offset in index file from which normal physid starts */
    static final int INDEX_OFFSET_START = RECID_FREE_PHYS_RECORDS_START +NUMBER_OF_PHYS_FREE_SLOT;
    public static final String DATA_FILE_EXT = ".p";


    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    protected final boolean appendOnly;
    protected final boolean deleteFilesOnExit;
    protected final boolean failOnWrongHeader;
    protected final boolean readOnly;

    volatile protected Volume phys;
    volatile protected Volume index;

    public StorageDirect(Volume.Factory volFac, boolean appendOnly,
                   boolean deleteFilesOnExit, boolean failOnWrongHeader, boolean readOnly) {

        this.appendOnly = appendOnly;
        this.deleteFilesOnExit = deleteFilesOnExit;
        this.failOnWrongHeader = failOnWrongHeader;
        this.readOnly = readOnly;

        try{
            lock.writeLock().lock();


            phys = volFac.createPhysVolume();
            index = volFac.createIndexVolume();
            phys.ensureAvailable(8);
            index.ensureAvailable(INDEX_OFFSET_START*8);

            final long header = index.isEmpty()? 0 : index.getLong(0);
            if(header!=HEADER){
                if(failOnWrongHeader) throw new IOError(new IOException("Wrong file header"));
                else writeInitValues();
            }

            File indexFile = index.getFile();
            if(!(this instanceof StorageJournaled) && indexFile !=null
                    && new File(indexFile.getPath()+ StorageJournaled.TRANS_LOG_FILE_EXT).exists()){
                throw new IllegalAccessError("Could not open DB in Direct Mode; WriteAhead log file exists, it may contain some data.");
            }

        }finally {
            lock.writeLock().unlock();
        }

    }

    public StorageDirect(Volume.Factory volFac){
        this(volFac, false, false,false, false);
    }




    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        if(value == null||serializer==null) throw new NullPointerException();
        try{
            DataOutput2 out = new DataOutput2();
            serializer.serialize(out,value);
            //TODO log warning if record is too big


            try{
                lock.writeLock().lock();
                //update index file, find free recid
                long recid = longStackTake(RECID_FREE_INDEX_SLOTS);
                if(recid == 0){
                    //could not reuse recid, so create new one
                    final long indexSize = index.getLong(RECID_CURRENT_INDEX_FILE_SIZE * 8);
                    if(indexSize%8!=0) throw new InternalError();
                    recid = indexSize/8;
                    //grow buffer if necessary
                    index.ensureAvailable(indexSize+8);
                    index.putLong(RECID_CURRENT_INDEX_FILE_SIZE * 8, indexSize + 8);
                }

                if(out.pos<MAX_RECORD_SIZE){
                    //is small size and can be stored in single record
                    //get physical record, first 16 bites is record size, remaining 48 bytes is record offset in phys file
                    final long indexValue = out.pos!=0?
                            freePhysRecTake(out.pos):
                            0L;

                    phys.putData(indexValue&PHYS_OFFSET_MASK, out.buf, out.pos);
                    index.putLong(recid * 8, indexValue);
                }else{
                    putLargeLinkedRecord(out, recid);
                }

                return recid - INDEX_OFFSET_START;
            }finally {
                lock.writeLock().unlock();
            }
        }catch(IOException e){
            throw new IOError(e);
        }
    }

    private void putLargeLinkedRecord(DataOutput2 out, long recid) throws IOException {
        //large size, needs to link multiple records together
        //start splitting from end, so we can build up linked list
        final int chunkSize = MAX_RECORD_SIZE-8;
        int lastArrayPos = out.pos;
        int arrayPos = out.pos - out.pos%chunkSize;
        long lastChunkPhysId = 0;
        while(arrayPos>=0){
            final int currentChunkSize = lastArrayPos-arrayPos;
            byte[] b = new byte[currentChunkSize+8]; //TODO reuse byte[]
            //append reference to prev physId
            ByteBuffer.wrap(b).putLong(0, lastChunkPhysId);
            //copy chunk
            System.arraycopy(out.buf, arrayPos, b, 8, currentChunkSize);
            //and write current chunk
            lastChunkPhysId = freePhysRecTake(currentChunkSize+8);
            phys.putData(lastChunkPhysId&PHYS_OFFSET_MASK, b, b.length);
            lastArrayPos = arrayPos;
            arrayPos-=chunkSize;
        }
        index.putLong(recid * 8, lastChunkPhysId);
    }


    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        if(serializer==null) throw new NullPointerException();
        if(recid<=0) throw new IllegalArgumentException("recid");
        recid += INDEX_OFFSET_START;
        try{
            try{
                lock.readLock().lock();
                final long indexValue = index.getLong(recid * 8) ;
                return recordGet2(indexValue, phys, serializer);
            }finally{
                lock.readLock().unlock();
            }


        }catch(IOException e){
            throw new IOError(e);
        }
    }



    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer){
        if(value == null||serializer==null) throw new NullPointerException();
        if(recid<=0) throw new IllegalArgumentException("recid");
        recid+=INDEX_OFFSET_START;
        try{
            DataOutput2 out = new DataOutput2();
            serializer.serialize(out,value);

            try{
                lock.writeLock().lock();

                final long oldIndexVal = index.getLong(recid * 8);
                final long oldSize = oldIndexVal>>>48;

                //check if we need to split new records into multiple one
                if(out.pos<MAX_RECORD_SIZE){
                    //check if size has changed
                    if(oldSize == 0 && out.pos==0){
                        //do nothing
                    }else if(oldSize == out.pos && oldSize!=MAX_RECORD_SIZE){
                        //size is the same, so just write new data
                        phys.putData(oldIndexVal&PHYS_OFFSET_MASK, out.buf, out.pos);
                    }else if(oldSize != 0 && out.pos==0){
                        //new record has zero size, just delete old phys one
                        freePhysRecPut(oldIndexVal);
                        index.putLong(recid * 8, 0L);
                    }else{
                        //size has changed, so write into new location
                        final long newIndexValue = freePhysRecTake(out.pos);
                        phys.putData(newIndexValue&PHYS_OFFSET_MASK, out.buf, out.pos);
                        //update index file with new location
                        index.putLong(recid * 8, newIndexValue);

                        //and set old phys record as free
                        unlinkPhysRecord(oldIndexVal,recid);
                    }
                }else{
                    putLargeLinkedRecord(out, recid);
                    //and set old phys record as free
                    unlinkPhysRecord(oldIndexVal,recid);
                }
            }finally {
                lock.writeLock().unlock();
            }
        }catch(IOException e){
            throw new IOError(e);
        }
    }


   @Override
   public <A> void delete(long recid, Serializer<A> serializer){
        if(serializer==null)throw new NullPointerException();
        if(recid<=0) throw new IllegalArgumentException("recid");
        recid+=INDEX_OFFSET_START;
        try{
            lock.writeLock().lock();
            final long oldIndexVal = index.getLong(recid * 8);
            index.putLong(recid * 8, 0L);
            longStackPut(RECID_FREE_INDEX_SLOTS,recid);
            unlinkPhysRecord(oldIndexVal,recid);
        }catch(IOException e){
            throw new IOError(e);
        }finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void commit() {
        //TODO sync here?
    }

    @Override
    public void rollback() {
        throw new UnsupportedOperationException("Can not rollback, transactions disabled.");
    }



   protected long longStackTake(final long listRecid) throws IOException {
        final long dataOffset = index.getLong(listRecid * 8) &PHYS_OFFSET_MASK;
        if(dataOffset == 0)
            return 0; //there is no such list, so just return 0

        writeLock_checkLocked();


        final int numberOfRecordsInPage = phys.getUnsignedByte(dataOffset);

        if(numberOfRecordsInPage<=0) throw new InternalError();
        if(numberOfRecordsInPage>LONG_STACK_NUM_OF_RECORDS_PER_PAGE) throw new InternalError();

        final long ret = phys.getLong (dataOffset+numberOfRecordsInPage*8);

        //was it only record at that page?
        if(numberOfRecordsInPage == 1){
            //yes, delete this page
            final long previousListPhysid =phys.getLong(dataOffset) &PHYS_OFFSET_MASK;
            if(previousListPhysid !=0){
                //update index so it points to previous page
                index.putLong(listRecid * 8, previousListPhysid | (((long) LONG_STACK_PAGE_SIZE) << 48));
            }else{
                //zero out index
                index.putLong(listRecid * 8, 0L);
            }
            //put space used by this page into free list
            freePhysRecPut(dataOffset | (((long)LONG_STACK_PAGE_SIZE)<<48));
        }else{
            //no, it was not last record at this page, so just decrement the counter
            phys.putUnsignedByte(dataOffset, (byte) (numberOfRecordsInPage - 1));
        }
        return ret;

    }


   protected void longStackPut(final long listRecid, final long offset) throws IOException {
       writeLock_checkLocked();

       //index position was cleared, put into free index list
        final long listPhysid2 = index.getLong(listRecid * 8) &PHYS_OFFSET_MASK;

        if(listPhysid2 == 0){ //empty list?
            //yes empty, create new page and fill it with values
            final long listPhysid = freePhysRecTake(LONG_STACK_PAGE_SIZE) &PHYS_OFFSET_MASK;
            if(listPhysid == 0) throw new InternalError();
            //set previous Free Index List page to zero as this is first page
            phys.putLong(listPhysid, 0L);
            //set number of free records in this page to 1
            phys.putUnsignedByte(listPhysid, (byte) 1);

            //set  record
            phys.putLong(listPhysid + 8, offset);
            //and update index file with new page location
            index.putLong(listRecid * 8, (((long) LONG_STACK_PAGE_SIZE) << 48) | listPhysid);
        }else{
            final int numberOfRecordsInPage = phys.getUnsignedByte(listPhysid2);
            if(numberOfRecordsInPage == LONG_STACK_NUM_OF_RECORDS_PER_PAGE){ //is current page full?
                //yes it is full, so we need to allocate new page and write our number there

                final long listPhysid = freePhysRecTake(LONG_STACK_PAGE_SIZE) &PHYS_OFFSET_MASK;
                if(listPhysid == 0) throw new InternalError();
                //final ByteBuffers dataBuf = dataBufs[((int) (listPhysid / BUF_SIZE))];
                //set location to previous page
                phys.putLong(listPhysid, listPhysid2);
                //set number of free records in this page to 1
                phys.putUnsignedByte(listPhysid, (byte) 1);
                //set free record
                phys.putLong(listPhysid +  8, offset);
                //and update index file with new page location
                index.putLong(listRecid * 8, (((long) LONG_STACK_PAGE_SIZE) << 48) | listPhysid);
            }else{
                //there is space on page, so just write released recid and increase the counter
                phys.putLong(listPhysid2 +  8 + 8 * numberOfRecordsInPage, offset);
                phys.putUnsignedByte(listPhysid2, (byte) (numberOfRecordsInPage + 1));
            }
        }
   }




	protected long freePhysRecTake(final int requiredSize) throws IOException {
        writeLock_checkLocked();

        if(requiredSize<=0) throw new InternalError();

        long freePhysRec = (appendOnly
                //TODO !HACK! to 'fix' issue 69
                || Thread.currentThread().getStackTrace().length>256)
                ? 0L:
                findFreePhysSlot(requiredSize);
        if(freePhysRec!=0){
            return freePhysRec;
        }



        //No free records found, so lets increase the file size.
        //We need to take case of growing ByteBuffers.
        // Also max size of ByteBuffers is 2GB, so we need to use multiple ones

        final long physFileSize = index.getLong(RECID_CURRENT_PHYS_FILE_SIZE*8);
        if(physFileSize <=0) throw new InternalError("illegal file size:"+physFileSize);

        //check if new record would be overflowing BUF_SIZE
        if(physFileSize%Volume.BUF_SIZE+requiredSize<=Volume.BUF_SIZE){
            //no, so just increase file size
            phys.ensureAvailable(physFileSize+requiredSize);
            //so just increase buffer size
            index.putLong(RECID_CURRENT_PHYS_FILE_SIZE * 8, physFileSize + requiredSize);

            //and return this
            return (((long)requiredSize)<<48) | physFileSize;
        }else{
            //new size is overlapping 2GB ByteBuffers size
            //so we need to create empty record for 'padding' size to 2GB

            final long  freeSizeToCreate = Volume.BUF_SIZE -  physFileSize%Volume.BUF_SIZE;
            if(freeSizeToCreate == 0) throw new InternalError();

            final long nextBufferStartOffset = physFileSize + freeSizeToCreate;
            if(nextBufferStartOffset%Volume.BUF_SIZE!=0) throw new InternalError();

            //increase the disk size
            phys.ensureAvailable(physFileSize + freeSizeToCreate + requiredSize);
            index.putLong(RECID_CURRENT_PHYS_FILE_SIZE * 8, physFileSize + freeSizeToCreate + requiredSize);

            //mark 'padding' free record
            freePhysRecPut((freeSizeToCreate<<48)|physFileSize);

            //and finally return position at beginning of new buffer
            return (((long)requiredSize)<<48) | nextBufferStartOffset;
        }

    }



    private void writeInitValues() {
        writeLock_checkLocked();

        //zero out all index values
        for(int i=1;i<=INDEX_OFFSET_START+Engine.LAST_RESERVED_RECID;i++){
            index.putLong(i*8, 0L);
        }

        //write headers
        phys.putLong(0, HEADER);
        index.putLong(0L,HEADER);
        if(index.getLong(0L)!=HEADER)
            throw new InternalError();


        //and set current sizes
        index.putLong(RECID_CURRENT_PHYS_FILE_SIZE * 8, 8L);
        index.putLong(RECID_CURRENT_INDEX_FILE_SIZE * 8, INDEX_OFFSET_START * 8 + Engine.LAST_RESERVED_RECID*8 + 8);
    }


    protected void writeLock_checkLocked() {
        if(!lock.writeLock().isHeldByCurrentThread())
            throw new IllegalAccessError("no write lock");
    }



    final int freePhysRecSize2FreeSlot(final int size){
        if(size>MAX_RECORD_SIZE) throw new IllegalArgumentException("too big record");
        if(size<0) throw new IllegalArgumentException("negative size");

        if(size<1535)
            return size-1;
        else if(size == MAX_RECORD_SIZE)
            return NUMBER_OF_PHYS_FREE_SLOT-1;
        else
            return 1535 -1 + (size-1535)/64;
    }

    @Override
    public void close() {
        try{
            lock.writeLock().lock();

            phys.close();
            index.close();
            if(deleteFilesOnExit){
                phys.deleteFile();
                index.deleteFile();
            }
            phys = null;
            index = null;

        }finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean isClosed(){
        return index == null;
    }

    protected  <A> A recordGet2(long indexValue, Volume data, Serializer<A> serializer) throws IOException {
        final long dataPos = indexValue & PHYS_OFFSET_MASK;
        final int dataSize = (int) (indexValue>>>48);
        if(dataPos == 0) return serializer.deserialize(new DataInput2(new byte[0]),0);

        if(dataSize<MAX_RECORD_SIZE){
            //single record
            DataInput2 in = data.getDataInput(dataPos, dataSize);
            final A value = serializer.deserialize(in,dataSize);

            if( in.pos != dataSize + (data.isSliced()?dataPos%Volume.BUF_SIZE:0))
                throw new InternalError("Data were not fully read.");
            return value;
        }else{
            //large linked record
            ArrayList<DataInput2> ins = new ArrayList<DataInput2>();
            ArrayList<Integer> sizes = new ArrayList<Integer>();
            int recSize = 0;
            long nextLink = indexValue;
            while(nextLink!=0){
                int currentSize = (int) (nextLink>>>48);
                recSize+= currentSize-8;
                DataInput2 in = data.getDataInput(nextLink & PHYS_OFFSET_MASK, currentSize);
                nextLink = in.readLong();
                ins.add(in);
                sizes.add(currentSize - 8);
            }
            //construct byte[]
            byte[] b = new byte[recSize];
            int pos = 0;
            for(int i=0;i<ins.size();i++){
                DataInput2 in = ins.set(i,null);
                int size = sizes.get(i);
                in.readFully(b, pos, size);
                pos+=size;
            }
            DataInput2 in = new DataInput2(b);
            final A value = serializer.deserialize(in,recSize);

            if( in.pos != recSize)
                throw new InternalError("Data were not fully read.");
            return value;
        }
    }



    protected void freePhysRecPut(final long indexValue) throws IOException {
        if((indexValue &PHYS_OFFSET_MASK)==0) throw new InternalError("zero indexValue: ");
        final int size =  (int) (indexValue>>>48);

        final long listRecid = RECID_FREE_PHYS_RECORDS_START + freePhysRecSize2FreeSlot(size);
        longStackPut(listRecid, indexValue);
    }

    protected long findFreePhysSlot(int requiredSize) throws IOException {
        int slot = freePhysRecSize2FreeSlot(requiredSize);
        //check if this slot can contain smaller records,
        if(requiredSize>1 && slot==freePhysRecSize2FreeSlot(requiredSize-1))
            slot ++; //yes, in this case we have to start at next slot with bigger record and divide it

        while(slot< NUMBER_OF_PHYS_FREE_SLOT){

            final long v = longStackTake(RECID_FREE_PHYS_RECORDS_START +slot);
            if(v!=0){
                //we found it, check if we need to split record
                final int foundRecSize = (int) (v>>>48);
                if(foundRecSize!=requiredSize){

                    //yes we need split
                    final long newIndexValue =
                            ((long)(foundRecSize - requiredSize)<<48) | //encode size into new free record
                                    (v & PHYS_OFFSET_MASK) +   requiredSize; //and encode new free record phys offset
                    freePhysRecPut(newIndexValue);
                }

                //return offset combined with required size
                return (v & PHYS_OFFSET_MASK) |
                        (((long)requiredSize)<<48);
            }else{
                slot++;
            }
        }
        return 0;

    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer){
        if(expectedOldValue == null||newValue==null||serializer==null) throw new NullPointerException();
        if(recid<=0) throw new IllegalArgumentException("recid");
        try{
            lock.writeLock().lock();
            Object oldVal = get(recid, serializer);
            if((oldVal==null && expectedOldValue==null)|| (oldVal!=null && oldVal.equals(expectedOldValue))){
                update(recid, newValue, serializer);
                return true;
            }else{
                return false;
            }
        }finally{
            lock.writeLock().unlock();
        }

    }



    @Override
    public boolean isReadOnly() {
        return readOnly;
    }


    protected void unlinkPhysRecord(long indexVal, long recid) throws IOException {
        int size = (int) (indexVal >>>48);
        if(size==0) return;
        if(size<MAX_RECORD_SIZE){
            freePhysRecPut(indexVal);
        }else{
            while(indexVal!=0){
                //traverse linked record
                long nextIndexVal = phys.getLong(indexVal&PHYS_OFFSET_MASK);
                freePhysRecPut(indexVal);
                indexVal = nextIndexVal;
            }
        }

    }

    @Override
    public void compact(){
        if(readOnly) throw new IllegalAccessError();
        if(index.getFile()==null) throw new UnsupportedOperationException("compact not supported for memory storage yet");
        lock.writeLock().lock();
        try{
            //create secondary files for compaction
            //TODO RAF
            //TODO memory based stores
            final File indexFile = index.getFile();
            final File physFile = phys.getFile();
            final boolean isRaf = index instanceof Volume.RandomAccessFileVol;
            Volume.Factory fab = Volume.fileFactory(false, isRaf, new File(indexFile+".compact"));
            StorageDirect store2 = new StorageDirect(fab);

            //transfer stack of free recids
            for(long recid =longStackTake(RECID_FREE_INDEX_SLOTS);
                recid!=0; recid=longStackTake(RECID_FREE_INDEX_SLOTS)){
                store2.longStackPut(recid, RECID_FREE_INDEX_SLOTS);
            }

            //iterate over recids and transfer physical records
            final long indexSize = index.getLong(RECID_CURRENT_INDEX_FILE_SIZE*8)/8;


            store2.lock.writeLock().lock();
            for(long recid = INDEX_OFFSET_START; recid<indexSize;recid++){
                //read data from first store
                long physOffset = index.getLong(recid*8);
                long physSize = physOffset >>> 48;
                //TODO linked records larger then 64KB
                physOffset = physOffset & PHYS_OFFSET_MASK;

                //write index value into second storage
                store2.index.ensureAvailable(recid*8+8);

                //get free place in second store, and write data there
                if(physSize!=0){
                    DataInput2 in = phys.getDataInput(physOffset, (int)physSize);
                    long physOffset2 =
                            store2.freePhysRecTake((int)physSize) & PHYS_OFFSET_MASK;

                    store2.phys.ensureAvailable((physOffset2 & PHYS_OFFSET_MASK)+physSize);
                    synchronized (in.buf){
                        //copy directly from buffer
                        in.buf.limit((int) (in.pos+physSize));
                        in.buf.position(in.pos);
                        store2.phys.putData(physOffset2, in.buf);
                    }
                    store2.index.putLong(recid*8, (physSize<<48)|physOffset2);
                }else{
                    //just write zeroes
                    store2.index.putLong(recid*8, 0);
                }
            }

            store2.index.putLong(RECID_CURRENT_INDEX_FILE_SIZE*8, indexSize*8);

            File indexFile2 = store2.index.getFile();
            File physFile2 = store2.phys.getFile();
            store2.lock.writeLock().unlock();
            store2.close();

            long time = System.currentTimeMillis();
            File indexFile_ = new File(indexFile.getPath()+"_"+time+"_orig");
            File physFile_ = new File(physFile.getPath()+"_"+time+"_orig");

            index.close();
            phys.close();
            if(!indexFile.renameTo(indexFile_))throw new InternalError();
            if(!physFile.renameTo(physFile_))throw new InternalError();

            if(!indexFile2.renameTo(indexFile))throw new InternalError();
            //TODO process may fail in middle of rename, analyze sequence and add recovery
            if(!physFile2.renameTo(physFile))throw new InternalError();

            indexFile_.delete();
            physFile_.delete();

            Volume.Factory fac2 = Volume.fileFactory(false, isRaf, indexFile);
            index = fac2.createIndexVolume();
            phys = fac2.createPhysVolume();

        }catch(IOException e){
            throw new IOError(e);
        }finally {
            lock.writeLock().unlock();
        }
    }




}