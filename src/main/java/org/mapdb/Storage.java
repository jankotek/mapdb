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
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Some common method for Storage
 *
 * @author Jan Kotek
 */
public abstract class Storage implements Engine {



    static final long PHYS_OFFSET_MASK = 0x0000FFFFFFFFFFFFL;


    /** File header. First 4 bytes are 'JDBM', last two bytes are store format version */
    static final long HEADER = 5646556656456456L;


    static final int RECID_CURRENT_PHYS_FILE_SIZE = 1;
    static final int RECID_CURRENT_INDEX_FILE_SIZE = 2;


    /** offset in index file which points to FREEINDEX list (free slots in index file) */
    static final int RECID_FREE_INDEX_SLOTS = 3;

    static final int RECID_SERIALIZER = 4;

    //TODO slots 5 to 18 are currently unused

    static final int RECID_NAME_DIR = 19;


    static final int RECID_FREE_PHYS_RECORDS_START = 20;

    static final int NUMBER_OF_PHYS_FREE_SLOT =1000 + 1535;
    static final int MAX_RECORD_SIZE = 65535;

    /** must be smaller then 127 */
    static final byte LONG_STACK_NUM_OF_RECORDS_PER_PAGE = 100;

    static final int LONG_STACK_PAGE_SIZE =   8 + LONG_STACK_NUM_OF_RECORDS_PER_PAGE * 8;

    /** offset in index file from which normal physid starts */
    static final int INDEX_OFFSET_START = RECID_FREE_PHYS_RECORDS_START +NUMBER_OF_PHYS_FREE_SLOT;
    public static final String DATA_FILE_EXT = ".p";


    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();;

    protected final boolean appendOnly;
    protected final boolean deleteFilesOnExit;
    protected final boolean failOnWrongHeader;
    protected final boolean readOnly;

    protected Volume phys;
    protected Volume index;

    public Storage(Volume.Factory volFac, boolean appendOnly,
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

        }finally {
            lock.writeLock().unlock();
        }

    }


    private void writeInitValues() {
        writeLock_checkLocked();

        //zero out all index values
        for(int i=1;i<INDEX_OFFSET_START;i++){
            index.putLong(i*8, 0L);
        }

        //write headers
        phys.putLong(0, HEADER);
        index.putLong(0L,HEADER);
        if(CC.ASSERT && index.getLong(0L)!=HEADER)
            throw new InternalError();


        //and set current sizes
        index.putLong(RECID_CURRENT_PHYS_FILE_SIZE * 8, 8L);
        index.putLong(RECID_CURRENT_INDEX_FILE_SIZE * 8, INDEX_OFFSET_START * 8);
        index.putLong(RECID_NAME_DIR *8,0);
    }


    protected void writeLock_checkLocked() {
        if(CC.ASSERT && !lock.writeLock().isHeldByCurrentThread())
            throw new IllegalAccessError("no write lock");
    }



    final int freePhysRecSize2FreeSlot(final int size){
        if(CC.ASSERT && size>MAX_RECORD_SIZE) throw new IllegalArgumentException("too big record");
        if(CC.ASSERT && size<0) throw new IllegalArgumentException("negative size");

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
        if(dataPos == 0) return null;

        if(dataSize<MAX_RECORD_SIZE){
            //single record
            DataInput2 in = data.getDataInput(dataPos, dataSize);
            final A value = serializer.deserialize(in,dataSize);

            if(CC.ASSERT &&  in.pos != dataSize + (data.isSliced()?dataPos%Volume.BUF_SIZE:0))
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

            if(CC.ASSERT &&  in.pos != recSize)
                throw new InternalError("Data were not fully read.");
            return value;
        }
    }


    abstract protected long longStackTake(final long listRecid) throws IOException;

    abstract protected void longStackPut(final long listRecid, final long offset) throws IOException;

    abstract protected long freePhysRecTake(final int requiredSize) throws IOException;

    protected void freePhysRecPut(final long indexValue) throws IOException {
        if(CC.ASSERT && (indexValue &PHYS_OFFSET_MASK)==0) throw new InternalError("zero indexValue: ");
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


    @Override public long serializerRecid() {
        return RECID_SERIALIZER;
    }

    @Override public long nameDirRecid() {
        return RECID_NAME_DIR;
    }


    @Override
    public boolean isReadOnly() {
        return readOnly;
    }


    protected void unlinkPhysRecord(long indexVal) throws IOException {
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
}
