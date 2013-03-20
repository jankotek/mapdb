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
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Storage Engine which saves record directly into file.
 * Is used when transaction journal is disabled.
 *
 * @author Jan Kotek
 */
public class StorageDirect2 implements Engine{

    protected static final long MASK_OFFSET = 0x0000FFFFFFFFFFFFL;

    protected static final long MASK_SIZE = 0x7fff000000000000L;
    protected static final long MASK_IS_LINKED = 0x8000000000000000L;

    protected static final long HEADER = 9032094932889042394L;

    /** maximal non linked record size */
    protected static final int MAX_REC_SIZE = 32767;

    /** number of free physical slots */
    protected static final int PHYS_FREE_SLOTS_COUNT = 2048;

    /** index file offset where current size of index file is stored*/
    protected static final int IO_INDEX_SIZE = 1*8;
    /** index file offset where current size of phys file is stored */
    protected static final int IO_PHYS_SIZE = 2*8;
    /** index file offset where reference to longstack of free recid is stored*/
    protected static final int IO_FREE_RECID = 15*8;

    /** index file offset where first recid available to user is stored */
    protected static final int IO_USER_START = IO_FREE_RECID+PHYS_FREE_SLOTS_COUNT*8;



    protected static final int CONCURRENCY_FACTOR = 32;

    protected final ReentrantReadWriteLock[] locks;
    protected final ReentrantLock freeSpaceLock;

    protected Volume index;
    protected Volume phys;

    protected final boolean readOnly;

    public StorageDirect2(Volume.Factory volFac, boolean readOnly) {
        this.readOnly = readOnly;

        locks = new ReentrantReadWriteLock[CONCURRENCY_FACTOR];
        for(int i=0;i<locks.length;i++) locks[i] = new ReentrantReadWriteLock();
        freeSpaceLock = new ReentrantLock();

        index = volFac.createIndexVolume();
        phys = volFac.createPhysVolume();
        if(index.isEmpty()){
            createStructure();
        }else{
            checkHeaders();
        }

    }

    protected void checkHeaders() {
        if(index.getLong(0)!=HEADER||phys.getLong(0)!=HEADER)throw new IOError(new IOException("storage has invalid header"));
    }

    protected void createStructure() {
        phys.ensureAvailable(8);
        phys.putLong(0, HEADER);
        final long indexSize = IO_USER_START+Engine.LAST_RESERVED_RECID*8+8;
        index.ensureAvailable(indexSize);
        for(int i=0;i<indexSize;i+=8) index.putLong(i,0L);
        index.putLong(0, HEADER);
        index.putLong(IO_INDEX_SIZE,indexSize);
        index.putLong(IO_PHYS_SIZE,8);
    }


    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        DataOutput2 out = serialize(value, serializer);

        freeSpaceLock.lock();
        long recid = 0;
        try{
           recid = freeRecidTake() ;
        }finally {
            freeSpaceLock.unlock();
        }
        final long ioRecid = IO_USER_START + recid*8;
        final Lock lock = locks[Utils.longHash(recid)%CONCURRENCY_FACTOR].writeLock();
        lock.lock();
        try{

            final long[] indexVals;
            freeSpaceLock.lock();
            try{
                indexVals = physAllocate(out.pos);
            }finally {
                freeSpaceLock.unlock();
            }

            index.putLong(ioRecid, indexVals[0]);
            //write stuff
            if(indexVals.length==1||indexVals[1]==0){ //is more then one? ie linked
                //write single

                phys.putData(indexVals[0]&MASK_OFFSET, out.buf, out.pos);

            }else{
                ByteBuffer buf = ByteBuffer.wrap(out.buf, 0, out.pos);
                //write linked
                for(int i=0;i<indexVals.length;i++){
                    final int c = i==0? 12 : (i==indexVals.length-1? 0 : 8);
                    final long indexVal = indexVals[i];
                    final boolean isLast = (indexVal & MASK_IS_LINKED) ==0;
                    if(isLast!=(i==indexVals.length-1)) throw new InternalError();
                    final int size = (int) ((indexVal& MASK_SIZE)>>48);
                    final long offset = indexVal&MASK_OFFSET;

                    //write data
                    buf.limit(buf.position()+size-c);
                    phys.putData(offset+c,buf);

                    if(c>0){
                        //write position of next linked record
                        phys.putLong(offset, indexVals[i+1]);
                    }
                    if(c==12){
                        //write total size in first record
                        phys.putInt(offset+8, out.pos);
                    }
                }
            }

            return recid;
        }finally{
            lock.unlock();
        }
    }



    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        final long ioRecid = IO_USER_START + recid*8;
        final Lock lock = locks[Utils.longHash(recid)%CONCURRENCY_FACTOR].readLock();
        lock.lock();
        try{
            long indexVal = index.getLong(ioRecid);

            int size = (int) ((indexVal&MASK_SIZE)>>>48);
            long offset = indexVal&MASK_OFFSET;
            if((indexVal&MASK_IS_LINKED)==0){
                //read single record
                return serializer.deserialize(phys.getDataInput(offset, size),size);
            }else{
                //is linked, first construct buffer we will read data to
                int totalSize = phys.getInt(offset+8);
                int pos = 0;
                int c = 12;
                byte[] buf = new byte[totalSize];
                //read parts into segment
                for(;;){
                    DataInput2 in = phys.getDataInput(offset + c, size-c);
                    in.readFully(buf,pos,size-c);
                    pos+=size-c;
                    if(c==0) break;
                    //read next part
                    long next = phys.getLong(offset);
                    offset = next&MASK_OFFSET;
                    size = (int) ((next&MASK_SIZE)>>>48);
                    //is the next part last?
                    c =  ((next&MASK_IS_LINKED)==0)? 0 : 8;
                }
                if(pos!=totalSize) throw new InternalError();
                return serializer.deserialize(new DataInput2(buf),totalSize);
            }
        }catch(IOException e){
            throw new IOError(e);
        }finally{
            lock.unlock();
        }
    }


    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        final Lock lock = locks[Utils.longHash(recid)%CONCURRENCY_FACTOR].writeLock();
        lock.lock();
        try{


        }finally{
            lock.unlock();
        }
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        final Lock lock = locks[Utils.longHash(recid)%CONCURRENCY_FACTOR].writeLock();
        lock.lock();
        try{
            return false;
        }finally{
            lock.unlock();
        }
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        final long ioRecid = IO_USER_START + recid*8;
        final Lock lock = locks[Utils.longHash(recid)%CONCURRENCY_FACTOR].writeLock();
        lock.lock();
        try{
            //get index val and zero it out
            final long indexVal = index.getLong(ioRecid);
            index.putLong(ioRecid,0L);

            long[] linkedRecords = null;
            int linkedPos = 0;
            if((indexVal&MASK_IS_LINKED)!=0){
                //record is composed of multiple linked records, so collect all of them
                linkedRecords = new long[2];

                //traverse linked records
                long linkedVal = phys.getLong(indexVal&MASK_OFFSET);
                for(;;){
                    if(linkedPos==linkedRecords.length) //grow if necessary
                        linkedRecords = Arrays.copyOf(linkedRecords, linkedRecords.length*2);
                    //store last linkedVal
                    linkedRecords[linkedPos] = linkedVal;

                    if((linkedVal&MASK_IS_LINKED)==0){
                        break; //this is last linked record, so break
                    }
                    //move and read to next
                    linkedPos++;
                    linkedVal = phys.getLong(linkedVal&MASK_OFFSET);
                }
            }

            //now lock everything and mark free space
            freeSpaceLock.lock();
            try{
                //free recid
                freeRecidPut(recid);
                //free first record pointed from indexVal
                freePhysPut(indexVal);

                //if there are more linked records, free those as well
                if(linkedRecords!=null){
                    for(int i=0;i<linkedPos;i++){
                        freePhysPut(linkedRecords[i]);
                    }
                }
            }finally {
                freeSpaceLock.unlock();
            }

        }finally{
            lock.unlock();
        }
    }

    protected long[] physAllocate(int size) {
        //append to end of file
        if(size<MAX_REC_SIZE){
            long physSize = index.getLong(IO_PHYS_SIZE);
            index.putLong(IO_PHYS_SIZE, physSize+size);
            phys.ensureAvailable(physSize+size);
            physSize = physSize ^ (((long)size)<<48);
            return new long[]{physSize};
        }else{
            long[] ret = new long[2];
            int retPos = 0;
            int c = 12;

            while(size>0){
                if(retPos == ret.length) ret = Arrays.copyOf(ret, ret.length*2);
                int allocSize = Math.min(size, MAX_REC_SIZE);
                size -= allocSize - c;

                //append to end of file
                long physSize = index.getLong(IO_PHYS_SIZE);
                index.putLong(IO_PHYS_SIZE, physSize+allocSize);
                phys.ensureAvailable(physSize+allocSize);
                physSize = physSize ^ (((long)allocSize)<<48);
                if(c!=0) physSize|=MASK_IS_LINKED;
                ret[retPos++] = physSize;

                c = size<=MAX_REC_SIZE ? 0 : 8;
            }
            if(size!=0) throw new InternalError();

            return Arrays.copyOf(ret, retPos);
        }
    }





    @Override
    public void close() {
        //TODO close
    }

    @Override
    public boolean isClosed() {
        return index==null;
    }

    @Override
    public void commit() {
        //TODO sync on commit setting
    }

    @Override
    public void rollback() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("rollback not supported with journal disabled");
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public void compact() {
        //TODO compact
    }


    protected long longStackTake(long indexOffset){
        return 0;
    }

    protected long longStackPut(long indexOffset, long value){
        return 0;
    }

    protected void freeRecidPut(long recid) {
        longStackPut(IO_FREE_RECID, recid);
    }

    protected long freeRecidTake(){
        long indexSize = index.getLong(IO_INDEX_SIZE);
        index.putLong(IO_INDEX_SIZE,indexSize+8);
        index.ensureAvailable(indexSize+8);
        return (indexSize-IO_USER_START)/8;
    }

    protected void freePhysPut(long indexVal) {
        long size = (indexVal&MASK_SIZE) >>>48;
        longStackPut(IO_FREE_RECID + ((size+1)/16)*16, indexVal & MASK_OFFSET);
    }

    protected long freePhysTake(int size) {
        return longStackTake(IO_FREE_RECID + ((size+1)/16)*16);
    }


    protected <A> DataOutput2 serialize(A value, Serializer<A> serializer) {
        try {
            DataOutput2 out = new DataOutput2();
            serializer.serialize(out,value);
            return out;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

}
