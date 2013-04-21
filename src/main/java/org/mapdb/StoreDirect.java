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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Storage Engine which saves record directly into file.
 * Is used when transaction journal is disabled.
 *
 * @author Jan Kotek
 */
public class StoreDirect implements Store{

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
    protected static final int IO_USER_START = IO_FREE_RECID+PHYS_FREE_SLOTS_COUNT*8+8;

    public static final String DATA_FILE_EXT = ".p";


    static final int LONG_STACK_PER_PAGE = 204;

    static final int LONG_STACK_PAGE_SIZE =   8 + LONG_STACK_PER_PAGE * 6;


    protected final ReentrantReadWriteLock[] locks = Utils.newReadWriteLocks(32);
    protected final ReentrantLock structuralLock = new ReentrantLock();

    protected Volume index;
    protected Volume phys;

    protected long physSize;
    protected long indexSize;

    protected final boolean deleteFilesAfterClose;

    protected final boolean readOnly;

    protected final boolean spaceReclaimReuse;
    protected final boolean spaceReclaimTrack;

    public StoreDirect(Volume.Factory volFac, boolean readOnly, boolean deleteFilesAfterClose,
                       int spaceReclaimMode) {
        this.readOnly = readOnly;
        this.deleteFilesAfterClose = deleteFilesAfterClose;

        this.spaceReclaimReuse = spaceReclaimMode>2;
        this.spaceReclaimTrack = spaceReclaimMode>0;

        index = volFac.createIndexVolume();
        phys = volFac.createPhysVolume();
        if(index.isEmpty()){
            createStructure();
        }else{
            checkHeaders();
            indexSize = index.getLong(IO_INDEX_SIZE);
            physSize = index.getLong(IO_PHYS_SIZE);
        }

    }

    public StoreDirect(Volume.Factory volFac) {
        this(volFac, false,false,5);
    }

    protected void checkHeaders() {
        if(index.getLong(0)!=HEADER||phys.getLong(0)!=HEADER)throw new IOError(new IOException("storage has invalid header"));
    }

    protected void createStructure() {
        indexSize = IO_USER_START+LAST_RESERVED_RECID*8+8;
        index.ensureAvailable(indexSize);
        for(int i=0;i<indexSize;i+=8) index.putLong(i,0L);
        index.putLong(0, HEADER);
        index.putLong(IO_INDEX_SIZE,indexSize);
        physSize =16;
        phys.ensureAvailable(physSize);
        phys.putLong(0, HEADER);
        index.putLong(IO_PHYS_SIZE,physSize);
    }


    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        DataOutput2 out = serialize(value, serializer);

        structuralLock.lock();
        final long ioRecid;
        final long[] indexVals;
        try{
            ioRecid = freeIoRecidTake(true) ;
            indexVals = physAllocate(out.pos,true);
        }finally {
            structuralLock.unlock();
        }

        put2(out, ioRecid, indexVals);

        return (ioRecid-IO_USER_START)/8;
    }

    private void put2(DataOutput2 out, long ioRecid, long[] indexVals) {
        index.putLong(ioRecid, indexVals[0]);
        //write stuff
        if(indexVals.length==1||indexVals[1]==0){ //is more then one? ie linked
            //write single

            phys.putData(indexVals[0]&MASK_OFFSET, out.buf, 0, out.pos);

        }else{
            int outPos = 0;
            //write linked
            for(int i=0;i<indexVals.length;i++){
                final int c = ccc(indexVals.length, i);
                final long indexVal = indexVals[i];
                final boolean isLast = (indexVal & MASK_IS_LINKED) ==0;
                if(isLast!=(i==indexVals.length-1)) throw new InternalError();
                final int size = (int) ((indexVal& MASK_SIZE)>>48);
                final long offset = indexVal&MASK_OFFSET;

                //write data
                phys.putData(offset+c,out.buf,outPos, size-c);
                outPos+=size-c;

                if(c>0){
                    //write position of next linked record
                    phys.putLong(offset, indexVals[i+1]);
                }
                if(c==12){
                    //write total size in first record
                    phys.putInt(offset+8, out.pos);
                }
            }
            if(outPos!=out.pos) throw new InternalError();
        }
    }


    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        final long ioRecid = IO_USER_START + recid*8;
        Utils.readLock(locks, recid);
        try{
            return get2(ioRecid,serializer);
        }catch(IOException e){
            throw new IOError(e);
        }finally{
            Utils.readUnlock(locks, recid);
        }
    }

    protected <A> A get2(long ioRecid,Serializer<A> serializer) throws IOException {
        long indexVal = index.getLong(ioRecid);

        int size = (int) ((indexVal&MASK_SIZE)>>>48);
        DataInput2 di;
        long offset = indexVal&MASK_OFFSET;
        if((indexVal&MASK_IS_LINKED)==0){
            //read single record
            di = phys.getDataInput(offset, size);

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
            di = new DataInput2(buf);
            size = totalSize;
        }
        int start = di.pos;
        A ret = serializer.deserialize(di,size);
        if(size+start>di.pos)throw new InternalError("data were not fully read, check your serializier");
        if(size+start<di.pos)throw new InternalError("data were read beyond record size, check your serializier");
        return ret;
    }


    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        DataOutput2 out = serialize(value, serializer);

        final long ioRecid = IO_USER_START + recid*8;


        Utils.writeLock(locks, recid);
        try{
            long indexVal = index.getLong(ioRecid);
            long[] indexVals = spaceReclaimTrack ? getLinkedRecordsIndexVals(indexVal) : null;
            structuralLock.lock();
            try{

                if(spaceReclaimTrack){
                    //free first record pointed from indexVal
                    freePhysPut(indexVal);

                    //if there are more linked records, free those as well
                    if(indexVals!=null){
                        for(int i=0;i<indexVals.length && indexVals[i]!=0;i++){
                            freePhysPut(indexVals[i]);
                        }
                    }
                }

                indexVals = physAllocate(out.pos,true);
            }finally {
                structuralLock.unlock();
            }

            put2(out, ioRecid, indexVals);
        }finally{
            Utils.writeUnlock(locks, recid);
        }
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        final long ioRecid = IO_USER_START + recid*8;
        Utils.writeLock(locks, recid);
        try{
            /*
             * deserialize old value
             */

            A oldVal = get2(ioRecid,serializer);

            /*
             * compare oldValue and expected
             */
            if((oldVal == null && expectedOldValue!=null) || (oldVal!=null && !oldVal.equals(expectedOldValue)))
                return false;

            /*
             * write new value
             */
            DataOutput2 out = serialize(newValue, serializer);

            long indexVal = index.getLong(ioRecid);
            long[] indexVals = spaceReclaimTrack ? getLinkedRecordsIndexVals(indexVal) : null;

            structuralLock.lock();
            try{
                if(spaceReclaimTrack){
                    //free first record pointed from indexVal
                    freePhysPut(indexVal);

                    //if there are more linked records, free those as well
                    if(indexVals!=null){
                        for(int i=0;i<indexVals.length && indexVals[i]!=0;i++){
                            freePhysPut(indexVals[i]);
                        }
                    }
                }

                indexVals = physAllocate(out.pos,true);
            }finally {
                structuralLock.unlock();
            }

            put2(out, ioRecid, indexVals);
            return true;
        }catch(IOException e){
            throw new IOError(e);
        }finally{
            Utils.writeUnlock(locks, recid);
        }
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        final long ioRecid = IO_USER_START + recid*8;
        Utils.writeLock(locks, recid);
        try{
            //get index val and zero it out
            final long indexVal = index.getLong(ioRecid);
            index.putLong(ioRecid,0L);

            if(!spaceReclaimTrack) return; //free space is not tracked, so do not mark stuff as free

            long[] linkedRecords = getLinkedRecordsIndexVals(indexVal);

            //now lock everything and mark free space
            structuralLock.lock();
            try{
                //free recid
                freeIoRecidPut(ioRecid);
                //free first record pointed from indexVal
                freePhysPut(indexVal);

                //if there are more linked records, free those as well
                if(linkedRecords!=null){
                    for(int i=0; i<linkedRecords.length &&linkedRecords[i]!=0;i++){
                        freePhysPut(linkedRecords[i]);
                    }
                }
            }finally {
                structuralLock.unlock();
            }

        }finally{
            Utils.writeUnlock(locks, recid);
        }
    }

    protected long[] getLinkedRecordsIndexVals(long indexVal) {
        long[] linkedRecords = null;

        int linkedPos = 0;
        if((indexVal&MASK_IS_LINKED)!=0){
            //record is composed of multiple linked records, so collect all of them
            linkedRecords = new long[2];

            //traverse linked records
            long linkedVal = phys.getLong(indexVal&MASK_OFFSET);
            for(;;){
                if(linkedPos==linkedRecords.length) //grow if necessary
                    linkedRecords = Arrays.copyOf(linkedRecords, linkedRecords.length * 2);
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
        return linkedRecords;
    }

    protected long[] physAllocate(int size, boolean ensureAvail) {
        if(size==0L) return new long[]{0L};
        //append to end of file
        if(size<MAX_REC_SIZE){
            long indexVal = freePhysTake(size,ensureAvail);
            indexVal |= ((long)size)<<48;
            return new long[]{indexVal};
        }else{
            long[] ret = new long[2];
            int retPos = 0;
            int c = 12;

            while(size>0){
                if(retPos == ret.length) ret = Arrays.copyOf(ret, ret.length*2);
                int allocSize = Math.min(size, MAX_REC_SIZE);
                size -= allocSize - c;

                //append to end of file
                long indexVal = freePhysTake(allocSize, ensureAvail);
                indexVal |= (((long)allocSize)<<48);
                if(c!=0) indexVal|=MASK_IS_LINKED;
                ret[retPos++] = indexVal;

                c = size<=MAX_REC_SIZE ? 0 : 8;
            }
            if(size!=0) throw new InternalError();

            return Arrays.copyOf(ret, retPos);
        }
    }



    protected static long roundTo16(long offset){
        long rem = offset%16;
        if(rem!=0) offset +=16-rem;
        return offset;
    }


    protected static int ccc(int size, int i) {
        return (size==1|| i==size-1)? 0: (i==0?12:8);
    }



    @Override
    public void close() {
        structuralLock.lock();
        Utils.writeLockAll(locks);
        if(!readOnly){
            index.putLong(IO_PHYS_SIZE,physSize);
            index.putLong(IO_INDEX_SIZE,indexSize);
        }

        index.sync();
        phys.sync();
        index.close();
        phys.close();
        if(deleteFilesAfterClose){
            index.deleteFile();
            phys.deleteFile();
        }
        index = null;
        phys = null;
        Utils.writeUnlockAll(locks);
        structuralLock.unlock();
    }

    @Override
    public boolean isClosed() {
        return index==null;
    }

    @Override
    public void commit() {
        if(!readOnly){
            index.putLong(IO_PHYS_SIZE,physSize);
            index.putLong(IO_INDEX_SIZE,indexSize);
        }
        index.sync();
        phys.sync();

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
    public void clearCache() {
    }

    @Override
    public void compact() {

        if(readOnly) throw new IllegalAccessError();
        index.putLong(IO_PHYS_SIZE,physSize);
        index.putLong(IO_INDEX_SIZE,indexSize);

        if(index.getFile()==null) throw new UnsupportedOperationException("compact not supported for memory storage yet");
        structuralLock.lock();
        for(ReentrantReadWriteLock l:locks) l.writeLock().lock();
        try{
            //create secondary files for compaction
            //TODO RAF
            //TODO memory based stores
            final File indexFile = index.getFile();
            final File physFile = phys.getFile();
            int rafMode = 0;
            if(index instanceof  Volume.FileChannelVol) rafMode=2;
            if(index instanceof  Volume.MappedFileVol && phys instanceof Volume.FileChannelVol) rafMode = 1;

            final boolean isRaf = index instanceof Volume.FileChannelVol;
            Volume.Factory fab = Volume.fileFactory(false, rafMode, new File(indexFile+".compact"));
            StoreDirect store2 = new StoreDirect(fab);
            store2.structuralLock.lock();

            //transfer stack of free recids
            for(long recid =longStackTake(IO_FREE_RECID);
                recid!=0; recid=longStackTake(IO_FREE_RECID)){
                store2.longStackPut(recid, IO_FREE_RECID);
            }

            //iterate over recids and transfer physical records
            store2.index.putLong(IO_INDEX_SIZE, indexSize);

            for(long ioRecid = IO_USER_START; ioRecid<indexSize;ioRecid+=8){
                byte[] bb = get2(ioRecid,Serializer.BYTE_ARRAY_SERIALIZER);
                store2.index.ensureAvailable(ioRecid+8);
                if(bb==null||bb.length==0){
                    store2.index.putLong(ioRecid,0);
                }else{
                    long[] indexVals = store2.physAllocate(bb.length,true);
                    DataOutput2 out = new DataOutput2();
                    out.buf = bb;
                    out.pos = bb.length;
                    store2.put2(out, ioRecid,indexVals);
                }
            }



            File indexFile2 = store2.index.getFile();
            File physFile2 = store2.phys.getFile();
            store2.structuralLock.unlock();
            store2.close();

            long time = System.currentTimeMillis();
            File indexFile_ = new File(indexFile.getPath()+"_"+time+"_orig");
            File physFile_ = new File(physFile.getPath()+"_"+time+"_orig");

            index.close();
            phys.close();
            if(!indexFile.renameTo(indexFile_))throw new InternalError("could not rename file");
            if(!physFile.renameTo(physFile_))throw new InternalError("could not rename file");

            if(!indexFile2.renameTo(indexFile))throw new InternalError("could not rename file");
            //TODO process may fail in middle of rename, analyze sequence and add recovery
            if(!physFile2.renameTo(physFile))throw new InternalError("could not rename file");

            indexFile_.delete();
            physFile_.delete();

            Volume.Factory fac2 = Volume.fileFactory(false, rafMode, indexFile);

            index = fac2.createIndexVolume();
            phys = fac2.createPhysVolume();

            physSize = store2.physSize;
            index.putLong(IO_PHYS_SIZE, physSize);
            index.putLong(IO_INDEX_SIZE, indexSize);
            index.putLong(IO_INDEX_SIZE, indexSize);

        }catch(IOException e){
            throw new IOError(e);
        }finally {
            structuralLock.unlock();
            for(ReentrantReadWriteLock l:locks) l.writeLock().unlock();
        }

    }


    protected long longStackTake(final long ioList) {
        if(!structuralLock.isLocked())throw new InternalError();
        if(ioList<IO_FREE_RECID || ioList>=IO_USER_START) throw new IllegalArgumentException("wrong ioList: "+ioList);

        final long dataOffset = index.getLong(ioList) &MASK_OFFSET;
        if(dataOffset == 0)
            return 0; //there is no such list, so just return 0


        final int numberOfRecordsInPage = phys.getUnsignedByte(dataOffset);

        if(numberOfRecordsInPage<=0)
            throw new InternalError();
        if(numberOfRecordsInPage> LONG_STACK_PER_PAGE)
            throw new InternalError();

        final long ret = phys.getSixLong(dataOffset + 2 + numberOfRecordsInPage * 6);

        //was it only record at that page?
        if(numberOfRecordsInPage == 1){
            //yes, delete this page
            final long previousListPhysid =phys.getSixLong(dataOffset+2);
            if(previousListPhysid !=0){
                //update index so it points to previous page
                index.putLong(ioList , previousListPhysid | (((long) LONG_STACK_PAGE_SIZE) << 48));
            }else{
                //zero out index
                index.putLong(ioList , 0L);
            }
            //put space used by this page into free list
            freePhysPut(dataOffset | (((long)LONG_STACK_PAGE_SIZE)<<48));
        }else{
            //no, it was not last record at this page, so just decrement the counter
            phys.putUnsignedByte(dataOffset, (byte) (numberOfRecordsInPage - 1));
        }

        //System.out.println("longStackTake: "+ioList+" - "+ret);

        return ret;

    }


    protected void longStackPut(final long ioList, long offset){
        if(offset>>>48!=0) throw new IllegalArgumentException();
        if(!structuralLock.isLocked())throw new InternalError();
        if(ioList<IO_FREE_RECID || ioList>=IO_USER_START) throw new InternalError("wrong ioList: "+ioList);

        //System.out.println("longStackPut: "+ioList+" - "+offset);

        //index position was cleared, put into free index list
        final long listPhysid2 = index.getLong(ioList) &MASK_OFFSET;

        if(listPhysid2 == 0){ //empty list?
            //yes empty, create new page and fill it with values
            final long listPhysid = freePhysTake(LONG_STACK_PAGE_SIZE,true) &MASK_OFFSET;
            if(listPhysid == 0) throw new InternalError();
            //set previous Free Index List page to zero as this is first page
            phys.putSixLong(listPhysid + 2, 0L);
            //set number of free records in this page to 1
            phys.putUnsignedByte(listPhysid, (byte) 1);
            //set  record
            phys.putSixLong(listPhysid + 8, offset);
            //and update index file with new page location
            index.putLong(ioList , (((long) LONG_STACK_PAGE_SIZE) << 48) | listPhysid);
        }else{
            final int numberOfRecordsInPage = phys.getUnsignedByte(listPhysid2);
            if(numberOfRecordsInPage == LONG_STACK_PER_PAGE){ //is current page full?
                //yes it is full, so we need to allocate new page and write our number there

                final long listPhysid = freePhysTake(LONG_STACK_PAGE_SIZE,true) &MASK_OFFSET;
                if(listPhysid == 0) throw new InternalError();
                //final ByteBuffers dataBuf = dataBufs[((int) (listPhysid / BUF_SIZE))];
                //set location to previous page
                phys.putSixLong(listPhysid + 2, listPhysid2);
                //set number of free records in this page to 1
                phys.putUnsignedByte(listPhysid, (byte) 1);
                //set free record
                phys.putSixLong(listPhysid + 8, offset);
                //and update index file with new page location
                index.putLong(ioList , (((long) LONG_STACK_PAGE_SIZE) << 48) | listPhysid);
            }else{
                //there is space on page, so just write released recid and increase the counter
                phys.putSixLong(listPhysid2 + 8 + 6 * numberOfRecordsInPage, offset);
                phys.putUnsignedByte(listPhysid2, (byte) (numberOfRecordsInPage + 1));
            }
        }
    }



    protected void freeIoRecidPut(long ioRecid) {
        if(spaceReclaimTrack)
            longStackPut(IO_FREE_RECID, ioRecid);
    }

    protected long freeIoRecidTake(boolean ensureAvail){
        if(spaceReclaimTrack){
            long ioRecid = longStackTake(IO_FREE_RECID);
            if(ioRecid!=0) return ioRecid;
        }
        indexSize+=8;
        if(ensureAvail)
            index.ensureAvailable(indexSize);
        return indexSize-8;
    }

    protected static final long size2ListIoRecid(long size){
        return IO_FREE_RECID + 8 + ((size-1)/16)*8;
    }
    protected void freePhysPut(long indexVal) {
        long size = (indexVal&MASK_SIZE) >>>48;
        longStackPut(size2ListIoRecid(size), indexVal & MASK_OFFSET);
    }

    protected long freePhysTake(int size, boolean ensureAvail) {
        if(size==0)throw new IllegalArgumentException();

        //check free space
        if(spaceReclaimReuse){
            long ret =  longStackTake(size2ListIoRecid(size));
            if(ret!=0) return ret;
        }
        //not available, increase file size
        if(physSize%Volume.BUF_SIZE+size>Volume.BUF_SIZE)
            physSize += Volume.BUF_SIZE - physSize%Volume.BUF_SIZE;
        long physSize2 = physSize;
        physSize = roundTo16(physSize+size);
        if(ensureAvail)
            phys.ensureAvailable(physSize);
        return physSize2;
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

    @Override
    public long getMaxRecid() {
        return (indexSize-IO_USER_START)/8;
    }

    @Override
    public ByteBuffer getRaw(long recid) {
        //TODO use direct BB
        byte[] bb = get(recid, Serializer.BYTE_ARRAY_SERIALIZER);
        if(bb==null) return null;
        return ByteBuffer.wrap(bb);
    }

    @Override
    public Iterator<Long> getFreeRecids() {
        return Collections.emptyIterator(); //TODO iterate over stack of free recids, without modifying it
    }

    @Override
    public void updateRaw(long recid, ByteBuffer data) {
        long ioRecid = recid*8 + IO_USER_START;
        if(ioRecid>=indexSize){
            indexSize = ioRecid+8;
            index.ensureAvailable(indexSize);
        }

        byte[] b = null;

        if(data!=null) synchronized (data){
            b = new byte[data.remaining()];
            data.get(b);
        }
        //TODO use BB without copying
        update(recid, b, Serializer.BYTE_ARRAY_SERIALIZER);
    }
}
