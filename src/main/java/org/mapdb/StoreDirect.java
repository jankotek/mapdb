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
import java.util.Iterator;
import java.util.concurrent.locks.Lock;

/**
 * Storage Engine which saves record directly into file.
 * It has zero protection from data corruption and must be closed properly after modifications.
 * It is  used when Write-Ahead-Log transactions are disabled.
 *
 *
 * Storage format
 * ----------------
 * `StoreDirect` is composed of two files: Index file is sequence of 8-byte longs, it translates
 * `recid` (offset in index file) to record size and offset in physical file. Records position
 * may change, but it requires stable ID, so the index file is used for translation.
 * This store uses data structure called `Long Stack` to manage (and reuse) free space, it is
 * is linked LIFO queue of 8-byte longs.
 *
 * Index file
 * --------------
 * Index file is translation table between permanent record ID (recid) and mutable location in physical file.
 * Index file is sequence of 8-byte longs, one for each record. It also has some extra longs to manage
 * free space and other metainfo. Index table and physical data could be stored in single file, but
 * keeping index table separate simplifies compaction.
 *
 * Basic **structure of index file** is bellow. Each slot is 8-bytes long so `offset=slot*8`
 *
 *  slot        | in code                       | description
 *  ---         | ---                               | ---
 *  0           | {@link StoreDirect#HEADER}        | File header, format version and flags
 *  1           | {@link StoreDirect#IO_INDEX_SIZE} | Allocated file size of index file in bytes.
 *  2           | {@link StoreDirect#IO_PHYS_SIZE}  | Allocated file size of physical file in bytes.
 *  3           | {@link StoreDirect#IO_FREE_SIZE}  | Space occupied by free records in physical file in bytes.
 *  4           | {@link StoreDirect#IO_INDEX_SUM}  | Checksum of all Index file headers. Checks if store was closed correctly
 *  5..9        |                                   | Reserved for future use
 *  10..14      |                                   | For usage by user
 *  15          | {@link StoreDirect#IO_FREE_RECID} |Long Stack of deleted recids, those will be reused and returned by {@link Engine#put(Object, Serializer)}
 *  16..4111    |                                   |Long Stack of free physical records. This contains free space released by record update or delete. Each slots corresponds to free record size. TODO check 4111 is right
 *  4112        | {@link StoreDirect#IO_USER_START} |Record size and offset in physical file for recid=1
 *  4113        |                                   |Record size and offset in physical file for recid=2
 *  ...         | ...                               |... snip ...
 *  N+4111      |                                   |Record size and offset in physical file for recid=N
 *
 *
 * Long Stack
 * ------------
 * Long Stack is data structure used to store free records. It is LIFO queue which uses linked records to store 8-byte longs.
 * Long Stack is identified by slot in Index File, which stores pointer to Long Stack head.  The structure of
 * of index pointer is following:
 *
 *  byte    | description
 *  ---     |---
 *  0..1    | relative offset in head Long Stack Record to take value from. This value decreases by 8 each take
 *  2..7    | physical file offset of head Long Stack Record, zero if Long Stack is empty
 *
 * Each Long Stack Record  is sequence of 8-byte longs, first slot is header. Long Stack Record structure is following:
 *
 *  byte    | description
 *  ---     |---
 *  0..1    | length of current Long Stack Record in bytes
 *  2..7    | physical file offset of next Long Stack Record, zero of this record is last
 *  8-15    | Long Stack value
 *  16-23   | Long Stack value
 *   ...    | and so on until end of Long Stack Record
 *
 * Physical pointer
 * ----------------
 * Index slot value typically contains physical pointer (information about record location and size in physical file). First 2 bytes
 * are record size (max 65536). Then there is 6 byte offset in physical file (max store size is 281 TB).
 * Physical file offset must always be multiple of 16, so last 4 bites are used to flag extra record information.
 * Structure of **physical pointer**:
 *
 * bite     | in code                                   | description
 *   ---    | ---                                       | ---
 * 0-15     |`val>>>48`                                 | record size
 * 16-59    |`val&{@link StoreDirect#MASK_OFFSET}`      | physical offset
 * 60       |`val&{@link StoreDirect#MASK_LINKED}!=0`   | linked record flag
 * 61       |`val&{@link StoreDirect#MASK_DISCARD}!=0`  | to be discarded while storage is offline flag
 * 62       |`val&{@link StoreDirect#MASK_ARCHIVE}!=0`  | record modified since last backup flag
 * 63       |                                           | not used yet
 *
 * Records in Physical File
 * ---------------------------
 * Records are stored in physical file. Maximal record size size is 64KB, so larger records must
 * be stored in form of the linked list. Each record starts by Physical Pointer from Index File.
 * There is flag in Physical Pointer indicating if record is linked. If record is not linked you may
 * just read ByteBuffer from given size and offset.
 *
 * If record is linked, each record starts with Physical Pointer to next record. So actual data payload is record size-8.
 * The last linked record does not have the Physical Pointer header to next record, there is MASK_LINKED flag which
 * indicates if next record is the last one.
 *
 *
 * @author Jan Kotek
 */
public class StoreDirect extends Store{

    protected static final long MASK_OFFSET = 0x0000FFFFFFFFFFF0L;

    protected static final long MASK_LINKED = 0x8L;
    protected static final long MASK_DISCARD = 0x4L;
    protected static final long MASK_ARCHIVE = 0x2L;

    /** 4 byte file header */
    protected static final int HEADER = 893298482;

    /** 2 byte store version*/
    protected static final short STORE_VERSION = 10000;

    /** maximal non linked record size */
    protected static final int MAX_REC_SIZE = 65536-1;

    /** number of free physical slots */
    protected static final int PHYS_FREE_SLOTS_COUNT = 2048*2;

    /** index file offset where current size of index file is stored*/
    protected static final int IO_INDEX_SIZE = 1*8;
    /** index file offset where current size of phys file is stored */
    protected static final int IO_PHYS_SIZE = 2*8;

    /** index file offset where space occupied by free phys records is stored */
    protected static final int IO_FREE_SIZE = 3*8;

    /** checksum of all index file headers. Used to verify store was closed correctly */
    protected static final int IO_INDEX_SUM = 4*8;

    /** index file offset where reference to longstack of free recid is stored*/
    protected static final int IO_FREE_RECID = 15*8;

    /** index file offset where first recid available to user is stored */
    protected static final int IO_USER_START = IO_FREE_RECID+PHYS_FREE_SLOTS_COUNT*8+8;

    public static final String DATA_FILE_EXT = ".p";

    protected final static int LONG_STACK_PREF_COUNT = 204;
    protected final static long LONG_STACK_PREF_SIZE = 8+LONG_STACK_PREF_COUNT*6;
    protected final static int LONG_STACK_PREF_COUNT_ALTER = 212;
    protected final static long LONG_STACK_PREF_SIZE_ALTER = 8+LONG_STACK_PREF_COUNT_ALTER*6;



    protected Volume index;
    protected Volume phys;

    protected long physSize;
    protected long indexSize;
    protected long freeSize;

    protected final boolean deleteFilesAfterClose;

    protected final boolean readOnly;
    protected final boolean syncOnCommitDisabled;

    protected final boolean spaceReclaimReuse;
    protected final boolean spaceReclaimSplit;
    protected final boolean spaceReclaimTrack;

    protected final long sizeLimit;
    protected final boolean fullChunkAllocation;

    /** maximal non zero slot in free phys record, access requires `structuralLock`*/
    protected long maxUsedIoList = 0;



    public StoreDirect(Volume.Factory volFac, boolean readOnly, boolean deleteFilesAfterClose,
                       int spaceReclaimMode, boolean syncOnCommitDisabled, long sizeLimit,
                       boolean checksum, boolean compress, byte[] password, boolean fullChunkAllocation) {
        super(checksum, compress, password);
        this.readOnly = readOnly;
        this.deleteFilesAfterClose = deleteFilesAfterClose;
        this.syncOnCommitDisabled = syncOnCommitDisabled;
        this.sizeLimit = sizeLimit;
        this.fullChunkAllocation = fullChunkAllocation;

        this.spaceReclaimSplit = spaceReclaimMode>4;
        this.spaceReclaimReuse = spaceReclaimMode>2;
        this.spaceReclaimTrack = spaceReclaimMode>0;

        boolean allGood = false;

        try{
            index = volFac.createIndexVolume();
            phys = volFac.createPhysVolume();
            if(index.isEmpty()){
                createStructure();
            }else{
                checkHeaders();
                indexSize = index.getLong(IO_INDEX_SIZE);
                physSize = index.getLong(IO_PHYS_SIZE);
                freeSize = index.getLong(IO_FREE_SIZE);

                maxUsedIoList=IO_USER_START-8;
                while(index.getLong(maxUsedIoList)!=0 && maxUsedIoList>IO_FREE_RECID)
                    maxUsedIoList-=8;
            }
            allGood = true;
        }finally{
            if(!allGood){
                //exception was thrown, try to unlock files
                if(index!=null){
                    index.sync();
                    index.close();
                    index = null;
                }
                if(phys!=null){
                    phys.sync();
                    phys.close();
                    phys = null;
                }
            }
        }

    }

    public StoreDirect(Volume.Factory volFac) {
        this(volFac, false,false,5,false,0L, false,false,null, false);
    }



    protected void checkHeaders() {
        if(index.getInt(0)!=HEADER||phys.getInt(0)!=HEADER)
            throw new IOError(new IOException("storage has invalid header"));

        if(index.getUnsignedShort(4)>StoreDirect.STORE_VERSION || phys.getUnsignedShort(4)>StoreDirect.STORE_VERSION )
            throw new IOError(new IOException("New store format version, please use newer MapDB version"));

        final int masks = index.getUnsignedShort(6);
        if(masks!=phys.getUnsignedShort(6))
            throw new IllegalArgumentException("Index and Phys file have different feature masks");

        if(masks!=expectedMasks())
            throw new IllegalArgumentException("File created with different features. Please check compression, checksum or encryption");


        long checksum = index.getLong(IO_INDEX_SUM);
        if(checksum!=indexHeaderChecksum())
            throw new IOError(new IOException("Wrong index checksum, store was not closed properly and could be corrupted."));
    }

    protected void createStructure() {
        indexSize = IO_USER_START+LAST_RESERVED_RECID*8+8;
        assert(indexSize>IO_USER_START);
        index.ensureAvailable(indexSize);
        for(int i=0;i<indexSize;i+=8) index.putLong(i,0L);
        index.putInt(0, HEADER);
        index.putUnsignedShort(4,STORE_VERSION);
        index.putUnsignedShort(6,expectedMasks());
        index.putLong(IO_INDEX_SIZE,indexSize);
        physSize =16;
        index.putLong(IO_PHYS_SIZE,physSize);
        phys.ensureAvailable(physSize);
        phys.putInt(0, HEADER);
        phys.putUnsignedShort(4,STORE_VERSION);
        phys.putUnsignedShort(6,expectedMasks());
        freeSize = 0;
        index.putLong(IO_FREE_SIZE,freeSize);
        index.putLong(IO_INDEX_SUM,indexHeaderChecksum());
    }

    protected long indexHeaderChecksum(){
        long ret = 0;
        for(long offset = 0;offset<IO_USER_START;offset+=8){
            if(offset == IO_INDEX_SUM) continue;
            long indexVal = index.getLong(offset);
            ret |=  indexVal | LongHashMap.longHash(indexVal|offset) ;
        }
        return ret;
    }

    @Override
    public long preallocate() {
        newRecidLock.readLock().lock();
        try{
            structuralLock.lock();
            final long ioRecid;
            try{
                ioRecid = freeIoRecidTake(true) ;
            }finally {
                structuralLock.unlock();
            }

            final Lock lock  = locks[Store.lockPos(ioRecid)].writeLock();
            lock.lock();
            try{
                index.putLong(ioRecid,MASK_DISCARD);
            }finally {
                lock.unlock();
            }
            long recid = (ioRecid-IO_USER_START)/8;
            assert(recid>0);
            return recid;
        }finally {
            newRecidLock.readLock().unlock();
        }
    }

    @Override
    public void preallocate(long[] recids) {
        newRecidLock.readLock().lock();
        try{
            structuralLock.lock();
            try{
                for(int i=0;i<recids.length;i++)
                    recids[i] = freeIoRecidTake(true) ;
            }finally {
                structuralLock.unlock();
            }
            for(int i=0;i<recids.length;i++){
                final long ioRecid = recids[i];
                final Lock lock  = locks[Store.lockPos(ioRecid)].writeLock();
                lock.lock();
                try{
                    index.putLong(ioRecid,MASK_DISCARD);
                }finally {
                    lock.unlock();
                }
                recids[i] = (ioRecid-IO_USER_START)/8;
                assert(recids[i]>0);
            }
        }finally {
            newRecidLock.readLock().unlock();
        }
    }


    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        assert(value!=null);
        DataOutput2 out = serialize(value, serializer);
        final long ioRecid;
        newRecidLock.readLock().lock();
        try{

            structuralLock.lock();

            final long[] indexVals;
            try{
                ioRecid = freeIoRecidTake(true) ;
                indexVals = physAllocate(out.pos,true,false);
            }finally {
                structuralLock.unlock();
            }
            final Lock lock  = locks[Store.lockPos(ioRecid)].writeLock();
            lock.lock();
            try{
                put2(out, ioRecid, indexVals);
            }finally {
                lock.unlock();
            }
        }finally {
            newRecidLock.readLock().unlock();
        }
        recycledDataOuts.offer(out);
        long recid = (ioRecid-IO_USER_START)/8;
        assert(recid>0);
        return recid;
    }

    protected void put2(DataOutput2 out, long ioRecid, long[] indexVals) {
        assert(locks[Store.lockPos(ioRecid)].writeLock().isHeldByCurrentThread());
        index.putLong(ioRecid, indexVals[0]|MASK_ARCHIVE);
        //write stuff
        if(indexVals.length==1||indexVals[1]==0){ //is more then one? ie linked
            //write single

            phys.putData(indexVals[0]&MASK_OFFSET, out.buf, 0, out.pos);

        }else{
            int outPos = 0;
            //write linked
            for(int i=0;i<indexVals.length;i++){
                final int c =   i==indexVals.length-1 ? 0: 8;
                final long indexVal = indexVals[i];
                final boolean isLast = (indexVal & MASK_LINKED) ==0;
                assert(isLast==(i==indexVals.length-1));
                final int size = (int) (indexVal>>>48);
                final long offset = indexVal&MASK_OFFSET;

                //write data
                phys.putData(offset+c,out.buf,outPos, size-c);
                outPos+=size-c;

                if(c>0){
                    //write position of next linked record
                    phys.putLong(offset, indexVals[i + 1]);
                }
            }
              if(outPos!=out.pos) throw new AssertionError();
        }
    }


    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        assert(recid>0);
        final long ioRecid = IO_USER_START + recid*8;
        final Lock lock  = locks[Store.lockPos(ioRecid)].readLock();
        lock.lock();
        try{
            return get2(ioRecid,serializer);
        }catch(IOException e){
            throw new IOError(e);
        }finally{
            lock.unlock();
        }
    }

    protected <A> A get2(long ioRecid,Serializer<A> serializer) throws IOException {
        assert(locks[Store.lockPos(ioRecid)].getWriteHoldCount()==0||
                locks[Store.lockPos(ioRecid)].writeLock().isHeldByCurrentThread());

        long indexVal = index.getLong(ioRecid);
        if(indexVal == MASK_DISCARD) return null; //preallocated record

        int size = (int) (indexVal>>>48);
        DataInput2 di;
        long offset = indexVal&MASK_OFFSET;
        if((indexVal& MASK_LINKED)==0){
            //read single record
            di = phys.getDataInput(offset, size);

        }else{
            //is linked, first construct buffer we will read data to
            int pos = 0;
            int c = 8;
            //TODO use mapped bb and direct copying?
            byte[] buf = new byte[64];
            //read parts into segment
            for(;;){
                DataInput2 in = phys.getDataInput(offset + c, size-c);

                if(buf.length<pos+size-c)
                    buf = Arrays.copyOf(buf,Math.max(pos+size-c,buf.length*2)); //buf to small, grow
                in.readFully(buf,pos,size-c);
                pos+=size-c;
                if(c==0) break;
                //read next part
                long next = phys.getLong(offset);
                offset = next&MASK_OFFSET;
                size = (int) (next>>>48);
                //is the next part last?
                c =  ((next& MASK_LINKED)==0)? 0 : 8;
            }
            di = new DataInput2(buf);
            size = pos;
        }
        return deserialize(serializer, size, di);
    }



    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        assert(value!=null);
        assert(recid>0);
        DataOutput2 out = serialize(value, serializer);

        final long ioRecid = IO_USER_START + recid*8;

        final Lock lock  = locks[Store.lockPos(ioRecid)].writeLock();
        lock.lock();
        try{
            update2(out, ioRecid);
        }finally{
            lock.unlock();
        }
        recycledDataOuts.offer(out);
    }

    protected void update2(DataOutput2 out, long ioRecid) {
        final long indexVal = index.getLong(ioRecid);
        final int size = (int) (indexVal>>>48);
        final boolean linked = (indexVal&MASK_LINKED)!=0;
        assert(locks[Store.lockPos(ioRecid)].writeLock().isHeldByCurrentThread());

        if(!linked && out.pos>0 && size>0 && size2ListIoRecid(size) == size2ListIoRecid(out.pos)){
            //size did change, but still fits into this location
            final long offset = indexVal & MASK_OFFSET;

            //note: if size would not change, we still have to write MASK_ARCHIVE bit
            index.putLong(ioRecid, (((long)out.pos)<<48)|offset|MASK_ARCHIVE);

            phys.putData(offset, out.buf, 0, out.pos);
        }else{

            long[] indexVals = spaceReclaimTrack ? getLinkedRecordsIndexVals(indexVal) : null;

            structuralLock.lock();
            try{

                if(spaceReclaimTrack){
                    //free first record pointed from indexVal
                    if(size>0)
                        freePhysPut(indexVal,false);

                    //if there are more linked records, free those as well
                    if(indexVals!=null){
                        for(int i=0;i<indexVals.length && indexVals[i]!=0;i++){
                            freePhysPut(indexVals[i],false);
                        }
                    }
                }

                indexVals = physAllocate(out.pos,true,false);
            }finally {
                structuralLock.unlock();
            }

            put2(out, ioRecid, indexVals);
        }
        assert(locks[Store.lockPos(ioRecid)].writeLock().isHeldByCurrentThread());
    }


    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        assert(expectedOldValue!=null && newValue!=null);
        assert(recid>0);
        final long ioRecid = IO_USER_START + recid*8;
        final Lock lock  = locks[Store.lockPos(ioRecid)].writeLock();
        lock.lock();

        DataOutput2 out;
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
             out = serialize(newValue, serializer);

            update2(out, ioRecid);

        }catch(IOException e){
            throw new IOError(e);
        }finally{
            lock.unlock();
        }
        recycledDataOuts.offer(out);
        return true;
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        assert(recid>0);
        final long ioRecid = IO_USER_START + recid*8;
        final Lock lock  = locks[Store.lockPos(ioRecid)].writeLock();
        lock.lock();
        try{
            //get index val and zero it out
            final long indexVal = index.getLong(ioRecid);
            index.putLong(ioRecid,0L|MASK_ARCHIVE);

            if(!spaceReclaimTrack) return; //free space is not tracked, so do not mark stuff as free

            long[] linkedRecords = getLinkedRecordsIndexVals(indexVal);

            //now lock everything and mark free space
            structuralLock.lock();
            try{
                //free recid
                freeIoRecidPut(ioRecid);
                //free first record pointed from indexVal\
                if((indexVal>>>48)>0)
                    freePhysPut(indexVal,false);

                //if there are more linked records, free those as well
                if(linkedRecords!=null){
                    for(int i=0; i<linkedRecords.length &&linkedRecords[i]!=0;i++){
                        freePhysPut(linkedRecords[i],false);
                    }
                }
            }finally {
                structuralLock.unlock();
            }

        }finally{
            lock.unlock();
        }
    }

    protected long[] getLinkedRecordsIndexVals(long indexVal) {
        long[] linkedRecords = null;

        int linkedPos = 0;
        if((indexVal& MASK_LINKED)!=0){
            //record is composed of multiple linked records, so collect all of them
            linkedRecords = new long[2];

            //traverse linked records
            long linkedVal = phys.getLong(indexVal&MASK_OFFSET);
            for(;;){
                if(linkedPos==linkedRecords.length) //grow if necessary
                    linkedRecords = Arrays.copyOf(linkedRecords, linkedRecords.length * 2);
                //store last linkedVal
                linkedRecords[linkedPos] = linkedVal;

                if((linkedVal& MASK_LINKED)==0){
                    break; //this is last linked record, so break
                }
                //move and read to next
                linkedPos++;
                linkedVal = phys.getLong(linkedVal&MASK_OFFSET);
            }
        }
        return linkedRecords;
    }

    protected long[] physAllocate(int size, boolean ensureAvail,boolean recursive) {
        assert(structuralLock.isHeldByCurrentThread());
        if(size==0L) return new long[]{0L};
        //append to end of file
        if(size<MAX_REC_SIZE){
            long indexVal = freePhysTake(size,ensureAvail,recursive);
            indexVal |= ((long)size)<<48;
            return new long[]{indexVal};
        }else{
            long[] ret = new long[2];
            int retPos = 0;
            int c = 8;

            while(size>0){
                if(retPos == ret.length) ret = Arrays.copyOf(ret, ret.length*2);
                int allocSize = Math.min(size, MAX_REC_SIZE);
                size -= allocSize - c;

                //append to end of file
                long indexVal = freePhysTake(allocSize, ensureAvail,recursive);
                indexVal |= (((long)allocSize)<<48);
                if(c!=0) indexVal|= MASK_LINKED;
                ret[retPos++] = indexVal;

                c = size<=MAX_REC_SIZE ? 0 : 8;
            }
            if(size!=0) throw new AssertionError();

            return Arrays.copyOf(ret, retPos);
        }
    }

    protected static long roundTo16(long offset){
        long rem = offset&15;  // modulo 16
        if(rem!=0) offset +=16-rem;
        return offset;
    }

    @Override
    public void close() {
        lockAllWrite();
        try{

            if(!readOnly){
                if(serializerPojo!=null && serializerPojo.hasUnsavedChanges()){
                    serializerPojo.save(this);
                }

                index.putLong(IO_PHYS_SIZE,physSize);
                index.putLong(IO_INDEX_SIZE,indexSize);
                index.putLong(IO_FREE_SIZE,freeSize);

                index.putLong(IO_INDEX_SUM,indexHeaderChecksum());
            }

            // Syncs are expensive -- don't sync if the files are going to
            // get deleted anyway.
            if (!deleteFilesAfterClose) {
              index.sync();
              phys.sync();
            }
            index.close();
            phys.close();
            if(deleteFilesAfterClose){
                index.deleteFile();
                phys.deleteFile();
            }
            index = null;
            phys = null;
        }finally{
            unlockAllWrite();
        }
    }

    @Override
    public boolean isClosed() {
        return index==null;
    }

    @Override
    public void commit() {
        if(!readOnly){

            if(serializerPojo!=null && serializerPojo.hasUnsavedChanges()){
                serializerPojo.save(this);
            }

            index.putLong(IO_PHYS_SIZE,physSize);
            index.putLong(IO_INDEX_SIZE,indexSize);
            index.putLong(IO_FREE_SIZE,freeSize);

            index.putLong(IO_INDEX_SUM, indexHeaderChecksum());
        }
        if(!syncOnCommitDisabled){
            index.sync();
            phys.sync();
        }
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
    public boolean canRollback(){
        return false;
    }

    @Override
    public void clearCache() {
    }

    @Override
    public void compact() {

        if(readOnly) throw new IllegalAccessError();

        final File indexFile = index.getFile();
        final File physFile = phys.getFile();

        final int rafMode;
        if(index instanceof  Volume.FileChannelVol){
            rafMode=2;
        }else if(index instanceof  Volume.MappedFileVol && phys instanceof Volume.FileChannelVol){
            rafMode = 1;
        }else{
            rafMode = 0;
        }


        lockAllWrite();
        try{
            final File compactedFile = new File((indexFile!=null?indexFile:File.createTempFile("mapdb","compact"))+".compact");
            Volume.Factory fab = Volume.fileFactory(false, rafMode,compactedFile,sizeLimit,fullChunkAllocation);
            StoreDirect store2 = new StoreDirect(fab,false,false,5,false,0L, checksum,compress,password, fullChunkAllocation);

            compactPreUnderLock();

            index.putLong(IO_PHYS_SIZE,physSize);
            index.putLong(IO_INDEX_SIZE,indexSize);
            index.putLong(IO_FREE_SIZE,freeSize);

            //create secondary files for compaction
            store2.lockAllWrite();

            //transfer stack of free recids
            //TODO long stack take modifies the original store
            for(long recid =longStackTake(IO_FREE_RECID,false);
                recid!=0; recid=longStackTake(IO_FREE_RECID,false)){
                store2.longStackPut(IO_FREE_RECID,recid, false);
            }

            //iterate over recids and transfer physical records
            store2.index.putLong(IO_INDEX_SIZE, indexSize);

            for(long ioRecid = IO_USER_START; ioRecid<indexSize;ioRecid+=8){
                byte[] bb = get2(ioRecid,Serializer.BYTE_ARRAY_NOSIZE);
                store2.index.ensureAvailable(ioRecid+8);
                if(bb==null||bb.length==0){
                    store2.index.putLong(ioRecid,0);
                }else{
                    DataOutput2 out = serialize(bb,Serializer.BYTE_ARRAY_NOSIZE);
                    long[] indexVals = store2.physAllocate(out.pos,true,false);
                    store2.put2(out, ioRecid,indexVals);
                }
            }

            File indexFile2 = store2.index.getFile();
            File physFile2 = store2.phys.getFile();
            store2.unlockAllWrite();

            final boolean useDirectBuffer = index instanceof Volume.MemoryVol &&
                    ((Volume.MemoryVol)index).useDirectBuffer;
            index.sync(); //TODO is sync needed here?
            index.close();
            index = null;
            phys.sync(); //TODO is sync needed here?
            phys.close();
            phys = null;

            if(indexFile != null){
                final long time = System.currentTimeMillis();
                final File indexFile_ = indexFile!=null? new File(indexFile.getPath()+"_"+time+"_orig"): null;
                final File physFile_ = physFile!=null? new File(physFile.getPath()+"_"+time+"_orig") : null;

                store2.close();
                //not in memory, so just rename files
                if(!indexFile.renameTo(indexFile_))
                    throw new AssertionError("could not rename file");
                if(!physFile.renameTo(physFile_))
                    throw new AssertionError("could not rename file");

                if(!indexFile2.renameTo(indexFile))
                    throw new AssertionError("could not rename file");
                //TODO process may fail in middle of rename, analyze sequence and add recovery
                if(!physFile2.renameTo(physFile))
                    throw new AssertionError("could not rename file");

                final Volume.Factory fac2 = Volume.fileFactory(false, rafMode, indexFile,sizeLimit,fullChunkAllocation);
                index = fac2.createIndexVolume();
                phys = fac2.createPhysVolume();

                indexFile_.delete();
                physFile_.delete();
            }else{
                //in memory, so copy files into memory
                Volume indexVol2 = new Volume.MemoryVol(useDirectBuffer,sizeLimit,fullChunkAllocation);
                Volume.volumeTransfer(indexSize, store2.index, indexVol2);
                Volume physVol2 = new Volume.MemoryVol(useDirectBuffer,sizeLimit,fullChunkAllocation);
                Volume.volumeTransfer(store2.physSize, store2.phys, physVol2);

                store2.close();

                index = indexVol2;
                phys = physVol2;
            }




            physSize = store2.physSize;
            freeSize = store2.freeSize;
            index.putLong(IO_PHYS_SIZE, physSize);
            index.putLong(IO_INDEX_SIZE, indexSize);
            index.putLong(IO_FREE_SIZE, freeSize);
            index.putLong(IO_INDEX_SUM,indexHeaderChecksum());

            maxUsedIoList=IO_USER_START-8;
            while(index.getLong(maxUsedIoList)!=0 && maxUsedIoList>IO_FREE_RECID)
                maxUsedIoList-=8;

            compactPostUnderLock();

        }catch(IOException e){
            throw new IOError(e);
        }finally {
            unlockAllWrite();
        }

    }

    /** subclasses put additional checks before compaction starts here */
    protected void compactPreUnderLock() {
    }

    /** subclasses put additional cleanup after compaction finishes here */
    protected void compactPostUnderLock() {
    }


    protected long longStackTake(final long ioList, boolean recursive) {
        assert(structuralLock.isHeldByCurrentThread());
        assert(ioList>=IO_FREE_RECID && ioList<IO_USER_START) :"wrong ioList: "+ioList;

        long dataOffset = index.getLong(ioList);
        if(dataOffset == 0) return 0; //there is no such list, so just return 0

        long pos = dataOffset>>>48;
        dataOffset &= MASK_OFFSET;

        if(pos<8) throw new AssertionError();

        final long ret = phys.getSixLong(dataOffset + pos);

        //was it only record at that page?
        if(pos == 8){
            //yes, delete this page
            long next =phys.getLong(dataOffset);
            long size = next>>>48;
            next &=MASK_OFFSET;
            if(next !=0){
                //update index so it points to previous page
                long nextSize = phys.getUnsignedShort(next);
                assert((nextSize-8)%6==0);
                index.putLong(ioList , ((nextSize-6)<<48)|next);
            }else{
                //zero out index
                index.putLong(ioList , 0L);
                if(maxUsedIoList==ioList){
                    //max value was just deleted, so find new maxima
                    while(index.getLong(maxUsedIoList)==0 && maxUsedIoList>IO_FREE_RECID){
                        maxUsedIoList-=8;
                    }
                }
            }
            //put space used by this page into free list
            freePhysPut((size<<48) | dataOffset, true);
        }else{
            //no, it was not last record at this page, so just decrement the counter
            pos-=6;
            index.putLong(ioList, (pos<<48)| dataOffset); //TODO update just 2 bytes
        }

        //System.out.println("longStackTake: "+ioList+" - "+ret);

        return ret;

    }


    protected void longStackPut(final long ioList, long offset, boolean recursive){
        assert(structuralLock.isHeldByCurrentThread());
        assert(offset>>>48==0);
        assert(ioList>=IO_FREE_RECID && ioList<=IO_USER_START): "wrong ioList: "+ioList;

        long dataOffset = index.getLong(ioList);
        long pos = dataOffset>>>48;
        dataOffset &= MASK_OFFSET;

        if(dataOffset == 0){ //empty list?
            //TODO allocate pages of mixed size
            //yes empty, create new page and fill it with values
            final long listPhysid = freePhysTake((int) LONG_STACK_PREF_SIZE,true,true) &MASK_OFFSET;
            if(listPhysid == 0) throw new AssertionError();
            //set previous Free Index List page to zero as this is first page
            //also set size of this record
            phys.putLong(listPhysid , LONG_STACK_PREF_SIZE << 48);
            //set  record
            phys.putSixLong(listPhysid + 8, offset);
            //and update index file with new page location
            index.putLong(ioList , ( 8L << 48) | listPhysid);
            if(maxUsedIoList<=ioList) maxUsedIoList=ioList;
        }else{
            long next = phys.getLong(dataOffset);
            long size = next>>>48;
            next &=MASK_OFFSET;
            assert(pos+6<=size);
            if(pos+6==size){ //is current page full?
                long newPageSize = LONG_STACK_PREF_SIZE;
                if(ioList == size2ListIoRecid(LONG_STACK_PREF_SIZE)){
                    //TODO double allocation fix needs more investigation
                    newPageSize = LONG_STACK_PREF_SIZE_ALTER;
                }
                //yes it is full, so we need to allocate new page and write our number there
                final long listPhysid = freePhysTake((int) newPageSize,true,true) &MASK_OFFSET;
                if(listPhysid == 0) throw new AssertionError();

                //set location to previous page and set current page size
                phys.putLong(listPhysid, (newPageSize<<48)|(dataOffset&MASK_OFFSET));

                //set the value itself
                phys.putSixLong(listPhysid+8, offset);

                //and update index file with new page location and number of records
                index.putLong(ioList , (8L<<48) | listPhysid);
            }else{
                //there is space on page, so just write offset and increase the counter
                pos+=6;
                phys.putSixLong(dataOffset + pos, offset);
                index.putLong(ioList, (pos<<48)| dataOffset); //TODO update just 2 bytes
            }
        }
    }



    protected void freeIoRecidPut(long ioRecid) {
        assert(ioRecid>IO_USER_START);
        assert(locks[Store.lockPos(ioRecid)].writeLock().isHeldByCurrentThread());
        if(spaceReclaimTrack)
            longStackPut(IO_FREE_RECID, ioRecid,false);
    }

    protected long freeIoRecidTake(boolean ensureAvail){
        if(spaceReclaimTrack){
            long ioRecid = longStackTake(IO_FREE_RECID,false);
            if(ioRecid!=0){
                assert(ioRecid>IO_USER_START);
                return ioRecid;
            }
        }
        indexSize+=8;
        if(ensureAvail)
            index.ensureAvailable(indexSize);
        assert(indexSize-8>IO_USER_START);
        return indexSize-8;
    }

    protected static long size2ListIoRecid(long size){
        return IO_FREE_RECID + 8 + ((size-1)/16)*8;
    }
    protected void freePhysPut(long indexVal, boolean recursive) {
        assert(structuralLock.isHeldByCurrentThread());
        long size = indexVal >>>48;
        assert(size!=0);
        freeSize+=roundTo16(size);
        longStackPut(size2ListIoRecid(size), indexVal & MASK_OFFSET,recursive);
    }

    protected long freePhysTake(int size, boolean ensureAvail, boolean recursive) {
        assert(structuralLock.isHeldByCurrentThread());
        assert(size>0);
        //check free space
        if(spaceReclaimReuse){
            long ret =  longStackTake(size2ListIoRecid(size),recursive);
            if(ret!=0){
                freeSize-=roundTo16(size);
                return ret;
            }
        }
        //try to take large record and split it into two
        if(!recursive && spaceReclaimSplit ){
            for(long s=  roundTo16(size)+16;s<MAX_REC_SIZE;s+=16){
                final long ioList = size2ListIoRecid(s);
                if(ioList>maxUsedIoList) break;
                long ret = longStackTake(ioList,recursive);
                if(ret!=0){
                    //found larger record, split in two chunks, take first, mark second free
                    final long offset = ret & MASK_OFFSET;

                    long remaining = s - roundTo16(size);
                    long markFree = (remaining<<48) | (offset+s-remaining);
                    freePhysPut(markFree,recursive);

                    freeSize-=roundTo16(s);
                    return (((long)size)<<48) |offset;
                }
            }
        }

        //not available, increase file size
        if((physSize&Volume.CHUNK_SIZE_MOD_MASK)+size>Volume.CHUNK_SIZE)
            physSize += Volume.CHUNK_SIZE - (physSize&Volume.CHUNK_SIZE_MOD_MASK);
        long physSize2 = physSize;
        physSize = roundTo16(physSize+size);
        if(ensureAvail)
            phys.ensureAvailable(physSize);
        return physSize2;
    }


    @Override
    public long getMaxRecid() {
        return (indexSize-IO_USER_START)/8;
    }

    @Override
    public ByteBuffer getRaw(long recid) {
        //TODO use direct BB
        byte[] bb = get(recid, Serializer.BYTE_ARRAY_NOSIZE);
        if(bb==null) return null;
        return ByteBuffer.wrap(bb);
    }

    @Override
    public Iterator<Long> getFreeRecids() {
        return Fun.EMPTY_ITERATOR; //TODO iterate over stack of free recids, without modifying it
    }

    @Override
    public void updateRaw(long recid, ByteBuffer data) {
        long ioRecid = recid*8 + IO_USER_START;
        if(ioRecid>=indexSize){
            indexSize = ioRecid+8;
            index.ensureAvailable(indexSize);
        }

        byte[] b = null;

        if(data!=null){
            data = data.duplicate();
            b = new byte[data.remaining()];
            data.get(b);
        }
        //TODO use BB without copying
        update(recid, b, Serializer.BYTE_ARRAY_NOSIZE);
    }

    @Override
    public long getSizeLimit() {
        return sizeLimit;
    }

    @Override
    public long getCurrSize() {
        return physSize;
    }

    @Override
    public long getFreeSize() {
        return freeSize;
    }

    @Override
    public String calculateStatistics() {
        String s = "";
        s+=getClass().getName()+"\n";
        s+="volume: "+"\n";
        s+="  "+phys+"\n";

        s+="indexSize="+indexSize+"\n";
        s+="physSize="+physSize+"\n";
        s+="freeSize="+freeSize+"\n";

        s+="num of freeRecids: "+countLongStackItems(IO_FREE_RECID)+"\n";

        for(int size = 16;size<MAX_REC_SIZE+10;size*=2){
            long sum = 0;
            for(int ss=size/2;ss<size;s+=16){
                sum+=countLongStackItems(size2ListIoRecid(ss))*ss;
            }
            s+="Size occupied by free records (size="+size+") = "+sum;
        }


        return s;
    }

    protected long countLongStackItems(long ioList){
        long ret=0;
        long v = index.getLong(ioList);

        while(true){
            long next = v&MASK_OFFSET;
            if(next==0) return ret;
            ret+=v>>>48;
            v = phys.getLong(next);
        }

    }
}
