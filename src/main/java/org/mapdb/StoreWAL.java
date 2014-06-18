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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.zip.CRC32;

/**
 * Write-Ahead-Log
 */
public class StoreWAL extends StoreDirect {

    protected static final long LOG_MASK_OFFSET = 0x0000FFFFFFFFFFFFL;

    protected static final byte WAL_INDEX_LONG = 101;
    protected static final byte WAL_LONGSTACK_PAGE = 102;
    protected static final byte WAL_PHYS_ARRAY_ONE_LONG = 103;

    protected static final byte WAL_PHYS_ARRAY = 104;
    protected static final byte WAL_SKIP_REST_OF_BLOCK = 105;


    /** last instruction in log file */
    protected static final byte WAL_SEAL = 111;
    /** added to offset 8 into log file, indicates that log was synced and closed*/
    protected static final long LOG_SEAL = 4566556446554645L;

    public static final String TRANS_LOG_FILE_EXT = ".t";

    protected static final long[] TOMBSTONE = new long[0];
    protected static final long[] PREALLOC = new long[0];

    protected final Volume.Factory volFac;
    protected Volume log;

    protected volatile long logSize;

    protected final LongConcurrentHashMap<long[]> modified = new LongConcurrentHashMap<long[]>();
    protected final LongMap<byte[]> longStackPages = new LongHashMap<byte[]>();
    protected final long[] indexVals = new long[IO_USER_START/8];
    protected final boolean[] indexValsModified = new boolean[indexVals.length];

    protected boolean replayPending = true;


    protected final AtomicInteger logChecksum = new AtomicInteger();

    public StoreWAL(Volume.Factory volFac) {
        this(volFac, false, false, 5, false, 0L, false, false, null,false,0);
    }
    public StoreWAL(Volume.Factory volFac, boolean readOnly, boolean deleteFilesAfterClose,
                    int spaceReclaimMode, boolean syncOnCommitDisabled, long sizeLimit,
                    boolean checksum, boolean compress, byte[] password, boolean disableLocks, int sizeIncrement) {
        super(volFac, readOnly, deleteFilesAfterClose, spaceReclaimMode, syncOnCommitDisabled, sizeLimit,
                checksum, compress, password,disableLocks, sizeIncrement);
        this.volFac = volFac;
        this.log = volFac.createTransLogVolume();

        boolean allGood = false;
        if(!disableLocks) {
            structuralLock.lock();
        }
        try{
            reloadIndexFile();
            if(verifyLogFile()){
                replayLogFile();
            }
            replayPending = false;
            checkHeaders();
            if(!readOnly)
                logReset();
            allGood = true;
        }finally{
            if(!allGood) {
                //exception was thrown, try to unlock files
                if (log!=null) {
                    log.close();
                    log = null;
                }
                if (index!=null) {
                    index.close();
                    index = null;
                }
                if (phys!=null) {
                    phys.close();
                    phys = null;
                }
            }

            if(!disableLocks) {
                structuralLock.unlock();
            }
        }
    }

    @Override
    protected void checkHeaders() {
        if(replayPending) return;
        super.checkHeaders();
    }

    protected void reloadIndexFile() {
        assert(disableLocks || structuralLock.isHeldByCurrentThread());
        logSize = 16;
        modified.clear();
        longStackPages.clear();
        indexSize = index.getLong(IO_INDEX_SIZE);
        physSize = index.getLong(IO_PHYS_SIZE);
        freeSize = index.getLong(IO_FREE_SIZE);
        for(int i = 0;i<IO_USER_START;i+=8){
            indexVals[i/8] = index.getLong(i);
        }
        Arrays.fill(indexValsModified, false);

        logChecksum.set(0);

        maxUsedIoList=IO_USER_START-8;
        while(indexVals[((int) (maxUsedIoList / 8))]!=0 && maxUsedIoList>IO_FREE_RECID)
            maxUsedIoList-=8;
    }

    protected  void logReset() {
        assert(disableLocks || structuralLock.isHeldByCurrentThread());
        log.truncate(16);
        log.ensureAvailable(16);
        log.putInt(0, HEADER);
        log.putUnsignedShort(4, STORE_VERSION);
        log.putUnsignedShort(6, expectedMasks());
        log.putLong(8, 0L);
        logSize = 16;
    }


    @Override
    public  long preallocate() {
        final long ioRecid;
        final long logPos;

        if(!disableLocks) {
            newRecidLock.readLock().lock();
        }
        try{
            if(!disableLocks) {
                structuralLock.lock();
            }
            try{
                ioRecid = freeIoRecidTake(false);
                logPos = logSize;
                //now get space in log
                logSize+=1+8+8; //space used for index val
                log.ensureAvailable(logSize);

            }finally{
                if(!disableLocks) {
                    structuralLock.unlock();
                }
            }
            final Lock lock;
            if(disableLocks) {
                lock = null;
            }else {
                lock = locks[Store.lockPos(ioRecid)].writeLock();
                lock.lock();
            }
            try{

                //write data into log
                walIndexVal(logPos, ioRecid, MASK_DISCARD);
                modified.put(ioRecid, PREALLOC);
            }finally{
                if(!disableLocks) {
                    lock.unlock();
                }
            }
        }finally{
            if(!disableLocks) {
                newRecidLock.readLock().unlock();
            }
        }

        long recid =  (ioRecid-IO_USER_START)/8;
        assert(recid>0);
        return recid;
    }


    @Override
    public void preallocate(final long[] recids) {
        long logPos;
        if(!disableLocks) {
            newRecidLock.readLock().lock();
        }
        try{

            if(!disableLocks) {
                structuralLock.lock();
            }
            try{
                logPos = logSize;
                for(int i=0;i<recids.length;i++)
                    recids[i] = freeIoRecidTake(false) ;

                //now get space in log
                logSize+=recids.length*(1+8+8); //space used for index vals
                log.ensureAvailable(logSize);

            }finally{
                if(!disableLocks) {
                    structuralLock.unlock();
                }
            }
            //write data into log
            for(int i=0;i<recids.length;i++){
                final long ioRecid = recids[i];
                final Lock lock2;
                if(disableLocks) {
                    lock2 = null;
                }else {
                    lock2 = locks[Store.lockPos(ioRecid)].writeLock();
                    lock2.lock();
                }
                try{
                    walIndexVal(logPos, ioRecid, MASK_DISCARD);
                    logPos+=1+8+8;
                    modified.put(ioRecid, PREALLOC);
                }finally{
                    if(!disableLocks) {
                        lock2.unlock();
                    }
                }
                recids[i] =  (ioRecid-IO_USER_START)/8;
                assert(recids[i]>0);
            }
        }finally{
            if(!disableLocks) {
                newRecidLock.readLock().unlock();
            }
        }
    }


    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        assert(value!=null);
        DataOutput2 out = serialize(value, serializer);

        final long ioRecid;
        final long[] physPos;
        final long[] logPos;

        if(!disableLocks) {
            newRecidLock.readLock().lock();
        }
        try{
            if(!disableLocks) {
                structuralLock.lock();
            }
        try{
            ioRecid = freeIoRecidTake(false);
            //first get space in phys
            physPos = physAllocate(out.pos,false,false);
            //now get space in log
            logPos = logAllocate(physPos);

        }finally{
            if(!disableLocks) {
                structuralLock.unlock();
            }
        }

        final Lock lock;
        if(disableLocks) {
            lock = null;
        }else {
            lock = locks[Store.lockPos(ioRecid)].writeLock();
            lock.lock();
        }
        try{
            //write data into log
            walIndexVal((logPos[0]&LOG_MASK_OFFSET) - 1-8-8-1-8, ioRecid, physPos[0]|MASK_ARCHIVE);
            walPhysArray(out, physPos, logPos);

            modified.put(ioRecid,logPos);
            recycledDataOuts.offer(out);
        }finally{
            if(!disableLocks) {
                lock.unlock();
            }
        }
        }finally{
            if(!disableLocks) {
                newRecidLock.readLock().unlock();
            }
        }

        long recid =  (ioRecid-IO_USER_START)/8;
        assert(recid>0);
        return recid;
    }

    protected void walPhysArray(DataOutput2 out, long[] physPos, long[] logPos) {
        //write byte[] data
        int outPos = 0;
        int logC = 0;
        CRC32 crc32  = new CRC32();

        for(int i=0;i<logPos.length;i++){
            int c =  i==logPos.length-1 ? 0: 8;
            final long pos = logPos[i]&LOG_MASK_OFFSET;
            int size = (int) (logPos[i]>>>48);

            byte header = c==0 ? WAL_PHYS_ARRAY : WAL_PHYS_ARRAY_ONE_LONG;
            log.putByte(pos -  8 - 1, header);
            log.putLong(pos -  8, physPos[i]);

            if(c>0){
                log.putLong(pos, physPos[i + 1]);
            }
            log.putData(pos+c, out.buf, outPos, size - c);

            crc32.reset();
            crc32.update(out.buf,outPos, size-c);
            logC |= LongHashMap.longHash( pos | header | physPos[i] | (c>0?physPos[i+1]:0) | crc32.getValue());

            outPos +=size-c;
            assert(logSize>=outPos);
        }
        logChecksumAdd(logC);
        assert(outPos==out.pos);
    }


    protected void walIndexVal(long logPos, long ioRecid, long indexVal) {
        assert(disableLocks || locks[Store.lockPos(ioRecid)].writeLock().isHeldByCurrentThread());
        assert(logSize>=logPos+1+8+8);
        log.putByte(logPos, WAL_INDEX_LONG);
        log.putLong(logPos + 1, ioRecid);
        log.putLong(logPos + 9, indexVal);

        logChecksumAdd(LongHashMap.longHash(logPos | WAL_INDEX_LONG | ioRecid | indexVal));
    }


    protected long[] logAllocate(long[] physPos) {
        assert(disableLocks || structuralLock.isHeldByCurrentThread());
        logSize+=1+8+8; //space used for index val

        long[] ret = new long[physPos.length];
        for(int i=0;i<physPos.length;i++){
            long size = physPos[i]>>>48;
            //would overlaps Volume Block?
            logSize+=1+8; //space used for WAL_PHYS_ARRAY
            ret[i] = (size<<48) | logSize;

            logSize+=size;
            checkLogRounding();
        }
        log.ensureAvailable(logSize);
        return ret;
    }

    protected void checkLogRounding() {
        assert(disableLocks || structuralLock.isHeldByCurrentThread());
        if((logSize&CHUNK_SIZE_MOD_MASK)+MAX_REC_SIZE*2>CHUNK_SIZE){
            log.ensureAvailable(logSize+1);
            log.putByte(logSize, WAL_SKIP_REST_OF_BLOCK);
            logSize += CHUNK_SIZE - (logSize&CHUNK_SIZE_MOD_MASK);
        }
    }


    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        assert(recid>0);
        final long ioRecid = IO_USER_START + recid*8;
        final Lock lock;
        if(disableLocks) {
            lock = null;
        }else {
            lock = locks[Store.lockPos(ioRecid)].readLock();
            lock.lock();
        }
        try{
            return get2(ioRecid, serializer);
        }catch(IOException e){
            throw new IOError(e);
        }finally{
            if(!disableLocks) {
                lock.unlock();
            }
        }
    }

    @Override
    protected <A> A get2(long ioRecid, Serializer<A> serializer) throws IOException {
        assert(disableLocks || locks[Store.lockPos(ioRecid)].getWriteHoldCount()==0||
                locks[Store.lockPos(ioRecid)].writeLock().isHeldByCurrentThread());

        //check if record was modified in current transaction
        long[] r = modified.get(ioRecid);
        //no, read main version
        if(r==null) return super.get2(ioRecid, serializer);
        //check for tombstone (was deleted in current trans)
        if(r==TOMBSTONE || r==PREALLOC || r.length==0) return null;

        //was modified in current transaction, so read it from trans log
        if(r.length==1){
            //single record
            final int size = (int) (r[0]>>>48);
            DataInput2 in = (DataInput2) log.getDataInput(r[0]&LOG_MASK_OFFSET, size);
            return deserialize(serializer,size,in);
        }else{
            //linked record
            int totalSize = 0;
            for(int i=0;i<r.length;i++){
                int c =  i==r.length-1 ? 0: 8;
                totalSize+=  (int) (r[i]>>>48)-c;
            }
            byte[] b = new byte[totalSize];
            int pos = 0;
            for(int i=0;i<r.length;i++){
                int c =  i==r.length-1 ? 0: 8;
                int size = (int) (r[i]>>>48) -c;
                log.getDataInput((r[i] & LOG_MASK_OFFSET) + c, size).readFully(b,pos,size);
                pos+=size;
            }
            if(pos!=totalSize)throw new AssertionError();

            return deserialize(serializer,totalSize, new DataInput2(b));
        }
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        assert(recid>0);
        assert(value!=null);
        DataOutput2 out = serialize(value, serializer);
        final long ioRecid = IO_USER_START + recid*8;
        final Lock lock;
        if(disableLocks) {
            lock = null;
        }else {
            lock = locks[Store.lockPos(ioRecid)].writeLock();
            lock.lock();
        }
        try{
            final long[] physPos;
            final long[] logPos;

            long indexVal = 0;
            long[] linkedRecords = getLinkedRecordsFromLog(ioRecid);
            if(linkedRecords==null){
                indexVal = index.getLong(ioRecid);
                linkedRecords = getLinkedRecordsIndexVals(indexVal);
            }else if(linkedRecords == PREALLOC){
                linkedRecords = null;
            }

            if(!disableLocks) {
                structuralLock.lock();
            }
            try{

                //free first record pointed from indexVal
                if((indexVal>>>48)>0)
                    freePhysPut(indexVal,false);

                //if there are more linked records, free those as well
                if(linkedRecords!=null){
                    for(int i=0; i<linkedRecords.length &&linkedRecords[i]!=0;i++){
                        freePhysPut(linkedRecords[i],false);
                    }
                }


                //first get space in phys
                physPos = physAllocate(out.pos,false,false);
                //now get space in log
                logPos = logAllocate(physPos);

            }finally{
                if(!disableLocks) {
                    structuralLock.unlock();
                }
            }

            //write data into log
            walIndexVal((logPos[0]&LOG_MASK_OFFSET) - 1-8-8-1-8, ioRecid, physPos[0]|MASK_ARCHIVE);
            walPhysArray(out, physPos, logPos);

            modified.put(ioRecid,logPos);
        }finally{
            if(!disableLocks) {
                lock.unlock();
            }
        }
        recycledDataOuts.offer(out);
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        assert(recid>0);
        assert(expectedOldValue!=null && newValue!=null);
        final long ioRecid = IO_USER_START + recid*8;
        final Lock lock;
        if(disableLocks) {
            lock = null;
        }else {
            lock = locks[Store.lockPos(ioRecid)].writeLock();
            lock.lock();
        }
        DataOutput2 out;
        try{

            A oldVal = get2(ioRecid,serializer);
            if((oldVal == null && expectedOldValue!=null) || (oldVal!=null && !oldVal.equals(expectedOldValue)))
                return false;

            out = serialize(newValue, serializer);

            final long[] physPos;
            final long[] logPos;

            long indexVal = 0;
            long[] linkedRecords = getLinkedRecordsFromLog(ioRecid);
            if(linkedRecords==null){
                indexVal = index.getLong(ioRecid);
                linkedRecords = getLinkedRecordsIndexVals(indexVal);
            }

            if(!disableLocks) {
                structuralLock.lock();
            }
            try{

                //free first record pointed from indexVal
                if((indexVal>>>48)>0)
                    freePhysPut(indexVal,false);

                //if there are more linked records, free those as well
                if(linkedRecords!=null){
                    for(int i=0; i<linkedRecords.length &&linkedRecords[i]!=0;i++){
                        freePhysPut(linkedRecords[i],false);
                    }
                }


                //first get space in phys
                physPos = physAllocate(out.pos,false,false);
                //now get space in log
                logPos = logAllocate(physPos);

            }finally{
                if(!disableLocks) {
                    structuralLock.unlock();
                }
            }

            //write data into log
            walIndexVal((logPos[0]&LOG_MASK_OFFSET) - 1-8-8-1-8, ioRecid, physPos[0]|MASK_ARCHIVE);
            walPhysArray(out, physPos, logPos);

            modified.put(ioRecid, logPos);

        }catch(IOException e){
            throw new IOError(e);
        }finally{
            if(!disableLocks) {
                lock.unlock();
            }
        }
        recycledDataOuts.offer(out);
        return true;
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        assert(recid>0);
        final long ioRecid = IO_USER_START + recid*8;
        final Lock lock;
        if(disableLocks) {
            lock = null;
        }else {
            lock = locks[Store.lockPos(ioRecid)].writeLock();
            lock.lock();
        }
        try{
            final long logPos;

            long indexVal = 0;
            long[] linkedRecords = getLinkedRecordsFromLog(ioRecid);
            if(linkedRecords==null){
                indexVal = index.getLong(ioRecid);
                if(indexVal==MASK_DISCARD) return;
                linkedRecords = getLinkedRecordsIndexVals(indexVal);
            }
            if(!disableLocks) {
                structuralLock.lock();
            }
            try{
                logPos = logSize;
                checkLogRounding();
                logSize+=1+8+8; //space used for index val
                log.ensureAvailable(logSize);
                longStackPut(IO_FREE_RECID, ioRecid,false);

                //free first record pointed from indexVal
                if((indexVal>>>48)>0)
                    freePhysPut(indexVal,false);

                //if there are more linked records, free those as well
                if(linkedRecords!=null){
                    for(int i=0; i<linkedRecords.length &&linkedRecords[i]!=0;i++){
                        freePhysPut(linkedRecords[i],false);
                    }
                }

            }finally {
                if(!disableLocks) {
                    structuralLock.unlock();
                }
            }
            walIndexVal(logPos,ioRecid,0|MASK_ARCHIVE);
            modified.put(ioRecid, TOMBSTONE);
        }finally {
            if(!disableLocks) {
                lock.unlock();
            }
        }
    }

    @Override
    public void commit() {
        lockAllWrite();
        try{
            if(serializerPojo!=null && serializerPojo.hasUnsavedChanges()){
                serializerPojo.save(this);
            }

            if(!logDirty()){
                return;
            }

            //dump long stack pages
            int crc = 0;
            LongMap.LongMapIterator<byte[]> iter = longStackPages.longMapIterator();
            while(iter.moveToNext()){
                assert(iter.key()>>>48==0);
                final byte[] array = iter.value();
                final long pageSize = ((array[0]&0xFF)<<8)|(array[1]&0xFF) ;
                assert(array.length==pageSize);
                final long firstVal = (pageSize<<48)|iter.key();
                log.ensureAvailable(logSize+1+8+pageSize);

                crc |= LongHashMap.longHash(logSize|WAL_LONGSTACK_PAGE|firstVal);

                log.putByte(logSize, WAL_LONGSTACK_PAGE);
                logSize+=1;
                log.putLong(logSize, firstVal);
                logSize+=8;

                //put array
                CRC32 crc32  = new CRC32();
                crc32.update(array);
                crc |= crc32.getValue();
                log.putData(logSize,array,0,array.length);
                logSize+=array.length;

                checkLogRounding();
            }


            for(int i=IO_FREE_RECID;i<IO_USER_START;i+=8){
                if(!indexValsModified[i/8]) continue;
                log.ensureAvailable(logSize + 17);
                logSize+=17;
                walIndexVal(logSize-17, i,indexVals[i/8]);
                //no need to update crc, since IndexVal already does it
            }

            //seal log file
            log.ensureAvailable(logSize + 1 + 3*6 + 8+4);
            long indexChecksum = indexHeaderChecksumUncommited();
            crc|=LongHashMap.longHash(logSize|WAL_SEAL|indexSize|physSize|freeSize|indexChecksum);
            log.putByte(logSize, WAL_SEAL);
            logSize+=1;
            log.putSixLong(logSize, indexSize);
            logSize+=6;
            log.putSixLong(logSize,physSize);
            logSize+=6;
            log.putSixLong(logSize,freeSize);
            logSize+=6;
            log.putLong(logSize, indexChecksum);
            logSize+=8;
            log.putInt(logSize, crc|logChecksum.get());
            logSize+=4;

            //write mark it was sealed
            log.putLong(8, LOG_SEAL);

            //and flush log file
            if(!syncOnCommitDisabled) log.sync();

            replayLogFile();
            reloadIndexFile();
        }finally {
             unlockAllWrite();
        }

    }

    protected boolean logDirty() {

        if(logSize!=16 || !longStackPages.isEmpty() || !modified.isEmpty())
            return true;

        for(boolean b: indexValsModified){
            if(b)
                return true;
        }

        return false;
    }

    protected long indexHeaderChecksumUncommited() {
        long ret = 0;

        for(int offset = 0;offset<IO_USER_START;offset+=8){
            if(offset == IO_INDEX_SUM) continue;
            long indexVal;

            if(offset==IO_INDEX_SIZE){
                indexVal = indexSize;
            }else if(offset==IO_PHYS_SIZE){
                indexVal = physSize;
            }else if(offset==IO_FREE_SIZE){
                indexVal = freeSize;
            }else
                indexVal = indexVals[offset / 8];

            ret |=  indexVal | LongHashMap.longHash(indexVal|offset) ;
        }

        return ret;
    }

    protected boolean verifyLogFile() {
        assert(disableLocks || structuralLock.isHeldByCurrentThread());

        if(readOnly && log==null)
            return false;

        logSize = 0;



        //read headers
        if(log.isEmpty() || log.getInt(0)!=HEADER  || log.getLong(8) !=LOG_SEAL){
            //wrong headers, discard log
            if(!isReadOnly())
                logReset();
            return false;
        }

        if(log.getUnsignedShort(4)>STORE_VERSION){
            throw new IOError(new IOException("New store format version, please use newer MapDB version"));
        }

        if(log.getUnsignedShort(6)!=expectedMasks())
            throw new IllegalArgumentException("Log file created with different features. Please check compression, checksum or encryption");



        final CRC32 crc32 = new CRC32();

        //all good, calculate checksum
        logSize=16;
        byte ins = log.getByte(logSize);
        logSize+=1;
        int crc = 0;

        while(ins!=WAL_SEAL) try{
            if(ins == WAL_INDEX_LONG){
                long ioRecid = log.getLong(logSize);
                logSize+=8;
                long indexVal = log.getLong(logSize);
                logSize+=8;
                crc |= LongHashMap.longHash((logSize-1-8-8) | WAL_INDEX_LONG | ioRecid | indexVal);
            }else if(ins == WAL_PHYS_ARRAY){
                final long offset2 = log.getLong(logSize);
                logSize+=8;
                final int size = (int) (offset2>>>48);

                byte[] b = new byte[size];
                try{
                    log.getDataInput(logSize, size).readFully(b);
                } catch (IOException e) {
                    throw new IOError(e);
                }
                crc32.reset();
                crc32.update(b);

                crc |= LongHashMap.longHash(logSize | WAL_PHYS_ARRAY | offset2 | crc32.getValue());

                logSize+=size;
            }else if(ins == WAL_PHYS_ARRAY_ONE_LONG){
                final long offset2 = log.getLong(logSize);
                logSize+=8;
                final int size = (int) (offset2>>>48)-8;

                final long nextPageLink = log.getLong(logSize);
                logSize+=8;

                byte[] b = new byte[size];
                log.getDataInput(logSize, size).readFully(b);
                crc32.reset();
                crc32.update(b);

                crc |= LongHashMap.longHash((logSize) | WAL_PHYS_ARRAY_ONE_LONG | offset2 | nextPageLink | crc32.getValue());

                logSize+=size;
            }else if(ins == WAL_LONGSTACK_PAGE){
                final long offset = log.getLong(logSize);
                logSize+=8;
                final long origLogSize = logSize;
                final int size = (int) (offset>>>48);

                crc |= LongHashMap.longHash(origLogSize | WAL_LONGSTACK_PAGE | offset );

                byte[] b = new byte[size];
                log.getDataInput(logSize, size).readFully(b);
                crc32.reset();
                crc32.update(b);
                crc|=crc32.getValue();

                log.getDataInput(logSize, size).readFully(b);
            }else if(ins == WAL_SKIP_REST_OF_BLOCK){
                logSize += CHUNK_SIZE -(logSize&CHUNK_SIZE_MOD_MASK);
            }else{
                throw new AssertionError("unknown trans log instruction '"+ins +"' at log offset: "+(logSize-1));
            }

            ins = log.getByte(logSize);
            logSize+=1;
        } catch (IOException e) {
            throw new IOError(e);
        }

        long indexSize = log.getSixLong(logSize);
        logSize+=6;
        long physSize = log.getSixLong(logSize);
        logSize+=6;
        long freeSize = log.getSixLong(logSize);
        logSize+=6;
        long indexSum = log.getLong(logSize);
        logSize+=8;
        crc |= LongHashMap.longHash((logSize-1-3*6-8)|indexSize|physSize|freeSize|indexSum);

        final int realCrc = log.getInt(logSize);
        logSize+=4;

        logSize=0;
        assert(disableLocks || structuralLock.isHeldByCurrentThread());
        if(realCrc == Long.MIN_VALUE)
            return true; //in future WAL CRC might be switched off, in that case this value will be used

        return realCrc == crc ;
    }



    protected void replayLogFile(){
        assert(disableLocks || structuralLock.isHeldByCurrentThread());

        if(readOnly && log==null)
            return; //TODO how to handle log replay if we are readonly?

        logSize = 0;


        //read headers
        if(log.isEmpty() || log.getInt(0)!=HEADER ||
                log.getUnsignedShort(4)>STORE_VERSION || log.getLong(8) !=LOG_SEAL ||
                log.getUnsignedShort(6)!=expectedMasks()){
            //wrong headers, discard log
            logReset();
            return;
        }


        //all good, start replay
        logSize=16;
        byte ins = log.getByte(logSize);
        logSize+=1;

        while(ins!=WAL_SEAL){
            if(ins == WAL_INDEX_LONG){
                long ioRecid = log.getLong(logSize);
                logSize+=8;
                long indexVal = log.getLong(logSize);
                logSize+=8;
                index.ensureAvailable(ioRecid+8);
                index.putLong(ioRecid, indexVal);
            }else if(ins == WAL_PHYS_ARRAY||ins == WAL_LONGSTACK_PAGE || ins == WAL_PHYS_ARRAY_ONE_LONG){
                long offset = log.getLong(logSize);
                logSize+=8;
                final int size = (int) (offset>>>48);
                offset = offset&MASK_OFFSET;

                //transfer byte[] directly from log file without copying into memory
                DataInput2 input = (DataInput2) log.getDataInput(logSize, size);
                ByteBuffer buf = input.buf.duplicate();

                buf.position(input.pos);
                buf.limit(input.pos+size);
                phys.ensureAvailable(offset+size);
                phys.putData(offset, buf);

                logSize+=size;
            }else if(ins == WAL_SKIP_REST_OF_BLOCK){
                logSize += CHUNK_SIZE -(logSize&CHUNK_SIZE_MOD_MASK);
            }else{
                throw new AssertionError("unknown trans log instruction '"+ins +"' at log offset: "+(logSize-1));
            }

            ins = log.getByte(logSize);
            logSize+=1;
        }
        index.putLong(IO_INDEX_SIZE,log.getSixLong(logSize));
        logSize+=6;
        index.putLong(IO_PHYS_SIZE,log.getSixLong(logSize));
        logSize+=6;
        index.putLong(IO_FREE_SIZE,log.getSixLong(logSize));
        logSize+=6;
        index.putLong(IO_INDEX_SUM,log.getLong(logSize));
        logSize+=8;



        //flush dbs
        if(!syncOnCommitDisabled){
            phys.sync();
            index.sync();
        }

        logReset();

        assert(disableLocks || structuralLock.isHeldByCurrentThread());
    }



    @Override
    public void rollback() throws UnsupportedOperationException {
        lockAllWrite();
        try{
            //discard trans log
            logReset();

            reloadIndexFile();
        }finally {
            unlockAllWrite();
        }
    }

    protected long[] getLinkedRecordsFromLog(long ioRecid){
        assert(disableLocks || locks[Store.lockPos(ioRecid)].writeLock().isHeldByCurrentThread());
        long[] ret0 = modified.get(ioRecid);
        if(ret0==PREALLOC) return ret0;

        if(ret0!=null && ret0!=TOMBSTONE){
            long[] ret = new long[ret0.length];
            for(int i=0;i<ret0.length;i++){
                long offset = ret0[i] & LOG_MASK_OFFSET;
                //offset now points to log file, read phys offset from log file
                ret[i] =  log.getLong(offset-8);
            }
            return ret;
        }
        return null;
    }

    @Override
    protected long longStackTake(long ioList, boolean recursive) {
        assert(disableLocks || structuralLock.isHeldByCurrentThread());
        assert(ioList>=IO_FREE_RECID && ioList<IO_USER_START) :"wrong ioList: "+ioList;


        long dataOffset = indexVals[((int) ioList/8)];
        if(dataOffset == 0)
            return 0; //there is no such list, so just return 0

        long pos = dataOffset>>>48;
        dataOffset &= MASK_OFFSET;
        byte[] page = longStackGetPage(dataOffset);

        if(pos<8) throw new AssertionError();

        final long ret = longStackGetSixLong(page, (int) pos);

        //was it only record at that page?
        if(pos == 8){
            //yes, delete this page
            long next = longStackGetSixLong(page,2);
            long size = ((page[0]&0xFF)<<8) | (page[1]&0xFF);
            assert(size == page.length);
            if(next !=0){
                //update index so it points to previous page
                byte[] nextPage = longStackGetPage(next); //TODO this page is not modifed, but is added to LOG
                long nextSize = ((nextPage[0]&0xFF)<<8) | (nextPage[1]&0xFF);
                assert((nextSize-8)%6==0);
                indexVals[((int) ioList/8)]=((nextSize-6)<<48)|next;
                indexValsModified[((int) ioList/8)]=true;
            }else{
                //zero out index
                indexVals[((int) ioList/8)]=0L;
                indexValsModified[((int) ioList/8)]=true;
                if(maxUsedIoList==ioList){
                    //max value was just deleted, so find new maxima
                    while(indexVals[((int) maxUsedIoList/8)]==0 && maxUsedIoList>IO_FREE_RECID){
                        maxUsedIoList-=8;
                    }
                }
            }
            //put space used by this page into free list
            freePhysPut((size<<48) | dataOffset, true);
            assert(dataOffset>>>48==0);
            longStackPages.remove(dataOffset);
        }else{
            //no, it was not last record at this page, so just decrement the counter
            pos-=6;
            indexVals[((int) ioList/8)] = (pos<<48)| dataOffset;
            indexValsModified[((int) ioList/8)] = true;
        }

        //System.out.println("longStackTake: "+ioList+" - "+ret);

        return ret;

    }

    @Override
    protected void longStackPut(long ioList, long offset, boolean recursive) {
        assert(disableLocks || structuralLock.isHeldByCurrentThread());
        assert(offset>>>48==0);
        assert(ioList>=IO_FREE_RECID && ioList<=IO_USER_START): "wrong ioList: "+ioList;

        long dataOffset = indexVals[((int) ioList/8)];
        long pos = dataOffset>>>48;
        dataOffset &= MASK_OFFSET;

        if(dataOffset == 0){ //empty list?
            //yes empty, create new page and fill it with values
            final long listPhysid = freePhysTake((int) LONG_STACK_PREF_SIZE,true,true) &MASK_OFFSET;
            if(listPhysid == 0) throw new AssertionError();
            assert(listPhysid>>>48==0);
            //set previous Free Index List page to zero as this is first page
            //also set size of this record
            byte[] page = new byte[(int) LONG_STACK_PREF_SIZE];
            page[0] = (byte) (0xFF & (page.length>>>8));
            page[1] = (byte) (0xFF & (page.length));
            longStackPutSixLong(page,2,0L);
            //set  record
            longStackPutSixLong(page, 8, offset);
            //and update index file with new page location
            indexVals[((int) ioList/8)] = ( 8L << 48) | listPhysid;
            indexValsModified[((int) ioList/8)] = true;
            if(maxUsedIoList<=ioList) maxUsedIoList=ioList;
            longStackPages.put(listPhysid,page);
        }else{
            byte[] page = longStackGetPage(dataOffset);
            long size = ((page[0]&0xFF)<<8)|(page[1]&0xFF);

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

                byte[] newPage = new byte[(int) newPageSize];

                //set current page size
                newPage[0] = (byte) (0xFF & (newPageSize>>>8));
                newPage[1] = (byte) (0xFF & (newPageSize));
                //set location to previous page and
                longStackPutSixLong(newPage,2,dataOffset&MASK_OFFSET);


                //set the value itself
                longStackPutSixLong(newPage, 8, offset);
                assert(listPhysid>>>48==0);
                longStackPages.put(listPhysid,newPage);

                //and update index file with new page location and number of records
                indexVals[((int) ioList/8)] = (8L<<48) | listPhysid;
                indexValsModified[((int) ioList/8)] = true;
            }else{
                //there is space on page, so just write offset and increase the counter
                pos+=6;
                longStackPutSixLong(page, (int) pos,offset);
                indexVals[((int) ioList/8)] = (pos<<48)| dataOffset;
                indexValsModified[((int) ioList/8)] = true;
            }
        }
    }

    protected static long longStackGetSixLong(byte[] page, int pos) {
        return
                ((long) (page[pos + 0] & 0xff) << 40) |
                        ((long) (page[pos + 1] & 0xff) << 32) |
                        ((long) (page[pos + 2] & 0xff) << 24) |
                        ((long) (page[pos + 3] & 0xff) << 16) |
                        ((long) (page[pos + 4] & 0xff) << 8) |
                        ((long) (page[pos + 5] & 0xff) << 0);
    }


    protected static void longStackPutSixLong(byte[] page, int pos, long value) {
        assert(value>=0 && (value>>>6*8)==0): "value does not fit";
        page[pos + 0] = (byte) (0xff & (value >> 40));
        page[pos + 1] = (byte) (0xff & (value >> 32));
        page[pos + 2] = (byte) (0xff & (value >> 24));
        page[pos + 3] = (byte) (0xff & (value >> 16));
        page[pos + 4] = (byte) (0xff & (value >> 8));
        page[pos + 5] = (byte) (0xff & (value >> 0));

    }


    protected byte[] longStackGetPage(long offset) {
        assert(offset>=16);
        assert(offset>>>48==0);

        byte[] ret = longStackPages.get(offset);
        if(ret==null){
            //read page size
            int size = phys.getUnsignedShort(offset);
            assert(size>=8+6);
            ret = new byte[size];
            try {
                phys.getDataInput(offset,size).readFully(ret);
            } catch (IOException e) {
                throw new IOError(e);
            }

            //and load page
            longStackPages.put(offset,ret);
        }

        return ret;
    }

    @Override
    public void close() {
        for(Runnable closeListener:closeListeners)
            closeListener.run();

        if(serializerPojo!=null && serializerPojo.hasUnsavedChanges()){
            serializerPojo.save(this);
        }

        lockAllWrite();
        try{
            if(log !=null){
                log.sync();
                log.close();
                if(deleteFilesAfterClose){
                    log.deleteFile();
                }
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
        }finally {
            unlockAllWrite();
        }
    }

    @Override protected void compactPreUnderLock() {
        assert(disableLocks || structuralLock.isLocked());
        if(logDirty())
            throw new IllegalAccessError("WAL not empty; commit first, than compact");
    }

    @Override protected void compactPostUnderLock() {
        assert(disableLocks || structuralLock.isLocked());
        reloadIndexFile();
    }


    @Override
    public boolean canRollback(){
        return true;
    }

    protected void logChecksumAdd(int cs) {
        for(;;){
            int old = logChecksum.get();
            if(logChecksum.compareAndSet(old,old|cs))
                return;
        }
    }



}
