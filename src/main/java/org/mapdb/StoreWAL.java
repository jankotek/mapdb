/*
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


import java.io.DataInput;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import static org.mapdb.DataIO.*;

/**
 * Write-Ahead-Log
 */
public class StoreWAL extends StoreCached {

    /** 2 byte store version*/
    protected static final int WAL_STORE_VERSION = 100;

    /** 4 byte file header */
    protected static final int WAL_HEADER = (0x8A77<<16) | WAL_STORE_VERSION;


    protected static final long WAL_SEAL = 8234892392398238983L;

    protected static final int FULL_REPLAY_AFTER_N_TX = 16;


    /**
     * Contains index table modified in previous transactions.
     *
     * If compaction is in progress, than the value is not index, but following:
     * <pre>
     *  Long.MAX_VALUE == TOMBSTONE
     *  First three bytes is WAL file number
     *  Remaining 5 bytes is offset in WAL file
     * </pre>
     *
     */
    protected final LongLongMap[] prevLongLongs;
    protected final LongLongMap[] currLongLongs;
    protected final LongLongMap[] prevDataLongs;
    protected final LongLongMap[] currDataLongs;

    protected final LongLongMap pageLongStack = new LongLongMap();
    protected final List<Volume> volumes = Collections.synchronizedList(new ArrayList<Volume>());

    /** WAL file sealed after compaction is completed, if no valid seal, compaction file should be destroyed */
    protected volatile Volume walC;

    /** File into which store is compacted. */
    protected volatile Volume walCCompact;

    /** record WALs, store recid-record pairs. Created during compaction when memory allocator is not available */
    protected final List<Volume> walRec = Collections.synchronizedList(new ArrayList<Volume>());

    protected final ReentrantLock compactLock = new ReentrantLock(CC.FAIR_LOCKS);
    /** protected by commitLock */
    protected volatile boolean compactionInProgress = false;

    protected Volume curVol;

    protected int fileNum = -1;

    //TODO how to protect concurrrently file offset when file is being swapped?
    protected final AtomicLong walOffset = new AtomicLong();

    protected Volume headVolBackup;

    protected long[] indexPagesBackup;

    protected Volume realVol;

    protected volatile boolean $_TEST_HACK_COMPACT_PRE_COMMIT_WAIT =false;

    protected volatile boolean $_TEST_HACK_COMPACT_POST_COMMIT_WAIT =false;


    public StoreWAL(String fileName) {
        this(fileName,
                fileName == null ? CC.DEFAULT_MEMORY_VOLUME_FACTORY : CC.DEFAULT_FILE_VOLUME_FACTORY,
                null,
                CC.DEFAULT_LOCK_SCALE,
                0,
                false, false, null, false,false, 0,
                false, 0,
                null, 0L,
                0);
    }

    public StoreWAL(
            String fileName,
            Volume.VolumeFactory volumeFactory,
            Cache cache,
            int lockScale,
            int lockingStrategy,
            boolean checksum,
            boolean compress,
            byte[] password,
            boolean readonly,
            boolean snapshotEnable,
            int freeSpaceReclaimQ,
            boolean commitFileSyncDisable,
            int sizeIncrement,
            ScheduledExecutorService executor,
            long executorScheduledRate,
            int writeQueueSize
        ) {
        super(fileName, volumeFactory, cache,
                lockScale,
                lockingStrategy,
                checksum, compress, password, readonly, snapshotEnable,
                freeSpaceReclaimQ, commitFileSyncDisable, sizeIncrement,
                executor,
                executorScheduledRate,
                writeQueueSize);
        prevLongLongs = new LongLongMap[this.lockScale];
        currLongLongs = new LongLongMap[this.lockScale];
        for (int i = 0; i < prevLongLongs.length; i++) {
            prevLongLongs[i] = new LongLongMap();
            currLongLongs[i] = new LongLongMap();
        }
        prevDataLongs = new LongLongMap[this.lockScale];
        currDataLongs = new LongLongMap[this.lockScale];
        for (int i = 0; i < prevDataLongs.length; i++) {
            prevDataLongs[i] = new LongLongMap();
            currDataLongs[i] = new LongLongMap();
        }

    }


    @Override
    protected void initCreate() {
        super.initCreate();
        indexPagesBackup = indexPages.clone();
        realVol = vol;
        //make main vol readonly, to make sure it is never overwritten outside WAL replay
        vol = new Volume.ReadOnly(vol);

        //start new WAL file
        walStartNextFile();
    }

    @Override
    public void initOpen(){
        //TODO disable readonly feature for this store

        realVol = vol;

        //replay WAL files
        String wal0Name = getWalFileName("0");
        String walCompSeal = getWalFileName("c");
        boolean walCompSealExists =
                walCompSeal!=null &&
                        new File(walCompSeal).exists();

        if(walCompSealExists ||
             (wal0Name!=null &&
                     new File(wal0Name).exists())){
            //fill compaction stuff

            walC =  walCompSealExists?volumeFactory.makeVolume(walCompSeal, readonly) : null;
            walCCompact = walCompSealExists? volumeFactory.makeVolume(walCompSeal + ".compact", readonly) : null;

            for(int i=0;;i++){
                String rname = getWalFileName("r"+i);
                if(!new File(rname).exists())
                    break;
                walRec.add(volumeFactory.makeVolume(rname, readonly));
            }


            //fill wal files
            for(int i=0;;i++){
                String wname = getWalFileName(""+i);
                if(!new File(wname).exists())
                    break;
                volumes.add(volumeFactory.makeVolume(wname, readonly));
            }

            initOpenPost();

            replayWAL();

            if(walC!=null)
                walC.close();
            walC = null;
            if(walCCompact!=null)
                walCCompact.close();
            walCCompact = null;
            for(Volume v:walRec){
                v.close();
            }
            walRec.clear();
            volumes.clear();
        }

        //start new WAL file
        //TODO do not start if readonly
        walStartNextFile();

        initOpenPost();
    }

    @Override
    protected void initFailedCloseFiles() {
        if(walC!=null && !walC.isClosed()) {
            walC.close();
        }
        walC = null;

        if(walCCompact!=null && !walCCompact.isClosed()) {
            walCCompact.close();
        }
        walCCompact = null;

        if(walRec!=null){
            for(Volume v:walRec){
                if(v!=null && !v.isClosed())
                    v.close();
            }
            walRec.clear();
        }
        if(volumes!=null){
            for(Volume v:volumes){
                if(v!=null && !v.isClosed())
                    v.close();
            }
            volumes.clear();
        }
    }

    protected void initOpenPost() {
        super.initOpen();
        indexPagesBackup = indexPages.clone();

        //make main vol readonly, to make sure it is never overwritten outside WAL replay
        //all data are written to realVol
        vol = new Volume.ReadOnly(vol);
    }


    @Override
    protected void initHeadVol() {
        super.initHeadVol();
        //backup headVol
        if(headVolBackup!=null && !headVolBackup.isClosed())
            headVolBackup.close();
        headVolBackup = new Volume.ByteArrayVol(CC.VOLUME_PAGE_SHIFT);
        headVolBackup.ensureAvailable(HEAD_END);
        byte[] b = new byte[(int) HEAD_END];
        //TODO use direct copy
        headVol.getData(0, b, 0, b.length);
        headVolBackup.putData(0,b,0,b.length);
    }

    protected void walStartNextFile() {
        if (CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        fileNum++;
        if (CC.ASSERT && fileNum != volumes.size())
            throw new DBException.DataCorruption();
        String filewal = getWalFileName(""+fileNum);
        Volume nextVol;
        if (readonly && filewal != null && !new File(filewal).exists()){
            nextVol = new Volume.ReadOnly(new Volume.ByteArrayVol(8));
        }else {
            nextVol = volumeFactory.makeVolume(filewal, readonly);
        }
        nextVol.ensureAvailable(16);

        if(!readonly) {
            nextVol.putInt(0, WAL_HEADER);
            nextVol.putLong(8, makeFeaturesBitmap());
        }

        walOffset.set(16);
        volumes.add(nextVol);

        curVol = nextVol;
    }

    protected String getWalFileName(String ext) {
        return fileName==null? null :
                fileName+".wal"+"."+ext;
    }

    protected void walPutLong(long offset, long value){
        final int plusSize = +1+8+6;
        long walOffset2 = walOffset.getAndAdd(plusSize);

        Volume curVol2 = curVol;

        //in case of overlap, put Skip Bytes instruction and try again
        if(hadToSkip(walOffset2, plusSize)){
            walPutLong(offset, value);
            return;
        }

        if(CC.ASSERT && offset>>>48!=0)
            throw new DBException.DataCorruption();
        curVol2.ensureAvailable(walOffset2+plusSize);
        int parity = 1+Long.bitCount(value)+Long.bitCount(offset);
        parity &=15;
        curVol2.putUnsignedByte(walOffset2, (1 << 4)|parity);
        walOffset2+=1;
        curVol2.putLong(walOffset2, value);
        walOffset2+=8;
        curVol2.putSixLong(walOffset2, offset);
    }


    protected void walPutUnsignedShort(long offset, int value) {
        final int plusSize = +1+8;
        long walOffset2 = walOffset.getAndAdd(plusSize);

        Volume curVol2 = curVol;

        //in case of overlap, put Skip Bytes instruction and try again
        if(hadToSkip(walOffset2, plusSize)){
            walPutUnsignedShort(offset, value);
            return;
        }

        curVol2.ensureAvailable(walOffset2+plusSize);
        if(CC.ASSERT && offset>>>48!=0)
            throw new DBException.DataCorruption();
        offset = (((long)value)<<48) | offset;
        int parity = 1+Long.bitCount(offset);
        parity &=15;
        curVol2.putUnsignedByte(walOffset2, (6 << 4)|parity);
        walOffset2+=1;
        curVol2.putLong(walOffset2, offset);
    }

    protected boolean hadToSkip(long walOffset2, int plusSize) {
        //does it overlap page boundaries?
        if((walOffset2>>>CC.VOLUME_PAGE_SHIFT)==(walOffset2+plusSize)>>>CC.VOLUME_PAGE_SHIFT){
            return false; //no, does not, all fine
        }

        //is there enough space for 4 byte skip N bytes instruction?
        while((walOffset2&PAGE_MASK) >= PAGE_SIZE-4 || plusSize<5){
            //pad with single byte skip instructions, until end of page is reached
            int singleByteSkip = (4<<4)|(Long.bitCount(walOffset2)&15);
            curVol.putUnsignedByte(walOffset2++, singleByteSkip);
            plusSize--;
            if(CC.ASSERT && plusSize<0)
                throw new DBException.DataCorruption();
        }

        //now new page starts, so add skip instruction for remaining bits
        int val = (3<<(4+3*8)) | (plusSize-4) | ((Integer.bitCount(plusSize-4)&15)<<(3*8));
        curVol.ensureAvailable(walOffset2 + 4);
        curVol.putInt(walOffset2, val);

        return true;
    }

    @Override
    protected void putDataSingleWithLink(int segment, long offset, long link, byte[] buf, int bufPos, int size) {
        if(CC.ASSERT && (size&0xFFFF)!=size)
            throw new DBException.DataCorruption();
        //TODO optimize so array copy is not necessary, that means to clone and modify putDataSingleWithoutLink method
        byte[] buf2 = new  byte[size+8];
        DataIO.putLong(buf2,0,link);
        System.arraycopy(buf,bufPos,buf2,8,size);
        putDataSingleWithoutLink(segment,offset,buf2,0,buf2.length);
    }

    @Override
    protected void putDataSingleWithoutLink(int segment, long offset, byte[] buf, int bufPos, int size) {
        if(CC.ASSERT && (size&0xFFFF)!=size)
            throw new DBException.DataCorruption();
        if(CC.ASSERT && (offset%16!=0 && offset!=4))
            throw new DBException.DataCorruption();
//        if(CC.ASSERT && size%16!=0)
//            throw new AssertionError(); //TODO allign record size to 16, and clear remaining bytes
        if(CC.ASSERT && segment!=-1)
            assertWriteLocked(segment);
        if(CC.ASSERT && segment==-1 && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        final int plusSize = +1+2+6+size;
        long walOffset2 = walOffset.getAndAdd(plusSize);

        if(hadToSkip(walOffset2, plusSize)){
            putDataSingleWithoutLink(segment,offset,buf,bufPos,size);
            return;
        }

        curVol.ensureAvailable(walOffset2+plusSize);
        int checksum = 1+Integer.bitCount(size)+Long.bitCount(offset)+sum(buf,bufPos,size);
        checksum &= 15;
        curVol.putUnsignedByte(walOffset2, (2 << 4)|checksum);
        walOffset2+=1;
        curVol.putLong(walOffset2, ((long) size) << 48 | offset);
        walOffset2+=8;
        curVol.putData(walOffset2, buf,bufPos,size);

        //TODO assertions
        long val = ((long)size)<<48;
        val |= ((long)fileNum)<<32;
        val |= walOffset2;

        (segment==-1?pageLongStack:currDataLongs[segment]).put(offset, val);
    }


    protected DataInput walGetData(long offset, int segment) {
        if (CC.ASSERT && offset % 16 != 0)
            throw new DBException.DataCorruption();

        long longval = currDataLongs[segment].get(offset);
        if(longval==0){
            longval = prevDataLongs[segment].get(offset);
        }
        if(longval==0)
            return null;

        int arraySize = (int) (longval >>> 48);
        int fileNum = (int) ((longval >>> 32) & 0xFFFFL);
        long dataOffset = longval & 0xFFFFFFFFL;

        Volume vol = volumes.get(fileNum);
        return vol.getDataInput(dataOffset, arraySize);
    }

    @Override
    protected long indexValGet(long recid) {
        if(CC.ASSERT)
            assertReadLocked(recid);
        int segment = lockPos(recid);
        long offset = recidToOffset(recid);
        long ret = currLongLongs[segment].get(offset);
        if(ret!=0) {
            return ret;
        }
        ret = prevLongLongs[segment].get(offset);
        if(ret!=0)
            return ret;
        return super.indexValGet(recid);
    }

    @Override
    protected void indexValPut(long recid, int size, long offset, boolean linked, boolean unused) {
        if(CC.ASSERT)
            assertWriteLocked(lockPos(recid));
//        if(CC.ASSERT && compactionInProgress)
//            throw new AssertionError();

        long newVal = composeIndexVal(size, offset, linked, unused, true);
        currLongLongs[lockPos(recid)].put(recidToOffset(recid), newVal);
    }

    @Override
    protected void indexLongPut(long offset, long val) {
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw  new AssertionError();
        if(CC.ASSERT && compactionInProgress)
            throw new AssertionError();
        walPutLong(offset,val);
    }

    @Override
    protected long pageAllocate() {
// TODO compaction assertion
//        if(CC.ASSERT && compactionInProgress)
//            throw new AssertionError();

        long storeSize = parity16Get(headVol.getLong(STORE_SIZE));
        headVol.putLong(STORE_SIZE, parity16Set(storeSize + PAGE_SIZE));
        //TODO clear data on page? perhaps special instruction?

        if(CC.ASSERT && storeSize%PAGE_SIZE!=0)
            throw new DBException.DataCorruption();


        return storeSize;
    }

    @Override
    protected byte[] loadLongStackPage(long pageOffset) {
        if (CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

//        if(CC.ASSERT && compactionInProgress)
//            throw new AssertionError();


        //first try to get it from dirty pages in current TX
        byte[] page = dirtyStackPages.get(pageOffset);
        if (page != null) {
            return page;
        }

        //try to get it from previous TX stored in WAL, but not yet replayed
        long walval = pageLongStack.get(pageOffset);
        if(walval!=0){
            //get file number, offset and size in WAL
            int arraySize = (int) (walval >>> 48);
            int fileNum = (int) ((walval >>> 32) & 0xFFFFL);
            long dataOffset = walval & 0xFFFFFFFFL;
            //read and return data
            byte[] b = new byte[arraySize];
            Volume vol = volumes.get(fileNum);
            vol.getData(dataOffset, b, 0, arraySize);
            //page is going to be modified, so put it back into dirtyStackPages)
            dirtyStackPages.put(pageOffset, b);
            return b;
        }

        //and finally read it from main store
        int pageSize = (int) (parity4Get(vol.getLong(pageOffset)) >>> 48);
        page = new byte[pageSize];
        vol.getData(pageOffset, page, 0, pageSize);
        dirtyStackPages.put(pageOffset, page);
        return page;
    }

    @Override
    protected <A> A get2(long recid, Serializer<A> serializer) {
        if (CC.ASSERT)
            assertReadLocked(recid);
        int segment = lockPos(recid);

        //is in write cache?
        {
            Object cached = writeCache[segment].get1(recid);
            if (cached != null) {
                if(cached==TOMBSTONE2)
                    return null;
                return (A) cached;
            }
        }
        //is in wal?
        {
            long walval = currLongLongs[segment].get(recidToOffset(recid));
            if(walval==0) {
                walval = prevLongLongs[segment].get(recidToOffset(recid));
            }

            if(walval!=0){
                if(compactionInProgress){
                    //read from Record log
                    if(walval==Long.MAX_VALUE) //TOMBSTONE or null
                        return null;
                    final int fileNum = (int) (walval>>>(5*8));
                    Volume recVol = walRec.get(fileNum);
                    long offset = walval&0xFFFFFFFFFFL; //last 5 bytes
                    if(CC.ASSERT){
                        int instruction = recVol.getUnsignedByte(offset);
                        //TODO exception should not be here
                        if(instruction!=(5<<4))
                            throw new DBException.DataCorruption("wrong instruction");
                        if(recid!=recVol.getSixLong(offset+1))
                            throw new DBException.DataCorruption("wrong recid");
                    }

                    //skip instruction and recid
                    offset+=1+6;
                    final int size = recVol.getInt(offset);
                    //TODO instruction checksum
                    final DataInput in = size==0?
                            new DataIO.DataInputByteArray(new byte[0]):
                            recVol.getDataInput(offset+4,size);

                    return deserialize(serializer, size, in);
                }

                //read record from WAL
                boolean linked = (walval&MLINKED)!=0;
                int size = (int) (walval>>>48);
                if(linked && size==0)
                    return null;
                if(size==0){
                    return deserialize(serializer,0,new DataIO.DataInputByteArray(new byte[0]));
                }
                if(linked)try {
                    //read linked record
                    int totalSize = 0;
                    byte[] in = new byte[100];
                    long link = walval;
                    while((link&MLINKED)!=0){
                        DataInput in2 = walGetData(link&MOFFSET, segment);
                        int chunkSize = (int) (link>>>48);
                        //get value of next link
                        link = in2.readLong();
                        //copy data into in
                        if(in.length<totalSize+chunkSize-8){
                            in = Arrays.copyOf(in, Math.max(in.length*2,totalSize+chunkSize-8 ));
                        }
                        in2.readFully(in,totalSize, chunkSize-8);
                        totalSize+=chunkSize-8;
                    }

                    //copy last chunk of data
                    DataInput in2 = walGetData(link&MOFFSET, segment);
                    int chunkSize = (int) (link>>>48);
                    //copy data into in
                    if(in.length<totalSize+chunkSize){
                        in = Arrays.copyOf(in, Math.max(in.length*2,totalSize+chunkSize ));
                    }
                    in2.readFully(in,totalSize, chunkSize);
                    totalSize+=chunkSize;

                    return deserialize(serializer, totalSize,new DataIO.DataInputByteArray(in,0));
                } catch (IOException e) {
                    throw new IOError(e);
                }

                //read  non-linked record
                DataInput in = walGetData(walval&MOFFSET, segment);
                return deserialize(serializer, (int) (walval>>>48),in);
            }
        }

        long[] offsets = offsetsGet(indexValGet(recid));
        if (offsets == null) {
            return null; //zero size
        }else if (offsets.length==0){
            return deserialize(serializer,0,new DataIO.DataInputByteArray(new byte[0]));
        }else if (offsets.length == 1) {
            //not linked
            int size = (int) (offsets[0] >>> 48);
            long offset = offsets[0] & MOFFSET;
            DataInput in = vol.getDataInput(offset, size);
            return deserialize(serializer, size, in);
        } else {
            //calculate total size
            int totalSize = offsetsTotalSize(offsets);

            //load data
            byte[] b = new byte[totalSize];
            int bpos = 0;
            for (int i = 0; i < offsets.length; i++) {
                int plus = (i == offsets.length - 1)?0:8;
                long size = (offsets[i] >>> 48) - plus;
                if(CC.ASSERT && (size&0xFFFF)!=size)
                    throw new DBException.DataCorruption("size mismatch");
                long offset = offsets[i] & MOFFSET;
                vol.getData(offset + plus, b, bpos, (int) size);
                bpos += size;
            }
            if (CC.ASSERT && bpos != totalSize)
                throw new DBException.DataCorruption("size does not match");

            DataInput in = new DataIO.DataInputByteArray(b);
            return deserialize(serializer, totalSize, in);
        }

    }

    @Override
    public void rollback() throws UnsupportedOperationException {
        commitLock.lock();
        try {
            //flush modified records
            for (int segment = 0; segment < locks.length; segment++) {
                Lock lock = locks[segment].writeLock();
                lock.lock();
                try {
                    writeCache[segment].clear();
                    if(caches!=null) {
                        caches[segment].clear();
                    }
                } finally {
                    lock.unlock();
                }
            }

            structuralLock.lock();
            try {
                dirtyStackPages.clear();

                //restore headVol from backup
                byte[] b = new byte[(int) HEAD_END];
                //TODO use direct copy
                headVolBackup.getData(0,b,0,b.length);
                headVol.putData(0,b,0,b.length);

                lastAllocatedData = parity3Get(headVol.getLong(LAST_PHYS_ALLOCATED_DATA_OFFSET));

                indexPages = indexPagesBackup.clone();
            } finally {
                structuralLock.unlock();
            }
        }finally {
            commitLock.unlock();
        }
    }

    @Override
    public void commit() {
        commitLock.lock();
        try{

            if(compactionInProgress){
                //use record format rather than instruction format.
                String recvalName = getWalFileName("r"+walRec.size());
                Volume v = volumeFactory.makeVolume(recvalName, readonly);
                walRec.add(v);
                v.ensureAvailable(16);
                long offset = 16;

                for(int segment=0;segment<locks.length;segment++) {
                    Lock lock = locks[segment].writeLock();
                    lock.lock();
                    try {
                        LongObjectObjectMap<Object,Serializer> writeCache1 = writeCache[segment];
                        LongLongMap prevLongs = prevLongLongs[segment];
                        long[] set = writeCache1.set;
                        Object[] values = writeCache1.values;
                        for(int i=0;i<set.length;i++){
                            long recid = set[i];
                            if(recid==0)
                                continue;
                            Object value = values[i*2];
                            DataOutputByteArray buf;
                            int size;
                            if (value == TOMBSTONE2) {
                                buf = null;
                                size = -2;
                            } else {
                                Serializer s = (Serializer) values[i*2+1];
                                buf = serialize(value, s); //TODO somehow serialize outside lock?
                                size = buf==null?-1:buf.pos;
                            }

                            int needed = 1+6+4 +(buf==null?0:buf.pos); //TODO int overflow, limit max record size to 1GB
                            //TODO skip page if overlap


                            prevLongs.put(recidToOffset(recid),
                                    (((long)fileNum)<<(5*8)) |  //first 3 bytes is file number
                                    offset                  //wal offset
                                    );

                            v.putUnsignedByte(offset, (5<<4));
                            offset++;

                            v.putSixLong(offset, recid);
                            offset+=6;

                            v.putInt(offset, size);
                            offset+=4;

                            if(size>0) {
                                v.putData(offset, buf.buf, 0, size);
                                offset+=size;
                            }

                            if(buf!=null)
                                recycledDataOut.lazySet(buf);

                        }
                        writeCache1.clear();

                    } finally {
                        lock.unlock();
                    }
                }
                structuralLock.lock();
                try {
                    //finish instruction
                    v.putUnsignedByte(offset, 0);
                    v.sync();
                    v.putLong(8, StoreWAL.WAL_SEAL);
                    v.sync();
                    return;
                }finally {
                    structuralLock.unlock();
                }
            }

            //if big enough, do full WAL replay
            if(volumes.size()>FULL_REPLAY_AFTER_N_TX && !compactionInProgress) {
                commitFullWALReplay();
                return;
            }

            //move all from current longs to prev
            //each segment requires write lock
            for(int segment=0;segment<locks.length;segment++){
                Lock lock = locks[segment].writeLock();
                lock.lock();
                try{
                    flushWriteCacheSegment(segment);

                    long[] v = currLongLongs[segment].table;
                    for(int i=0;i<v.length;i+=2){
                        long offset = v[i];
                        if(offset==0)
                            continue;
                        long value = v[i+1];
                        prevLongLongs[segment].put(offset,value);
                        walPutLong(offset,value);
                        if(checksum && offset>HEAD_END && offset%PAGE_SIZE!=0) {
                            walPutUnsignedShort(offset + 8, DataIO.longHash(value) & 0xFFFF);
                        }
                    }
                    currLongLongs[segment].clear();

                    v = currDataLongs[segment].table;
                    currDataLongs[segment].size=0;
                    for(int i=0;i<v.length;i+=2){
                        long offset = v[i];
                        if(offset==0)
                            continue;
                        long value = v[i+1];
                        prevDataLongs[segment].put(offset,value);
                    }
                    currDataLongs[segment].clear();

                }finally {
                    lock.unlock();
                }
            }
            structuralLock.lock();
            try {
                //flush modified Long Stack Pages into WAL
                {
                    long[] set = dirtyStackPages.set;
                    for(int i=0;i<set.length;i++){
                        long offset = set[i];
                        if(offset==0)
                            continue;
                        byte[] val = (byte[]) dirtyStackPages.values[i];

                        if (CC.ASSERT && offset < PAGE_SIZE)
                            throw new DBException.DataCorruption();
                        if (CC.ASSERT && val.length % 16 != 0)
                            throw new DBException.DataCorruption();
                        if (CC.ASSERT && val.length <= 0 || val.length > MAX_REC_SIZE)
                            throw new DBException.DataCorruption();

                        putDataSingleWithoutLink(-1, offset, val, 0, val.length);

                    }
                    dirtyStackPages.clear();
                }

                headVol.putLong(LAST_PHYS_ALLOCATED_DATA_OFFSET,parity3Set(lastAllocatedData));
                //update index checksum
                headVol.putInt(HEAD_CHECKSUM, headChecksum(headVol));

                // flush headVol into WAL
                byte[] b = new byte[(int) HEAD_END-4];
                //TODO use direct copy
                headVol.getData(4, b, 0, b.length);
                //put headVol into WAL
                putDataSingleWithoutLink(-1, 4L, b, 0, b.length);

                //make copy of current headVol
                headVolBackup.putData(4, b, 0, b.length);
                indexPagesBackup = indexPages.clone();

                long finalOffset = walOffset.get();
                curVol.ensureAvailable(finalOffset + 1); //TODO overlap here
                //put EOF instruction
                curVol.putUnsignedByte(finalOffset, (0 << 4) | (Long.bitCount(finalOffset) & 15));
                curVol.sync();
                //put wal seal
                curVol.putLong(8, WAL_SEAL);
                curVol.sync();

                walStartNextFile();

            } finally {
                structuralLock.unlock();
            }
        }finally {
            commitLock.unlock();
        }
    }

    protected void commitFullWALReplay() {
        if(CC.ASSERT && !commitLock.isHeldByCurrentThread())
            throw new AssertionError();

        //lock all segment locks
        //TODO use series of try..finally statements, perhaps recursion with runnable

        for(int i=0;i<locks.length;i++){
            locks[i].writeLock().lock();
        }
        try {
            //flush entire write cache
            for(int segment=0;segment<locks.length;segment++){
                flushWriteCacheSegment(segment);

                long[] v = currLongLongs[segment].table;
                for(int i=0;i<v.length;i+=2){
                    long offset = v[i];
                    if(offset==0)
                        continue;
                    long value = v[i+1];
                    walPutLong(offset,value);
                    if(checksum && offset>HEAD_END && offset%PAGE_SIZE!=0) {
                        walPutUnsignedShort(offset + 8, DataIO.longHash(value) & 0xFFFF);
                    }

                    //remove from this
                    v[i] = 0;
                    v[i+1] = 0;
                }
                currLongLongs[segment].clear();

                if(CC.ASSERT && currLongLongs[segment].size()!=0)
                    throw new AssertionError();

                currDataLongs[segment].clear();
                prevDataLongs[segment].clear();
                prevLongLongs[segment].clear();
            }
            structuralLock.lock();
            try {
                //flush modified Long Stack Pages into WAL
                {
                    long[] set = dirtyStackPages.set;
                    for(int i=0;i<set.length;i++){
                        long offset = set[i];
                        if(offset==0)
                            continue;
                        byte[] val = (byte[]) dirtyStackPages.values[i];

                        if (CC.ASSERT && offset < PAGE_SIZE)
                            throw new DBException.DataCorruption();
                        if (CC.ASSERT && val.length % 16 != 0)
                            throw new DBException.DataCorruption();
                        if (CC.ASSERT && val.length <= 0 || val.length > MAX_REC_SIZE)
                            throw new DBException.DataCorruption();

                        putDataSingleWithoutLink(-1, offset, val, 0, val.length);
                    }
                    dirtyStackPages.clear();
                }
                if(CC.ASSERT && dirtyStackPages.size!=0)
                    throw new AssertionError();

                pageLongStack.clear();

                headVol.putLong(LAST_PHYS_ALLOCATED_DATA_OFFSET,parity3Set(lastAllocatedData));

                //update index checksum
                headVol.putInt(HEAD_CHECKSUM, headChecksum(headVol));

                // flush headVol into WAL
                byte[] b = new byte[(int) HEAD_END-4];
                //TODO use direct copy
                headVol.getData(4, b, 0, b.length);
                //put headVol into WAL
                putDataSingleWithoutLink(-1, 4L, b, 0, b.length);

                //make copy of current headVol
                headVolBackup.putData(4, b, 0, b.length);
                indexPagesBackup = indexPages.clone();

                long finalOffset = walOffset.get();
                curVol.ensureAvailable(finalOffset+1); //TODO overlap here
                //put EOF instruction
                curVol.putUnsignedByte(finalOffset, (0<<4) | (Long.bitCount(finalOffset)&15));
                curVol.sync();
                //put wal seal
                curVol.putLong(8, WAL_SEAL);
                curVol.sync();

                //now replay full WAL
                replayWAL();

                walStartNextFile();
            } finally {
                structuralLock.unlock();
            }
        }finally {
            for(int i=locks.length-1;i>=0;i--){
                locks[i].writeLock().unlock();
            }
        }
    }


    protected void replayWAL(){

         /*
          Init Open for StoreWAL has following phases:

          1) check existing files and their seals
          2) if compacted file exists, swap it with original
          3) if Record WAL files exists, initialize Memory Allocator
          4) if Record WAL exists, convert it to WAL
          5) replay WAL if any
          6) reinitialize memory allocator if replay WAL happened
         */

        //check if compaction files are present and walid
        final boolean compaction =
                walC!=null && !walC.isEmpty() &&
                walCCompact!=null && !walCCompact.isEmpty();


        if(compaction){
            //check compaction file was finished well
            walC.ensureAvailable(16);
            boolean walCSeal = walC.getLong(8) == WAL_SEAL;

            //TODO if walCSeal check indexChecksum on walCCompact volume

            if(!walCSeal){
                LOG.warning("Compaction failed, seal not present. Removing incomplete compacted file, keeping old fragmented file.");
                walC.close();
                walC.deleteFile();
                walC = null;
                walCCompact.close();
                walCCompact.deleteFile();
                walCCompact = null;
            }else{

                //compaction is valid, so swap compacted file with current
                if(vol.getFile()==null){
                    //no file present, so we are in-memory, just swap volumes
                    //in memory vol without file, just swap everything
                    Volume oldVol = this.vol;
                    this.realVol = walCCompact;
                    this.vol = new Volume.ReadOnly(realVol);
                    this.headVol.close();
                    this.headVolBackup.close();
                    initHeadVol();
                    //TODO update variables
                    oldVol.close();
                }else{
                    //file is not null, we are working on file system, so swap files
                    File walCCompactFile = walCCompact.getFile();
                    walCCompact.sync();
                    walCCompact.close();
                    walCCompact = null;

                    File thisFile = new File(fileName);
                    File thisFileBackup = new File(fileName+".wal.c.orig");

                    this.vol.close();
                    if(!thisFile.renameTo(thisFileBackup)){
                        //TODO recovery here. Perhaps copy data from one file to other, instead of renaming it
                        throw new AssertionError("failed to rename file " + thisFile);
                    }

                    //rename compacted file to current file
                    if (!walCCompactFile.renameTo(thisFile)) {
                        //TODO recovery here.
                        throw new AssertionError("failed to rename file " + walCCompactFile);
                    }

                    //and reopen volume
                    this.realVol = volumeFactory.makeVolume(this.fileName, readonly);
                    this.vol = new Volume.ReadOnly(this.realVol);
                    this.initHeadVol();

                    //delete orig file
                    if(!thisFileBackup.delete()){
                        LOG.warning("Could not delete original compacted file: "+thisFileBackup);
                    }
                }
                walC.close();
                walC.deleteFile();
                walC = null;

                initOpenPost();
            }
        }

        if(!walRec.isEmpty()){
            //convert walRec into WAL log files.
            //memory allocator was not available at the time of compaction
//  TODO no wal open during compaction
//            if(CC.ASSERT && !volumes.isEmpty())
//                throw new AssertionError();
//
//            if(CC.ASSERT && curVol!=null)
//                throw new AssertionError();
            structuralLock.lock();
            try {
                walStartNextFile();
            }finally {
                structuralLock.unlock();
            }

            for(Volume wr:walRec){
                if(wr.isEmpty())
                    break;
                wr.ensureAvailable(16); //TODO this should not be here, Volume should be already mapped if file existsi
                if(wr.getLong(8)!=StoreWAL.WAL_SEAL)
                    break;
                long pos = 16;
                for(;;) {
                    int instr = wr.getUnsignedByte(pos++);
                    if (instr >>> 4 == 0) {
                        //EOF
                        break;
                    } else if (instr >>> 4 != 5) {
                        //TODO failsafe with corrupted wal
                        throw new DBException.DataCorruption("Invalid instruction in WAL REC" + (instr >>> 4));
                    }

                    long recid = wr.getSixLong(pos);
                    pos += 6;
                    int size = wr.getInt(pos);
                    //TODO zero size, null records, tombstone
                    pos += 4;
                    byte[] arr = new byte[size]; //TODO reuse array if bellow certain size
                    wr.getData(pos, arr, 0, size);
                    pos += size;
                    update(recid, arr, Serializer.BYTE_ARRAY_NOSIZE);
                }
            }
            List<Volume> l = new ArrayList(walRec);
            walRec.clear();
            commitFullWALReplay();
            //delete all wr files
            for(Volume wr:l){
                File f = wr.getFile();
                wr.close();
                wr.deleteFile();
                if(f!=null && f.exists() && !f.delete()){
                    LOG.warning("Could not delete WAL REC file: "+f);
                }
            }
            walRec.clear();
        }


        replayWALInstructionFiles();
    }

    private void replayWALInstructionFiles() {
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.ASSERT && !commitLock.isHeldByCurrentThread())
            throw new AssertionError();

        file:for(Volume wal:volumes){
            if(wal.isEmpty()) {
                break file;
            }
            if(wal.getLong(8)!=WAL_SEAL) {
                break file;
                //TODO better handling for corrupted logs
            }

            long pos = 16;
            for(;;) {
                int checksum = wal.getUnsignedByte(pos++);
                int instruction = checksum>>>4;
                checksum = (checksum&15);
                if (instruction == 0) {
                    //EOF
                    if((Long.bitCount(pos-1)&15) != checksum)
                        throw new InternalError("WAL corrupted");
                    continue file;
                } else if (instruction == 1) {
                    //write long
                    long val = wal.getLong(pos);
                    pos += 8;
                    long offset = wal.getSixLong(pos);
                    pos += 6;
                    if(((1+Long.bitCount(val)+Long.bitCount(offset))&15)!=checksum)
                        throw new InternalError("WAL corrupted");
                    realVol.ensureAvailable(offset+8);
                    realVol.putLong(offset, val);
                } else if (instruction == 2) {
                    //write byte[]
                    int dataSize = wal.getUnsignedShort(pos);
                    pos += 2;
                    long offset = wal.getSixLong(pos);
                    pos += 6;
                    byte[] data = new byte[dataSize];
                    wal.getData(pos, data, 0, data.length);
                    pos += data.length;
                    if(((1+Integer.bitCount(dataSize)+Long.bitCount(offset)+sum(data))&15)!=checksum)
                        throw new InternalError("WAL corrupted");
                    //TODO direct transfer
                    realVol.ensureAvailable(offset+data.length);
                    realVol.putData(offset, data, 0, data.length);
                } else if (instruction == 3) {
                    //skip N bytes
                    int skipN = wal.getInt(pos - 1) & 0xFFFFFF; //read 3 bytes
                    if((Integer.bitCount(skipN)&15) != checksum)
                        throw new InternalError("WAL corrupted");
                    pos += 3 + skipN;
                } else if (instruction == 4) {
                    //skip single byte
                    if((Long.bitCount(pos-1)&15) != checksum)
                        throw new InternalError("WAL corrupted");
                } else if (instruction == 6) {
                    //write two bytes
                    long s = wal.getLong(pos);
                    pos+=8;
                    if(((1+Long.bitCount(s))&15) != checksum)
                        throw new InternalError("WAL corrupted");
                    long offset = s&0xFFFFFFFFFFFFL;
                    realVol.ensureAvailable(offset + 2);
                    realVol.putUnsignedShort(offset, (int) (s>>>48));
                }else{
                    throw new InternalError("WAL corrupted, unknown instruction");
                }

            }
        }

        realVol.sync();

        //destroy old wal files
        for(Volume wal:volumes){
            if(!wal.isClosed()) {
                wal.truncate(0);
                wal.close();
            }
            wal.deleteFile();

        }
        fileNum = -1;
        curVol = null;
        volumes.clear();
    }

    private int sum(byte[] data) {
        int ret = 0;
        for(byte b:data){
            ret+=b;
        }
        return Math.abs(ret);
    }

    private int sum(byte[] buf, int bufPos, int size) {
        int ret = 0;
        size+=bufPos;
        while(bufPos<size){
            ret+=buf[bufPos++];
        }
        return Math.abs(ret);
    }


    @Override
    public boolean canRollback() {
        return true;
    }

    @Override
    public void close() {
        compactLock.lock();
        try{
            commitLock.lock();
            try{

                if(closed) {
                    return;
                }

                if(hasUncommitedData()){
                    LOG.warning("Closing storage with uncommited data, those data will be discarted.");
                }


                //TODO do not replay if not dirty
                if(!readonly) {
                    structuralLock.lock();
                    try {
                        replayWAL();
                    } finally {
                        structuralLock.unlock();
                    }
                }

                if(walC!=null)
                    walC.close();
                if(walCCompact!=null)
                    walCCompact.close();


                for(Volume v:walRec){
                    v.close();
                }
                walRec.clear();


                for(Volume v:volumes){
                    v.close();
                }
                volumes.clear();

                vol.close();
                vol = null;

                headVol.close();
                headVol = null;
                headVolBackup.close();
                headVolBackup = null;

                curVol = null;
                dirtyStackPages.clear();

                if(caches!=null){
                    for(Cache c:caches){
                        c.close();
                    }
                    Arrays.fill(caches,null);
                }
                closed = true;
            }finally {
                commitLock.unlock();
            }
        }finally {
            compactLock.unlock();
        }
    }

    @Override
    public void compact() {
        compactLock.lock();

        try{

            if(compactOldFilesExists())
                return;

            commitLock.lock();
            try{
                //check if there are uncommited data, and log warning if yes
                if(hasUncommitedData()){
                    //TODO how to deal with uncommited data? Is there way not to commit? Perhaps upgrade to recordWAL?
                    LOG.warning("Compaction started with uncommited data. Calling commit automatically.");
                }

                snapshotCloseAllOnCompact();

                //cleanup everything
                commitFullWALReplay();
                //start compaction
                compactionInProgress = true;

                //start zero WAL file with compaction flag
                structuralLock.lock();
                try {
                    if(CC.ASSERT && fileNum!=0)
                        throw new AssertionError();
                    if(CC.ASSERT && walC!=null)
                        throw new AssertionError();

                    //start walC file, which indicates if compaction finished fine
                    String walCFileName = getWalFileName("c");
                    if(walC!=null)
                        walC.close();
                    walC = volumeFactory.makeVolume(walCFileName, readonly);
                    walC.ensureAvailable(16);
                    walC.putLong(0,0); //TODO wal header
                    walC.putLong(8,0);

                }finally {
                    structuralLock.unlock();
                }
            }finally {
                commitLock.unlock();
            }

            final long maxRecidOffset = parity1Get(headVol.getLong(MAX_RECID_OFFSET));

            //open target file
            final String targetFile = getWalFileName("c.compact");

            final StoreDirect target = new StoreDirect(targetFile,
                    volumeFactory,
                    null,lockScale,
                    executor==null?LOCKING_STRATEGY_NOLOCK:LOCKING_STRATEGY_WRITELOCK,
                    checksum,compress,null,false,false,0,false,0,
                    null);
            target.init();
            walCCompact = target.vol;

            final AtomicLong maxRecid = new AtomicLong(RECID_LAST_RESERVED);

            compactIndexPages(maxRecidOffset, target, maxRecid);

            while($_TEST_HACK_COMPACT_PRE_COMMIT_WAIT){
                LockSupport.parkNanos(10000);
            }


            target.vol.putLong(MAX_RECID_OFFSET, parity1Set(maxRecid.get() * indexValSize));

            //compaction finished fine, so now flush target file, and seal log file. This makes compaction durable
            target.commit(); //sync all files, that is durable since there are no background tasks

            walC.putLong(8, WAL_SEAL);
            walC.sync();


            commitLock.lock();
            try{

                if(hasUncommitedData()){
                    LOG.warning("Uncommited data at end of compaction, autocommit");

                }
                //TODO there should be full WAL replay, but without commit
                commitFullWALReplay();

                compactionInProgress = false;
            }finally {
                commitLock.unlock();
            }

            while($_TEST_HACK_COMPACT_POST_COMMIT_WAIT){
                LockSupport.parkNanos(10000);
            }

        }finally {
            compactionInProgress = false; //TODO this should be under commitLock, but still better than leaving it true
            compactLock.unlock();
        }
    }

    /** return true if there are uncommited data in current transaction, otherwise false*/
    protected boolean hasUncommitedData() {
        for(int i=0;i<locks.length;i++){
            final Lock lock  = locks[i].readLock();
            lock.lock();
            try{
                if(currLongLongs[i].size()!=0 ||
                        currDataLongs[i].size()!=0 ||
                        writeCache[i].size!=0)
                    return true;
            }finally {
                lock.unlock();
            }
        }
        return false;
    }
}
