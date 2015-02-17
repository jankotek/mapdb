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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import static org.mapdb.DataIO.parity16Get;
import static org.mapdb.DataIO.parity16Set;
import static org.mapdb.DataIO.parity4Get;

/**
 * Write-Ahead-Log
 */
public class StoreWAL extends StoreCached {


    public static final String TRANS_LOG_FILE_EXT = ".t";

    protected static final long WAL_SEAL = 8234892392398238983L;
    protected static final int WAL_CHECKSUM_MASK = 0x1F; //5 bits

    protected static final int FULL_REPLAY_AFTER_N_TX = 16;


    protected final LongLongMap[] prevLongLongs;
    protected final LongLongMap[] currLongLongs;
    protected final LongLongMap[] prevDataLongs;
    protected final LongLongMap[] currDataLongs;

    protected final LongLongMap pageLongStack = new LongLongMap();
    protected final List<Volume> volumes = new CopyOnWriteArrayList<Volume>();

    protected Volume curVol;

    protected int fileNum = -1;

    //TODO how to protect concurrrently file offset when file is being swapped?
    protected final AtomicLong walOffset = new AtomicLong();

    protected Volume headVolBackup;

    protected long[] indexPagesBackup;

    protected Volume realVol;


    public StoreWAL(String fileName) {
        this(fileName,
                fileName == null ? Volume.memoryFactory() : Volume.fileFactory(),
                null,
                CC.DEFAULT_LOCK_SCALE,
                0,
                false, false, null, false, 0,
                false, 0);
    }

    public StoreWAL(
            String fileName,
            Fun.Function1<Volume, String> volumeFactory,
            Cache cache,
            int lockScale,
            int lockingStrategy,
            boolean checksum,
            boolean compress,
            byte[] password,
            boolean readonly,
            int freeSpaceReclaimQ,
            boolean commitFileSyncDisable,
            int sizeIncrement) {
        super(fileName, volumeFactory, cache,
                lockScale,
                lockingStrategy,
                checksum, compress, password, readonly,
                freeSpaceReclaimQ, commitFileSyncDisable, sizeIncrement);
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
        String wal0Name = getWalFileName(0);
        if(wal0Name!=null && new File(wal0Name).exists()){
            //fill wal files
            for(int i=0;;i++){
                String wname = getWalFileName(i);
                if(!new File(wname).exists())
                    break;
                volumes.add(volumeFactory.run(wname));
            }

            replayWAL();

            volumes.clear();
        }

        //start new WAL file
        walStartNextFile();

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
        headVolBackup = new Volume.ByteArrayVol(CC.VOLUME_PAGE_SHIFT);
        headVolBackup.ensureAvailable(HEAD_END);
        byte[] b = new byte[(int) HEAD_END];
        //TODO use direct copy
        headVol.getData(0,b,0,b.length);
        headVolBackup.putData(0,b,0,b.length);
    }

    protected void walStartNextFile() {
        if (CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        fileNum++;
        if (CC.PARANOID && fileNum != volumes.size())
            throw new AssertionError();
        String filewal = getWalFileName(fileNum);
        Volume nextVol;
        if (readonly && filewal != null && !new File(filewal).exists()){
            nextVol = new Volume.ReadOnly(new Volume.ByteArrayVol(8));
        }else {
            nextVol = volumeFactory.run(filewal);
        }
        nextVol.ensureAvailable(16);
        //TODO write headers and stuff
        walOffset.set(16);
        volumes.add(nextVol);

        curVol = nextVol;
    }

    protected String getWalFileName(int fileNum) {
        return fileName==null? null :
                fileName+"."+fileNum+".wal";
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

        curVol2.ensureAvailable(walOffset2+plusSize);
        int parity = 1+Long.bitCount(value)+Long.bitCount(offset);
        parity &=31;
        curVol2.putUnsignedByte(walOffset2, (1 << 5)|parity);
        walOffset2+=1;
        curVol2.putLong(walOffset2, value);
        walOffset2+=8;
        curVol2.putSixLong(walOffset2, offset);
    }

    protected boolean hadToSkip(long walOffset2, int plusSize) {
        //does it overlap page boundaries?
        if((walOffset2>>>CC.VOLUME_PAGE_SHIFT)==(walOffset2+plusSize)>>>CC.VOLUME_PAGE_SHIFT){
            return false; //no, does not, all fine
        }

        //is there enough space for 4 byte skip N bytes instruction?
        while((walOffset2&PAGE_MASK) >= PAGE_SIZE-4 || plusSize<5){
            //pad with single byte skip instructions, until end of page is reached
            int singleByteSkip = (4<<5)|(Long.bitCount(walOffset2)&31);
            curVol.putUnsignedByte(walOffset2++, singleByteSkip);
            plusSize--;
            if(CC.PARANOID && plusSize<0)
                throw new AssertionError();
        }

        //now new page starts, so add skip instruction for remaining bits
        int val = (3<<(5+3*8)) | (plusSize-4) | ((Integer.bitCount(plusSize-4)&31)<<(3*8));
        curVol.ensureAvailable(walOffset2+4);
        curVol.putInt(walOffset2,val);

        return true;
        }

    protected long walGetLong(long offset, int segment){
        if(CC.PARANOID && offset%8!=0)
            throw new AssertionError();
        long ret = currLongLongs[segment].get(offset);
        if(ret==0) {
            ret = prevLongLongs[segment].get(offset);
        }

        return ret;
    }

    @Override
    protected void putDataSingleWithLink(int segment, long offset, long link, byte[] buf, int bufPos, int size) {
        if(CC.PARANOID && (size&0xFFFF)!=size)
            throw new AssertionError();
        //TODO optimize so array copy is not necessary, that means to clone and modify putDataSingleWithoutLink method
        byte[] buf2 = new  byte[size+8];
        DataIO.putLong(buf2,0,link);
        System.arraycopy(buf,bufPos,buf2,8,size);
        putDataSingleWithoutLink(segment,offset,buf2,0,buf2.length);
    }

    @Override
    protected void putDataSingleWithoutLink(int segment, long offset, byte[] buf, int bufPos, int size) {
        if(CC.PARANOID && (size&0xFFFF)!=size)
            throw new AssertionError();
        if(CC.PARANOID && (offset%16!=0 && offset!=4))
            throw new AssertionError();
//        if(CC.PARANOID && size%16!=0)
//            throw new AssertionError(); //TODO allign record size to 16, and clear remaining bytes
        if(CC.PARANOID && segment!=-1)
            assertWriteLocked(segment);
        if(CC.PARANOID && segment==-1 && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        final int plusSize = +1+2+6+size;
        long walOffset2 = walOffset.getAndAdd(plusSize);

        if(hadToSkip(walOffset2, plusSize)){
            putDataSingleWithoutLink(segment,offset,buf,bufPos,size);
            return;
        }

        curVol.ensureAvailable(walOffset2+plusSize);
        int checksum = 1+Integer.bitCount(size)+Long.bitCount(offset)+sum(buf,bufPos,size);
        checksum &= 31;
        curVol.putUnsignedByte(walOffset2, (2 << 5)|checksum);
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
        if (CC.PARANOID && offset % 16 != 0)
            throw new AssertionError();

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
        if(CC.PARANOID)
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
        if(CC.PARANOID)
            assertWriteLocked(lockPos(recid));
        long newVal = composeIndexVal(size, offset, linked, unused, true);
        currLongLongs[lockPos(recid)].put(recidToOffset(recid),newVal);
    }

    @Override
    protected void indexLongPut(long offset, long val) {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw  new AssertionError();
        walPutLong(offset,val);
    }

    @Override
    protected long pageAllocate() {
        long storeSize = parity16Get(headVol.getLong(STORE_SIZE));
        headVol.putLong(STORE_SIZE, parity16Set(storeSize + PAGE_SIZE));
        //TODO clear data on page? perhaps special instruction?

        if(CC.PARANOID && storeSize%PAGE_SIZE!=0)
            throw new AssertionError();

        return storeSize;
    }

    @Override
    protected byte[] loadLongStackPage(long pageOffset) {
        if (CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

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
        int pageSize = (int) (parity4Get(vol.getLong(pageOffset + 4)) >>> 48);
        page = new byte[pageSize];
        vol.getData(pageOffset, page, 0, pageSize);
        dirtyStackPages.put(pageOffset, page);
        return page;
    }

    @Override
    protected <A> A get2(long recid, Serializer<A> serializer) {
        if (CC.PARANOID)
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

        long[] offsets = offsetsGet(recid);
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
                if(CC.PARANOID && (size&0xFFFF)!=size)
                    throw new AssertionError("size mismatch");
                long offset = offsets[i] & MOFFSET;
                vol.getData(offset + plus, b, bpos, (int) size);
                bpos += size;
            }
            if (CC.PARANOID && bpos != totalSize)
                throw new AssertionError("size does not match");

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
                    caches[segment].clear();
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
            //if big enough, do full WAL replay
            if(volumes.size()>FULL_REPLAY_AFTER_N_TX) {
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

                        if (CC.PARANOID && offset < PAGE_SIZE)
                            throw new AssertionError();
                        if (CC.PARANOID && val.length % 16 != 0)
                            throw new AssertionError();
                        if (CC.PARANOID && val.length <= 0 || val.length > MAX_REC_SIZE)
                            throw new AssertionError();

                        putDataSingleWithoutLink(-1, offset, val, 0, val.length);

                    }
                    dirtyStackPages.clear();
                }

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
                curVol.putUnsignedByte(finalOffset, (0<<5) | (Long.bitCount(finalOffset)));
                curVol.sync();
                //put wal seal
                curVol.putLong(8, WAL_SEAL);

                walStartNextFile();
            } finally {
                structuralLock.unlock();
            }
        }finally {
            commitLock.unlock();
        }
    }

    private void commitFullWALReplay() {
        if(CC.PARANOID && !commitLock.isHeldByCurrentThread())
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

                    //remove from this
                    v[i] = 0;
                    v[i+1] = 0;
                }
                currLongLongs[segment].clear();

                if(CC.PARANOID && currLongLongs[segment].size()!=0)
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
                            continue;;
                        byte[] val = (byte[]) dirtyStackPages.values[i];

                        if (CC.PARANOID && offset < PAGE_SIZE)
                            throw new AssertionError();
                        if (CC.PARANOID && val.length % 16 != 0)
                            throw new AssertionError();
                        if (CC.PARANOID && val.length <= 0 || val.length > MAX_REC_SIZE)
                            throw new AssertionError();

                        putDataSingleWithoutLink(-1, offset, val, 0, val.length);
                    }
                    dirtyStackPages.clear();
                }
                if(CC.PARANOID && dirtyStackPages.size!=0)
                    throw new AssertionError();

                pageLongStack.clear();

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
                curVol.putUnsignedByte(finalOffset, (0<<5) | (Long.bitCount(finalOffset)));
                curVol.sync();
                //put wal seal
                curVol.putLong(8, WAL_SEAL);

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
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.PARANOID && !commitLock.isHeldByCurrentThread())
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
                int instruction = checksum>>>5;
                checksum = (checksum&WAL_CHECKSUM_MASK);
                if (instruction == 0) {
                    //EOF
                    if((Long.bitCount(pos-1)&31) != checksum)
                        throw new InternalError("WAL corrupted");
                    continue file;
                } else if (instruction == 1) {
                    //write long
                    long val = wal.getLong(pos);
                    pos += 8;
                    long offset = wal.getSixLong(pos);
                    pos += 6;
                    if(((1+Long.bitCount(val)+Long.bitCount(offset))&31)!=checksum)
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
                    if(((1+Integer.bitCount(dataSize)+Long.bitCount(offset)+sum(data))&31)!=checksum)
                        throw new InternalError("WAL corrupted");
                    //TODO direct transfer
                    realVol.ensureAvailable(offset+data.length);
                    realVol.putData(offset, data, 0, data.length);
                } else if (instruction == 3) {
                    //skip N bytes
                    int skipN = wal.getInt(pos - 1) & 0xFFFFFF; //read 3 bytes
                    if((Integer.bitCount(skipN)&31) != checksum)
                        throw new InternalError("WAL corrupted");
                    pos += 3 + skipN;
                } else if (instruction == 4) {
                    //skip single byte
                    if((Long.bitCount(pos-1)&31) != checksum)
                        throw new InternalError("WAL corrupted");
                }
            }
        }

        realVol.sync();

        //destroy old wal files
        for(Volume wal:volumes){
            wal.truncate(0);
            wal.close();
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
        commitLock.lock();
        try{
            if(closed)
                return;

            closed = true;

            //TODO do not replay if not dirty
            if(!readonly) {
                structuralLock.lock();
                try {
                    replayWAL();
                } finally {
                    structuralLock.unlock();
                }
            }

            for(Volume v:volumes){
                v.close();
            }
            volumes.clear();

            headVol = null;
            headVolBackup = null;

            curVol = null;
            dirtyStackPages.clear();

            if(caches!=null){
                for(Cache c:caches){
                    c.close();
                }
                Arrays.fill(caches,null);
            }
        }finally {
            commitLock.unlock();
        }
    }
}
