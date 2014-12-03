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
    public static final long LOG_SEAL = 123321234423334324L;

    protected final LongMap<Long>[] prevLongs;
    protected final LongMap<Long>[] currLongs;
    protected final LongMap<Long> pageLongStack = new LongHashMap<Long>();
    protected final List<Volume> volumes = new CopyOnWriteArrayList<Volume>();

    protected Volume curVol;

    protected int fileNum = -1;

    //TODO how to protect concurrrently file offset when file is being swapped?
    protected final AtomicLong walOffset = new AtomicLong();

    protected Volume headVolBackup;

    protected Volume realVol;


    public StoreWAL(String fileName) {
        this(fileName,
                fileName == null ? Volume.memoryFactory() : Volume.fileFactory(),
                false, false, null, false, false, 0,
                false, 0);
    }

    public StoreWAL(String fileName, Fun.Function1<Volume, String> volumeFactory, boolean checksum, boolean compress,
                    byte[] password, boolean readonly, boolean deleteFilesAfterClose, int freeSpaceReclaimQ,
                    boolean commitFileSyncDisable, int sizeIncrement) {
        super(fileName, volumeFactory, checksum, compress, password, readonly, deleteFilesAfterClose,
                freeSpaceReclaimQ, commitFileSyncDisable, sizeIncrement);

        commitLock.lock();
        try {

            structuralLock.lock();
            try {

                realVol = vol;
                //make main vol readonly, to make sure it is never overwritten outside WAL replay
                vol = new Volume.ReadOnly(vol);

                prevLongs = new LongMap[CC.CONCURRENCY];
                currLongs = new LongMap[CC.CONCURRENCY];
                for (int i = 0; i < CC.CONCURRENCY; i++) {
                    prevLongs[i] = new LongHashMap<Long>();
                    currLongs[i] = new LongHashMap<Long>();
                }

                //TODO disable readonly feature for this store

                //backup headVol
                headVolBackup = new Volume.ByteArrayVol(CC.VOLUME_PAGE_SHIFT);
                headVolBackup.ensureAvailable(HEAD_END);
                byte[] b = new byte[(int) HEAD_END];
                //TODO use direct copy
                headVol.getData(0,b,0,b.length);
                headVolBackup.putData(0,b,0,b.length);

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
            }finally {
                structuralLock.unlock();
            }
        }finally {
            commitLock.unlock();
        }
    }


    protected void walStartNextFile(){
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        fileNum++;
        if(CC.PARANOID && fileNum!=volumes.size())
            throw new AssertionError();
        Volume nextVol = volumeFactory.run(getWalFileName(fileNum));
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
        long walOffset2;
        do{
            walOffset2 = walOffset.get();
        }while(!walOffset.compareAndSet(walOffset2, walOffset2+plusSize));

        curVol.ensureAvailable(walOffset2+plusSize);
        curVol.putUnsignedByte(walOffset2, (byte) (1 << 5));
        walOffset2+=1;
        curVol.putLong(walOffset2, value);
        walOffset2+=8;
        curVol.putSixLong(walOffset2, offset);
    }

    protected long walGetLong(long offset, int segment){
        if(CC.PARANOID && offset%8!=0)
            throw new AssertionError();
        Long ret = currLongs[segment].get(offset);
        if(ret==null) {
            ret = prevLongs[segment].get(offset);
        }

        return ret==null?0L:ret;
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
        if(CC.PARANOID && offset%16!=0)
            throw new AssertionError();
//        if(CC.PARANOID && size%16!=0)
//            throw new AssertionError(); //TODO allign record size to 16, and clear remaining bytes
        if(CC.PARANOID && segment!=-1 && !locks[segment].isWriteLockedByCurrentThread())
            throw new AssertionError();
        if(CC.PARANOID && segment==-1 && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        final int plusSize = +1+2+6+size;
        long walOffset2;
        do{
            walOffset2 = walOffset.get();
        }while(!walOffset.compareAndSet(walOffset2, walOffset2+plusSize));

        //TODO if offset overlaps, write skip instruction and try again

        curVol.ensureAvailable(walOffset2+plusSize);
        curVol.putUnsignedByte(walOffset2, (byte) (2 << 5));
        walOffset2+=1;
        curVol.putLong(walOffset2, ((long) size) << 48 | offset);
        walOffset2+=8;
        curVol.putData(walOffset2, buf,bufPos,size);

        //TODO assertions
        long val = ((long)size)<<48;
        val |= ((long)fileNum)<<32;
        val |= walOffset2;

        (segment==-1?pageLongStack:currLongs[segment]).put(offset, val);
    }

    protected DataInput walGetData(long offset, int segment) {
        if (CC.PARANOID && offset % 16 != 0)
            throw new AssertionError();

        Long longval = currLongs[segment].get(offset);
        if(longval==null){
            prevLongs[segment].get(offset);
        }
        if(longval==null)
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
        Long ret = currLongs[segment].get(offset);
        if(ret!=null) {
            return ret;
        }
        ret = prevLongs[segment].get(offset);
        if(ret!=null)
            return ret;
        return super.indexValGet(recid);
    }

    @Override
    protected void indexValPut(long recid, int size, long offset, boolean linked, boolean unused) {
        if(CC.PARANOID)
            assertWriteLocked(recid);
        long newVal = composeIndexVal(size,offset,linked,unused,true);
        currLongs[lockPos(recid)].put(recidToOffset(recid),newVal);
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
        Long walval = pageLongStack.get(pageOffset);
        if(walval!=null){
            //get file number, offset and size in WAL
            int arraySize = (int) (walval >>> 48);
            int fileNum = (int) ((walval >>> 32) & 0xFFFFL);
            long dataOffset = walval & 0xFFFFFFFFL;
            //read and return data
            byte[] b = new byte[arraySize];
            Volume vol = volumes.get(fileNum);
            vol.getData(dataOffset, b, 0, arraySize);
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
    public void rollback() throws UnsupportedOperationException {
        commitLock.lock();
        try {

            //flush modified records
            for (int segment = 0; segment < locks.length; segment++) {
                Lock lock = locks[segment].writeLock();
                lock.lock();
                try {
                    writeCache[segment].clear();
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
            //move all from current longs to prev
            //each segment requires write lock
            for(int segment=0;segment<currLongs.length;segment++){
                Lock lock = locks[segment].writeLock();
                lock.lock();
                try{
                    flushWriteCacheSegment(segment);

                    LongMap.LongMapIterator<Long> iter = currLongs[segment].longMapIterator();
                    while(iter.moveToNext()){
                        long offset = iter.key();
                        long value = iter.value();
                        prevLongs[segment].put(offset,value);
                        if((value&MARCHIVE)!=0)
                            walPutLong(offset,value);
                        iter.remove();
                    }
                }finally {
                    lock.unlock();
                }
            }
            structuralLock.lock();
            try {
                //flush modified Long Stack Pages into WAL
                LongMap.LongMapIterator<byte[]> iter = dirtyStackPages.longMapIterator();
                while (iter.moveToNext()) {
                    long offset = iter.key();
                    byte[] val = iter.value();

                    if (CC.PARANOID && offset < PAGE_SIZE)
                        throw new AssertionError();
                    if (CC.PARANOID && val.length % 16 != 0)
                        throw new AssertionError();
                    if (CC.PARANOID && val.length <= 0 || val.length > MAX_REC_SIZE)
                        throw new AssertionError();

                    putDataSingleWithoutLink(-1, offset, val, 0, val.length);

                    iter.remove();
                }

                byte[] b = new byte[(int) HEAD_END];
                //TODO use direct copy
                headVol.getData(0, b, 0, b.length);
                //put headVol into WAL
                putDataSingleWithoutLink(-1, 0L, b, 0, b.length);

                //make copy of current headVol
                headVolBackup.putData(0, b, 0, b.length);
                curVol.putUnsignedByte(walOffset.get(),0);
                curVol.sync();
                walStartNextFile();
            } finally {
                structuralLock.unlock();
            }
        }finally {
            commitLock.unlock();
        }
    }


    protected void replayWAL(){
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.PARANOID && !commitLock.isHeldByCurrentThread())
            throw new AssertionError();

        file:for(Volume wal:volumes){
            long pos = 16;
            for(;;) {
                int instruction = wal.getUnsignedByte(pos++)>>>5;
                if (instruction == 0) {
                    //EOF
                    continue file;
                } else if (instruction == 1) {
                    //write long
                    long val = wal.getLong(pos);
                    pos += 8;
                    long offset = wal.getSixLong(pos);
                    pos += 6;
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
                    //TODO direct transfer
                    realVol.putData(offset, data, 0, data.length);
                } else if (instruction == 3) {
                    //skip N bytes
                    int skipN = wal.getInt(pos - 1) & 0xFFFFFF; //read 3 bytes
                    pos += 3 + skipN;
                }
            }
        }

        realVol.sync();

        //destroy old wal files
        for(Volume wal:volumes){
            wal.truncate(0);
            wal.deleteFile();
        }
        volumes.clear();
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

            for(Volume v:volumes){
                v.close();
            }
            volumes.clear();
            headVol = null;
            headVolBackup = null;

            curVol = null;
            dirtyStackPages.clear();
        }finally {
            commitLock.unlock();
        }
    }
}
