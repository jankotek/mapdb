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

import org.omg.CORBA.CODESET_INCOMPATIBLE;

import java.io.DataInput;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Write-Ahead-Log
 */
public class StoreWAL extends StoreCached {


    public static final String TRANS_LOG_FILE_EXT = ".t";
    public static final long LOG_SEAL = 123321234423334324L;


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

        prevLongs = new LongMap[CC.CONCURRENCY];
        currLongs = new LongMap[CC.CONCURRENCY];
        for(int i=0;i<CC.CONCURRENCY;i++){
            prevLongs[i] = new LongHashMap<Long>();
            currLongs[i] = new LongHashMap<Long>();
        }
    }

    protected final LongMap<Long>[] prevLongs;
    protected final LongMap<Long>[] currLongs;
    protected final List<Volume> volumes = new CopyOnWriteArrayList<Volume>();

    protected Volume curVol;

    protected int fileNum;
    protected final AtomicLong walOffset = new AtomicLong();


    protected void walPutLong(long offset, long value, int segment){
        if(CC.PARANOID && !locks[segment].isWriteLocked())
            throw new AssertionError();
        final int plusSize = +1+8+6;
        long walOffset2;
        do{
            walOffset2 = walOffset.get();
        }while(walOffset.compareAndSet(walOffset2, walOffset2+plusSize));

        curVol.ensureAvailable(walOffset2+plusSize);
        curVol.putByte(walOffset2, (byte) (1<<5));
        walOffset2+=1;
        curVol.putLong(walOffset2, value);
        walOffset2+=8;
        curVol.putSixLong(walOffset2, offset);

        currLongs[segment].put(offset, value);
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

    protected void walPutData(long offset, byte[] value, int segment){
        if(CC.PARANOID && offset%16!=0)
            throw new AssertionError();
        if(CC.PARANOID && value.length%16!=0)
            throw new AssertionError();
        if(CC.PARANOID && !locks[segment].isWriteLocked())
            throw new AssertionError();

        final int plusSize = +1+2+6+value.length;
        long walOffset2;
        do{
            walOffset2 = walOffset.get();
        }while(walOffset.compareAndSet(walOffset2, walOffset2+plusSize));

        //TODO if offset overlaps, write skip instruction and try again

        curVol.ensureAvailable(walOffset2+plusSize);
        curVol.putByte(walOffset2, (byte) (2<<5));
        walOffset2+=1;
        curVol.putLong(walOffset2, ((long) value.length) << 48 | offset);
        walOffset2+=8;
        curVol.putData(walOffset2, value,0,value.length);

        //TODO assertions
        long val = ((long)value.length)<<48;
        val |= ((long)fileNum)<<32;
        val |= walOffset2;

        currLongs[segment].put(offset, val);
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
                initHeadVol();

                //TODO restore headVol from backup
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
                        prevLongs[segment].put(iter.key(),iter.value());
                        iter.remove();
                    }
                }finally {
                    lock.unlock();
                }
            }

            //TODO make defensive copy of headVol under structural lock

        }finally {
            commitLock.unlock();
        }
    }


    @Override
    public boolean canRollback() {
        return true;
    }
}
