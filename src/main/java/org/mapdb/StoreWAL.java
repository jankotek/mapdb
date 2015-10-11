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
import java.io.IOError;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

import static org.mapdb.DataIO.*;

/**
 * Write-Ahead-Log
 */
public class StoreWAL extends StoreCached {


    protected static final int FULL_REPLAY_AFTER_N_TX = 16;


    /**
     * Contains index table modified in previous transactions.
     */
    protected final LongLongMap[] prevLongLongs;
    protected final LongLongMap[] currLongLongs;
    protected final LongLongMap[] prevDataLongs;
    protected final LongLongMap[] currDataLongs;

    protected final LongLongMap pageLongStack = new LongLongMap();

    protected Volume headVolBackup;

    protected long[] indexPagesBackup;

    protected Volume realVol;

    protected volatile boolean $_TEST_HACK_COMPACT_PRE_COMMIT_WAIT =false;

    protected volatile boolean $_TEST_HACK_COMPACT_POST_COMMIT_WAIT =false;

    protected final WriteAheadLog wal;

    public StoreWAL(String fileName) {
        this(fileName,
                fileName == null ? CC.DEFAULT_MEMORY_VOLUME_FACTORY : CC.DEFAULT_FILE_VOLUME_FACTORY,
                null,
                CC.DEFAULT_LOCK_SCALE,
                0,
                false, false, null, false,false, false, null,
                null, 0L, 0L, false,
                0L,
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
            boolean fileLockDisable,
            HeartbeatFileLock fileLockHeartbeat,
            ScheduledExecutorService executor,
            long startSize,
            long sizeIncrement,
            boolean recidReuseDisable,
            long executorScheduledRate,
            int writeQueueSize
        ) {
        super(fileName, volumeFactory, cache,
                lockScale,
                lockingStrategy,
                checksum, compress, password, readonly, snapshotEnable, fileLockDisable, fileLockHeartbeat,
                executor,
                startSize,
                sizeIncrement,
                recidReuseDisable,
                executorScheduledRate,
                writeQueueSize);
        wal = new WriteAheadLog(fileName, volumeFactory, makeFeaturesBitmap());

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

        wal.open(new Replay2(){
            @Override
            public void beforeReplayStart() {
                super.beforeReplayStart();
                initOpenPost();
            }
        });

        //start new WAL file
        //TODO do not start if readonly
        walStartNextFile();

        initOpenPost();
    }

    @Override
    protected void initFailedCloseFiles() {
        wal.initFailedCloseFiles();
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
        byte[] b = new byte[(int) HEAD_END];
        headVol.getData(0, b, 0, b.length);
        headVolBackup = new Volume.SingleByteArrayVol(b);
    }

    protected void walStartNextFile() {
        if (CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        wal.startNextFile();
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

        long val = wal.walPutByteArray(offset, buf, bufPos,size);

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

        return wal.walGetByteArray(longval);
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
    protected long indexValGetRaw(long recid) {
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
        return super.indexValGetRaw(recid);
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
        wal.walPutLong(offset,val);
    }

    @Override
    protected long pageAllocate() {
// TODO compaction assertion
//        if(CC.ASSERT && compactionInProgress)
//            throw new AssertionError();

        long storeSize = storeSizeGet();
        storeSizeSet(storeSize + PAGE_SIZE);
        //TODO clear data on page? perhaps special instruction?

        if(CC.ASSERT && storeSize%PAGE_SIZE!=0)
            throw new DBException.DataCorruption();


        return storeSize;
    }

    @Override
    protected byte[] loadLongStackPage(long pageOffset, boolean willBeModified) {
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
            byte[] b = wal.walGetByteArray2(walval);
            //page is going to be modified, so put it back into dirtyStackPages)
            if (willBeModified) {
                dirtyStackPages.put(pageOffset, b);
            }
            return b;
        }

        //and finally read it from main store
        int pageSize = (int) (parity4Get(vol.getLong(pageOffset)) >>> 48);
        page = new byte[pageSize];
        vol.getData(pageOffset, page, 0, pageSize);
        if (willBeModified){
            dirtyStackPages.put(pageOffset, page);
        }
        return page;
    }


    /** return positions of (possibly) linked record */
    @Override
    protected long[] offsetsGet(int segment, long indexVal) {;
        if(indexVal>>>48==0){
            return ((indexVal&MLINKED)!=0) ? null : StoreDirect.EMPTY_LONGS;
        }

        long[] ret = new long[]{indexVal};
        while((ret[ret.length-1]&MLINKED)!=0){
            ret = Arrays.copyOf(ret, ret.length + 1);
            long oldLink = ret[ret.length-2]&MOFFSET;

            //get WAL position from current transaction, or previous (not yet fully replayed) transactions
            long val = currDataLongs[segment].get(oldLink);
            if(val==0)
                val = prevDataLongs[segment].get(oldLink);
            if(val!=0) {
//                //was found in previous position, read link from WAL
//                int file = (int) ((val>>>32) & 0xFFFFL); // get WAL file number
//                val = val & 0xFFFFFFFFL; // convert to WAL offset;
//                val = volumes.get(file).getLong(val);
                try {
                    val = wal.walGetByteArray(val).readLong();
                } catch (IOException e) {
                    throw new DBException.VolumeIOError(e);
                }
            }else{
                //was not found in any transaction, read from main store
                val = vol.getLong(oldLink);
            }
            ret[ret.length-1] = parity3Get(val);
        }

        if(CC.ASSERT){
           offsetsVerify(ret);
        }

        if (CC.LOG_STORE && LOG.isLoggable(Level.FINEST)) {
            LOG.log(Level.FINEST, "indexVal={0}, ret={1}",
                    new Object[]{Long.toHexString(indexVal), Arrays.toString(ret)});
        }

        return ret;
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

        long[] offsets = offsetsGet(lockPos(recid),indexValGet(recid));
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
            if(wal.getNumberOfFiles()>FULL_REPLAY_AFTER_N_TX) {
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
                        wal.walPutLong(offset,value);

                    }
                    currLongLongs[segment].clear();

                    v = currDataLongs[segment].table;

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

                wal.seal();

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
                    wal.walPutLong(offset,value);

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

                wal.seal();

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

    protected class Replay2 implements WriteAheadLog.WALReplay {
        @Override
        public void beforeReplayStart() {
            if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
                throw new AssertionError();
            if(CC.ASSERT && !commitLock.isHeldByCurrentThread())
                throw new AssertionError();
        }

        @Override
        public void writeLong(long offset, long value) {
            realVol.ensureAvailable(offset+8);
            realVol.putLong(offset, value);
        }

        @Override
        public void writeRecord(long recid, byte[] data) {
            throw new AssertionError();
        }

        @Override
        public void writeByteArray(long offset, byte[] val) {
            realVol.ensureAvailable(offset+val.length);
            realVol.putData(offset, val, 0, val.length);
        }

        @Override
        public void beforeDestroyWAL() {
            realVol.sync();
        }
    }

    protected void replayWAL(){
        WriteAheadLog.WALReplay replay = new Replay2();
        wal.replayWAL(replay);
    }





    @Override
    public boolean canRollback() {
        return true;
    }

    @Override
    public void close() {
            commitLock.lock();
            try{

                if(closed) {
                    return;
                }

                if(hasUncommitedData()){
                    LOG.warning("Closing storage with uncommited data, those data will be discarded.");
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

                wal.close();

                vol.close();
                vol = null;

                headVol.close();
                headVol = null;
                headVolBackup.close();
                headVolBackup = null;


                dirtyStackPages.clear();

                if(caches!=null){
                    for(Cache c:caches){
                        c.close();
                    }
                    Arrays.fill(caches,null);
                }
                if(fileLockHeartbeat !=null) {
                    fileLockHeartbeat.unlock();
                    fileLockHeartbeat = null;
                }
                closed = true;
            }finally {
                commitLock.unlock();
            }
    }

    @Override
    public void compact() {
        LOG.warning("Compaction not yet implemented with StoreWAL, disable transactions to compact this store");
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
