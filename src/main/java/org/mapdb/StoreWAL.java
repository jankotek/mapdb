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
     * Contains index table modifications from previous committed transactions, which are not yet replayed into vol.
     * Key is offset in vol, value is new index table value
     */
    protected final LongLongMap[] committedIndexTable;

    /**
     * Contains index table modifications from current not yet committed transaction.
     * Key is offset in vol, value is new index table value
     */
    protected final LongLongMap[] uncommittedIndexTable;

    /**
     * Contains vol modifications from previous committed transactions, which are not yet replayed into vol.
     * Key is offset in vol, value is walPointer returned by {@link WriteAheadLog#walPutByteArray(long, byte[], int, int)}
     */
    protected final LongLongMap[] committedDataLongs;

    /**
     * Contains vol modifications from current not yet committed transaction.
     * Key is offset in vol, value is walPointer returned by {@link WriteAheadLog#walPutByteArray(long, byte[], int, int)}
     */
    protected final LongLongMap[] uncommittedDataLongs;

    /**
     * Contains modified Long Stack Pages from previous committed transactions, which are not yet replayed into vol.
     * Key is offset in vol, value is walPointer returned by {@link WriteAheadLog#walPutByteArray(long, byte[], int, int)}
     */
    protected final LongLongMap committedPageLongStack = new LongLongMap();

    protected byte[] headVolBackup;

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

        committedIndexTable = new LongLongMap[this.lockScale];
        uncommittedIndexTable = new LongLongMap[this.lockScale];
        committedDataLongs = new LongLongMap[this.lockScale];
        uncommittedDataLongs = new LongLongMap[this.lockScale];
        for (int i = 0; i < committedIndexTable.length; i++) {
            committedIndexTable[i] = new LongLongMap();
            uncommittedIndexTable[i] = new LongLongMap();
            committedDataLongs[i] = new LongLongMap();
            uncommittedDataLongs[i] = new LongLongMap();
        }
    }


    @Override
    protected void initCreate() {
        super.initCreate();
        indexPagesBackup = indexPages.clone();
        realVol = vol;
        //make main vol readonly, to make sure it is never overwritten outside WAL replay
        vol = new Volume.ReadOnly(vol);
    }

    @Override
    public void initOpen(){
        //TODO disable readonly feature for this store

        realVol = vol;

        if(readonly && !Volume.isEmptyFile(fileName+".wal.0"))
            throw new DBException.WrongConfig("There is dirty WAL file, but storage is read-only. Can not replay file");

        wal.open(new WriteAheadLog.WALReplay(){

            @Override
            public void beforeReplayStart() {

            }

            @Override
            public void writeLong(long offset, long value) {
                if(CC.ASSERT && offset%8!=0)
                    throw new AssertionError();
                realVol.ensureAvailable(Fun.roundUp(offset+8, StoreDirect.PAGE_SIZE));
                realVol.putLong(offset,value);
            }

            @Override
            public void writeRecord(long recid, long walId, Volume vol, long volOffset, int length) {
                throw new DBException.DataCorruption();
            }

            @Override
            public void writeByteArray(long offset, long walId, Volume vol, long volOffset, int length) {
                if(CC.ASSERT && offset%8!=0)
                    throw new AssertionError();
                realVol.ensureAvailable(Fun.roundUp(offset + length, StoreDirect.PAGE_SIZE));
                vol.transferInto(volOffset, realVol, offset,length);
            }

            @Override
            public void beforeDestroyWAL() {

            }

            @Override
            public void commit() {

            }

            @Override
            public void rollback() {
                throw new DBException.DataCorruption();
            }

            @Override
            public void writeTombstone(long recid) {
                throw new DBException.DataCorruption();
            }

            @Override
            public void writePreallocate(long recid) {
                throw new DBException.DataCorruption();
            }
        });
        realVol.sync();
        wal.destroyWalFiles();

        initOpenPost();

        //TODO reenable this assertion
//        if(CC.PARANOID)
//            storeCheck();
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
        headVolBackup = new byte[(int) HEAD_END];
        headVol.getData(0, headVolBackup, 0, headVolBackup.length);
    }

    @Override
    protected void putDataSingleWithLink(int segment, long offset, long link, byte[] buf, int bufPos, int size) {
        if(CC.ASSERT && (size&0xFFFF)!=size)
            throw new DBException.DataCorruption();
        //PERF optimize so array copy is not necessary, that means to clone and modify putDataSingleWithoutLink method
        byte[] buf2 = new  byte[size+8];
        DataIO.putLong(buf2,0,link);
        System.arraycopy(buf,bufPos,buf2,8,size);
        putDataSingleWithoutLink(segment,offset,buf2,0,buf2.length);
    }

    @Override
    protected void putDataSingleWithoutLink(int segment, long offset, byte[] buf, int bufPos, int size) {
        if (CC.ASSERT && offset < PAGE_SIZE)
            throw new DBException.DataCorruption("offset to small");
        if (CC.ASSERT && size <= 0 || size > MAX_REC_SIZE)
            throw new DBException.DataCorruption("wrong length");

        if(CC.ASSERT && segment>=0)
            assertWriteLocked(segment);

        long val = wal.walPutByteArray(offset, buf, bufPos,size);
        uncommittedDataLongs[segment].put(offset, val);
    }


    protected DataInput walGetData(long offset, int segment) {
        if (CC.ASSERT && offset % 16 != 0)
            throw new DBException.DataCorruption();

        long longval = uncommittedDataLongs[segment].get(offset);
        if(longval==0){
            longval = committedDataLongs[segment].get(offset);
        }
        if(longval==0)
            return null;

        return wal.walGetByteArray(longval);
    }

    @Override
    protected long indexValGet(long recid) {
        if(CC.ASSERT)
            assertReadLocked(lockPos(recid));
        int segment = lockPos(recid);
        long offset = recidToOffset(recid);
        long ret = uncommittedIndexTable[segment].get(offset);
        if(ret!=0) {
            return ret;
        }
        ret = committedIndexTable[segment].get(offset);
        if(ret!=0)
            return ret;
        return super.indexValGet(recid);
    }

    @Override
    protected long indexValGetRaw(long recid) {
        if(CC.ASSERT)
            assertReadLocked(lockPos(recid));
        int segment = lockPos(recid);
        long offset = recidToOffset(recid);
        long ret = uncommittedIndexTable[segment].get(offset);
        if(ret!=0) {
            return ret;
        }
        ret = committedIndexTable[segment].get(offset);
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
        uncommittedIndexTable[lockPos(recid)].put(recidToOffset(recid), newVal);
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
        byte[] page = uncommittedStackPages.get(pageOffset);
        if (page != null) {
            return page;
        }

        //try to get it from previous TX stored in WAL, but not yet replayed
        long walval = committedPageLongStack.get(pageOffset);
        if(walval!=0){
            byte[] b = wal.walGetByteArray2(walval);
            //page is going to be modified, so put it back into uncommittedStackPages)
            if (willBeModified) {
                uncommittedStackPages.put(pageOffset, b);
            }
            return b;
        }

        //and finally read it from main store
        int pageSize = (int) (parity4Get(vol.getLong(pageOffset)) >>> 48);
        page = new byte[pageSize];
        vol.getData(pageOffset, page, 0, pageSize);
        if (willBeModified){
            uncommittedStackPages.put(pageOffset, page);
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
            long val = uncommittedDataLongs[segment].get(oldLink);
            if(val==0)
                val = committedDataLongs[segment].get(oldLink);
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
            assertReadLocked(lockPos(recid));
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
            long walval = uncommittedIndexTable[segment].get(recidToOffset(recid));
            if(walval==0) {
                walval = committedIndexTable[segment].get(recidToOffset(recid));
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
                    uncommittedDataLongs[segment].clear();
                    uncommittedIndexTable[segment].clear();
                } finally {
                    lock.unlock();
                }
            }

            structuralLock.lock();
            try {
                uncommittedStackPages.clear();

                //restore headVol from backup
                headVol.putData(0,headVolBackup,0,headVolBackup.length);
                indexPages = indexPagesBackup.clone();

                wal.rollback();
                wal.sync();
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
            //flush write caches into write ahead log
            flushWriteCache();

            //move uncommited data to committed
            for(int segment=0;segment<locks.length;segment++){
                locks[segment].writeLock().lock();
                try{
                    //dump index vals into WAL
                    long[] table = uncommittedIndexTable[segment].table;
                    for(int i=0;i<table.length;){
                        long offset = table[i++];
                        long val = table[i++];
                        if(offset==0)
                            continue;
                        wal.walPutLong(offset,val);
                    }

                    moveAndClear(uncommittedIndexTable[segment], committedIndexTable[segment]);
                    moveAndClear(uncommittedDataLongs[segment], committedDataLongs[segment]);

                }finally {
                    locks[segment].writeLock().unlock();
                }
            }

            structuralLock.lock();
            try {
                //flush modified Long Stack pages into WAL
                long[] set = uncommittedStackPages.set;
                longStackPagesLoop:
                for (int i = 0; i < set.length; i++) {
                    long offset = set[i];
                    if (offset == 0)
                        continue longStackPagesLoop;
                    byte[] val = (byte[]) uncommittedStackPages.values[i];

                    if(val==LONG_STACK_PAGE_TOMBSTONE)
                        committedPageLongStack.put(offset,-1);
                    else {
                        if (CC.ASSERT)
                            assertLongStackPage(offset, val);

                        long walPointer = wal.walPutByteArray(offset, val, 0, val.length);
                        committedPageLongStack.put(offset, walPointer);
                    }
                }
                uncommittedStackPages.clear();

                //update checksum
                headVol.putInt(HEAD_CHECKSUM, headChecksum(headVol));
                //take backup of headVol
                headVol.getData(0,headVolBackup,0,headVolBackup.length);
                wal.walPutByteArray(0, headVolBackup,0, headVolBackup.length);
                wal.commit();
                wal.seal();
                replaySoft();
                realVol.sync();
                wal.destroyWalFiles();
            }finally {
                structuralLock.unlock();
            }
        }finally {
            commitLock.unlock();
        }
    }

    private void moveAndClear(LongLongMap from, LongLongMap to) {
        long[] table = from.table;
        for(int i=0;i<table.length;){
            long key = table[i++];
            long val = table[i++];
            if(key==0)
                continue;
            to.put(key,val);
        }
        from.clear();
    }

    protected void replaySoft(){
        if(CC.ASSERT && !commitLock.isHeldByCurrentThread())
            throw new AssertionError();

        LongList written = CC.PARANOID? new LongList():null;

        for(int lockPos = 0; lockPos< locks.length; lockPos++){
            locks[lockPos].writeLock().lock();
            try{
                //update index table
                long[] table = committedIndexTable[lockPos].table;
                indexValLoop:
                for(int pos=0;pos<table.length;){
                    long recidOffset = table[pos++];
                    long val = table[pos++];
                    if(recidOffset==0 || val==-1)
                        continue indexValLoop;

                    realVol.ensureAvailable(Fun.roundUp(recidOffset+8, StoreDirect.PAGE_SIZE));
                    realVol.putLong(recidOffset,val);

                    if(CC.PARANOID){
                        //check this is index page
                        if(!Fun.arrayContains(indexPages,Fun.roundDown(recidOffset,PAGE_SIZE))) {
                            throw new AssertionError("not index page");
                        }
                    }
                }
                committedIndexTable[lockPos].clear();

                //write data
                table = committedDataLongs[lockPos].table;
                dataLoop:
                for(int pos=0;pos<table.length;){
                    long volOffset = table[pos++];
                    long walPointer = table[pos++];
                    if(volOffset==0 || walPointer==-1)
                        continue dataLoop;

                    byte[] b = wal.walGetByteArray2(walPointer);
                    if(CC.ASSERT)
                        assertRecord(volOffset, b);

                    realVol.ensureAvailable(Fun.roundUp(volOffset+b.length, StoreDirect.PAGE_SIZE));
                    realVol.putData(volOffset, b, 0, b.length);
                    if(CC.ASSERT && b.length>MAX_REC_SIZE)
                        throw new AssertionError();

                    if(CC.PARANOID)
                        written.add((volOffset<<16) | b.length);
                }
                committedDataLongs[lockPos].clear();
            }finally {
                locks[lockPos].writeLock().unlock();
            }
        }
        structuralLock.lock();
        try{
            //flush modified Long Stack pages
            dataLoop:
            for(int pos=0;pos<committedPageLongStack.table.length;){
                long volOffset = committedPageLongStack.table[pos++];
                long walPointer = committedPageLongStack.table[pos++];
                if(volOffset==0 || walPointer==-1)
                    continue dataLoop;

                byte[] b = wal.walGetByteArray2(walPointer);
                if(CC.ASSERT)
                    assertLongStackPage(volOffset, b);

                realVol.ensureAvailable(Fun.roundUp(volOffset+b.length, StoreDirect.PAGE_SIZE));
                realVol.putData(volOffset, b, 0, b.length);

                if(CC.PARANOID)
                    written.add((volOffset<<16) | b.length);
            }
            committedPageLongStack.clear();

            if(CC.PARANOID){
                byte[] headVolBuf = new byte[headVolBackup.length];
                headVol.getData(0, headVolBuf, 0, headVolBuf.length);
                if(!Arrays.equals(headVolBuf, headVolBackup))
                    throw new AssertionError();
            }

            //update page header
            realVol.putData(0,headVolBackup,0,headVolBackup.length);
        }finally {
            structuralLock.unlock();
        }

        if(CC.PARANOID){
            //check for overlaps
            long[] w = Arrays.copyOf(written.array,written.size);
            Arrays.sort(w);
            for(int i=0;i<w.length-1;i++){
                long offset1 = w[i]>>>16;
                long size1 = w[i] & 0xFF;
                long offset2 = w[i+1]>>>16;
                long size2 = w[i+1] & 0xFF;

                if(offset1+size1>offset2){
                    throw new AssertionError("write overlap conflict at: "+offset1+" + "+size1+" > "+offset2 + " ("+size2+")");
                }
            }
        }

    }

    private void assertRecord(long volOffset, byte[] b) {
        if(CC.ASSERT && volOffset<PAGE_SIZE)
            throw new AssertionError();
        if(CC.ASSERT && b.length>MAX_REC_SIZE)
            throw new AssertionError();
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
                    LOG.warning("Closing storage with uncommited data, this data will be discarded.");
                }

                headVol.putData(0,headVolBackup,0,headVolBackup.length);

                if(!readonly) {
                    replaySoft();
                    wal.destroyWalFiles();
                }
                wal.close();

                vol.close();
                vol = null;

                headVol.close();
                headVol = null;
                headVolBackup = null;

                uncommittedStackPages.clear();

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
                if(uncommittedIndexTable[i].size()!=0 ||
                        uncommittedDataLongs[i].size()!=0 ||
                        writeCache[i].size!=0)
                    return true;
            }finally {
                lock.unlock();
            }
        }
        return false;
    }

    @Override
    protected void freeDataPut(int segment, long offset, int size) {
        if(CC.ASSERT && segment>=0)
            assertWriteLocked(segment);
        if(segment>=0) {
            uncommittedDataLongs[segment].put(offset, -1);
        }
        super.freeDataPut(segment, offset, size);
    }
}
