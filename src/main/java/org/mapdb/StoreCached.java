package org.mapdb;

import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

import static org.mapdb.DataIO.*;

/**
 * Extends {@link StoreDirect} with Write Cache
 */
public class StoreCached extends StoreDirect {


    protected static final byte[] LONG_STACK_PAGE_TOMBSTONE = new byte[0];

    /**
     * stores modified stack pages.
     */
    //TODO only accessed under structural lock, should be LongConcurrentHashMap?
    protected final LongObjectMap<byte[]> uncommittedStackPages = new LongObjectMap<byte[]>();
    protected final LongObjectObjectMap[] writeCache;

    protected final static Object TOMBSTONE2 = new Object(){
        @Override
        public String toString() {
            return StoreCached.class.getName()+".TOMBSTONE2";
        }
    };

    protected final int writeQueueSize;
    protected final int writeQueueSizePerSegment;
    protected final boolean flushInThread;

    public StoreCached(
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
            final int writeQueueSize) {
        super(fileName, volumeFactory, cache,
                lockScale,
                lockingStrategy,
                checksum, compress, password, readonly, snapshotEnable, fileLockDisable, fileLockHeartbeat,
                executor,startSize, sizeIncrement, recidReuseDisable);

        this.writeQueueSize = writeQueueSize;
        this.writeQueueSizePerSegment = writeQueueSize/lockScale;

        writeCache = new LongObjectObjectMap[this.lockScale];
        for (int i = 0; i < writeCache.length; i++) {
            writeCache[i] = new LongObjectObjectMap();
        }

        flushInThread = this.executor==null &&
                writeQueueSize!=0 &&
                !(this instanceof StoreWAL); //TODO StoreWAL should dump data into WAL

        if(this.executor!=null &&
                !(this instanceof StoreWAL) //TODO async write should work for StoreWAL as well
                ){
            for(int i=0;i<this.lockScale;i++){
                final int seg = i;
                final Lock lock = locks[i].writeLock();
                this.executor.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        lock.lock();
                        try {
                            if(writeCache[seg].size>writeQueueSizePerSegment) {
                                flushWriteCacheSegment(seg);
                            }
                        }finally {
                            lock.unlock();
                        }
                    }
                    },
                        (long) (executorScheduledRate*Math.random()),
                        executorScheduledRate,
                        TimeUnit.MILLISECONDS);
            }
        }
    }


    public StoreCached(String fileName) {
        this(fileName,
                fileName==null? CC.DEFAULT_MEMORY_VOLUME_FACTORY : CC.DEFAULT_FILE_VOLUME_FACTORY,
                null,
                CC.DEFAULT_LOCK_SCALE,
                0,
                false, false, null, false, false, false, null,
                null, 0L, 0L, false, 0L, 0);
    }



    @Override
    protected void initHeadVol() {
        if (CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        if(this.headVol!=null && !this.headVol.isClosed())
            headVol.close();
        this.headVol = new Volume.SingleByteArrayVol((int) HEAD_END);
        vol.transferInto(0,headVol,0,HEAD_END);
    }


    @Override
    protected void longStackPut(long masterLinkOffset, long value, boolean recursive) {
        if (CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if (CC.ASSERT && (masterLinkOffset <= 0 || masterLinkOffset > PAGE_SIZE || masterLinkOffset % 8 != 0))
            throw new DBException.DataCorruption("wrong master link");

        long masterLinkVal = parity4Get(headVol.getLong(masterLinkOffset));
        long pageOffset = masterLinkVal & MOFFSET;

        if (masterLinkVal == 0L) {
            longStackNewPage(masterLinkOffset, 0L, value, recursive);
            return;
        }

        byte[] page = loadLongStackPage(pageOffset, true);

        long currSize = masterLinkVal >>> 48;

        long prevLinkVal = parity4Get(DataIO.getLong(page, 0));
        long pageSize = prevLinkVal >>> 48;
        //is there enough space in current page?
        if (currSize + 8 >= pageSize) {
            //no there is not enough space
            //first zero out rest of the page
            Arrays.fill(page, (int) currSize, (int) pageSize, (byte) 0);
            //allocate new page
            longStackNewPage(masterLinkOffset, pageOffset, value, recursive);
            return;
        }

        //there is enough space, so just write new value
        currSize += DataIO.packLongBidi(page, (int) currSize, longParitySet(value));

        //and update master pointer
        headVol.putLong(masterLinkOffset, parity4Set(currSize << 48 | pageOffset));
    }

    @Override
    protected long longStackTake(long masterLinkOffset, boolean recursive) {
        if (CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if (CC.ASSERT && (masterLinkOffset < FREE_RECID_STACK ||
                masterLinkOffset > longStackMasterLinkOffset(round16Up(MAX_REC_SIZE)) ||
                masterLinkOffset % 8 != 0))
            throw new DBException.DataCorruption("wrong master link");

        long masterLinkVal = parity4Get(headVol.getLong(masterLinkOffset));
        if (masterLinkVal == 0) {
            return 0;
        }
        long currSize = masterLinkVal >>> 48;
        final long pageOffset = masterLinkVal & MOFFSET;

        byte[] page = loadLongStackPage(pageOffset,true);

        //read packed link from stack
        long ret = DataIO.unpackLongBidiReverse(page, (int) currSize, 8);
        //extract number of read bytes
        long oldCurrSize = currSize;
        currSize -= ret >>> 60;
        //clear bytes occupied by prev value
        Arrays.fill(page, (int) currSize, (int) oldCurrSize, (byte) 0);
        //and finally set return value
        ret = longParityGet(ret & DataIO.PACK_LONG_RESULT_MASK);

        if (CC.ASSERT && currSize < 8)
            throw new DBException.DataCorruption("wrong currSize");

        //is there space left on current page?
        if (currSize > 8) {
            //yes, just update master link
            headVol.putLong(masterLinkOffset, parity4Set(currSize << 48 | pageOffset));
            return ret;
        }

        //there is no space at current page, so delete current page and update master pointer
        long prevPageOffset = parity4Get(DataIO.getLong(page, 0));
        final int currPageSize = (int) (prevPageOffset >>> 48);
        prevPageOffset &= MOFFSET;

        //does previous page exists?
        if (prevPageOffset != 0) {
            //yes previous page exists

            byte[] page2 = loadLongStackPage(prevPageOffset,true);

            //find pointer to end of previous page
            // (data are packed with var size, traverse from end of page, until zeros

            //first read size of current page
            currSize = parity4Get(DataIO.getLong(page2, 0)) >>> 48;

            //now read bytes from end of page, until they are zeros
            while (page2[((int) (currSize - 1))] == 0) {
                currSize--;
            }

            if (CC.ASSERT && currSize < 10)
                throw new DBException.DataCorruption("wrong currSize");
        } else {
            //no prev page does not exist
            currSize = 0;
        }

        //update master link with curr page size and offset
        headVol.putLong(masterLinkOffset, parity4Set(currSize << 48 | prevPageOffset));

        //release old page, size is stored as part of prev page value
        uncommittedStackPages.put(pageOffset,LONG_STACK_PAGE_TOMBSTONE);
        
        freeDataPut(-1, pageOffset, currPageSize);
        //TODO how TX should handle this

        return ret;
    }

    protected byte[] loadLongStackPage(long pageOffset, boolean willBeModified) {
        if (CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        byte[] page = uncommittedStackPages.get(pageOffset);
        if (page == null) {
            int pageSize = (int) (parity4Get(vol.getLong(pageOffset)) >>> 48);
            page = new byte[pageSize];
            vol.getData(pageOffset, page, 0, pageSize);
            if(willBeModified) {
                uncommittedStackPages.put(pageOffset, page);
            }
        }
        if(CC.ASSERT)
            assertLongStackPage(pageOffset, page);
        return page;
    }


    @Override
    protected long longStackCount(final long masterLinkOffset){
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if (CC.ASSERT && (masterLinkOffset <= 0 || masterLinkOffset > PAGE_SIZE || masterLinkOffset % 8 != 0))
            throw new DBException.DataCorruption("wrong master link");

        long nextLinkVal = DataIO.parity4Get(
                headVol.getLong(masterLinkOffset));
        long ret = 0;
        while(true){
            int currSize = (int) (nextLinkVal>>>48);
            final long pageOffset = nextLinkVal&MOFFSET;

            if(pageOffset==0)
                break;

            byte[] page = loadLongStackPage(pageOffset, false);

            //work on dirty page
            while ((page[currSize-1] & 0xFF) == 0) {
                currSize--;
            }

            //iterate from end of page until start of page is reached
            while(currSize>8){
                long read = DataIO.unpackLongBidiReverse(page,currSize,8);
                //extract number of read bytes
                currSize-= read >>>60;
                ret++;
            }

            nextLinkVal = DataIO.parity4Get(
                    DataIO.getLong(page,0));

        }
        return ret;
    }


    @Override
    protected void longStackNewPage(long masterLinkOffset, long prevPageOffset, long value, boolean recursive) {
        if (CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        long newPageSize=LONG_STACK_PREF_SIZE;
        if(!recursive) {
            sizeLoop:
            //loop if we find size which is already used;
            for (long size = LONG_STACK_MAX_SIZE; size >= LONG_STACK_MIN_SIZE; size -= 16) {
                long masterLinkOffset2 = longStackMasterLinkOffset(size);
                if (masterLinkOffset == masterLinkOffset2)
                    continue sizeLoop;
                long indexVal = parity4Get(headVol.getLong(masterLinkOffset2));
                if (indexVal != 0) {
                    newPageSize = size;
                    break sizeLoop;
                }
            }

            if (longStackMasterLinkOffset(newPageSize) == masterLinkOffset) {
                // this would cause recursive mess
                newPageSize += 16;
            }
        }


        // take space, if free space was found, it will be reused
        long newPageOffset = freeDataTakeSingle((int) newPageSize, true);

        byte[] page = new byte[(int) newPageSize];
//TODO this is new page, so data should be clear, no need to read them, but perhaps check data are really zero, handle EOF
//        vol.getData(newPageOffset, page, 0, page.length);
        uncommittedStackPages.put(newPageOffset, page);
        //write size of current chunk with link to prev page
        DataIO.putLong(page, 0, parity4Set((newPageSize << 48) | prevPageOffset));
        //put value
        long currSize = 8 + DataIO.packLongBidi(page, 8, longParitySet(value));
        //update master pointer
        headVol.putLong(masterLinkOffset, parity4Set((currSize << 48) | newPageOffset));
    }

    @Override
    protected void flush() {
        if (CC.ASSERT && !commitLock.isHeldByCurrentThread())
            throw new AssertionError();

        if (isReadOnly())
            return;
        flushWriteCache();


        structuralLock.lock();
        try {
            if(CC.PARANOID){
                assertNoOverlaps(uncommittedStackPages);
            }

            //flush modified Long Stack pages
            long[] set = uncommittedStackPages.set;
            for(int i=0;i<set.length;i++){
                long offset = set[i];
                if(offset==0)
                    continue;
                byte[] val = (byte[]) uncommittedStackPages.values[i];
                if(val==LONG_STACK_PAGE_TOMBSTONE)
                    continue;

                if(CC.ASSERT)
                    assertLongStackPage(offset, val);

                vol.putData(offset, val, 0, val.length);
            }
            uncommittedStackPages.clear();
            //set header checksum
            headVol.putInt(HEAD_CHECKSUM, headChecksum(headVol));
            //and flush head
            byte[] buf = new byte[(int) HEAD_END]; //PERF copy directly
            headVol.getData(0, buf, 0, buf.length);
            vol.putData(0, buf, 0, buf.length);
        } finally {
            structuralLock.unlock();
        }
        vol.sync();
    }

    protected void assertLongStackPage(long offset, byte[] val) {
        if (CC.ASSERT && offset < PAGE_SIZE)
            throw new DBException.DataCorruption("offset to small");
        if (CC.ASSERT && val.length % 16 != 0)
            throw new AssertionError("not aligned to 16");
        if (CC.ASSERT && val.length <= 0 || val.length > MAX_REC_SIZE)
            throw new DBException.DataCorruption("wrong length");
    }


    protected void assertNoOverlaps(LongObjectMap<byte[]> pages) {
        //put all keys into sorted array
        long[] sorted = new long[pages.size];

        int c = 0;
        for(long key:pages.set){
            if(key==0)
                continue;
            sorted[c++] = key;
        }

        Arrays.sort(sorted);

        for(int i=0;i<sorted.length-1;i++){
            long offset = sorted[i];
            long pageSize = pages.get(offset).length;
            long offsetNext = sorted[i+1];

            if(offset+pageSize>offsetNext)
                throw new AssertionError();
        }
    }

    protected void flushWriteCache() {
        if (CC.ASSERT && !commitLock.isHeldByCurrentThread())
            throw new AssertionError();

        //flush modified records
        for (int i = 0; i < locks.length; i++) {
            Lock lock = locks[i].writeLock();
            lock.lock();
            try {
                flushWriteCacheSegment(i);

            } finally {
                lock.unlock();
            }
        }
    }

    protected void flushWriteCacheSegment(int segment) {
        if (CC.ASSERT)
            assertWriteLocked(segment);

        LongObjectObjectMap writeCache1 = writeCache[segment];
        long[] set = writeCache1.set;
        Object[] values = writeCache1.values;
        for(int i=0;i<set.length;i++){
            long recid = set[i];
            if(recid==0)
                continue;
            Object value = values[i*2];
            if (value == TOMBSTONE2) {
                super.delete2(recid, Serializer.ILLEGAL_ACCESS);
            } else {
                Serializer s = (Serializer) values[i*2+1];
                DataOutputByteArray buf = serialize(value, s); //PERF somehow serialize outside lock?
                super.update2(recid, buf);
                recycledDataOut.lazySet(buf);
            }
        }
        writeCache1.clear();

        if (CC.ASSERT && writeCache[segment].size!=0)
            throw new AssertionError();
    }



    @Override
    protected <A> A get2(long recid, Serializer<A> serializer) {
        LongObjectObjectMap m = writeCache[lockPos(recid)];
        Object cached = m.get1(recid);
        if (cached !=null) {
            if(cached==TOMBSTONE2)
                return null;
            return (A) cached;
        }
        return super.get2(recid, serializer);
    }

    @Override
    protected <A> void delete2(long recid, Serializer<A> serializer) {
        if(CC.LOG_STORE_RECORD && LOG.isLoggable(Level.FINER))
            LOG.log(Level.FINER, "REC DEL recid={0}, serializer={1}",new Object[]{recid,serializer});

        if (serializer == null)
            throw new NullPointerException();
        int lockPos = lockPos(recid);

        LongObjectObjectMap map = writeCache[lockPos];
        map.put(recid, TOMBSTONE2, null);

        if(flushInThread && map.size>writeQueueSize){
            flushWriteCacheSegment(lockPos);
        }
    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        if (serializer == null)
            throw new NullPointerException();

        //PERF this causes double locking, merge two methods into single method
        long recid = preallocate();
        update(recid, value, serializer);

        if(CC.LOG_STORE_RECORD && LOG.isLoggable(Level.FINER))
            LOG.log(Level.FINER, "REC PUT recid={0}, value={1}, serializer={2}",new Object[]{recid,value, serializer});

        return recid;
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        if (serializer == null)
            throw new NullPointerException();

        if(CC.LOG_STORE_RECORD && LOG.isLoggable(Level.FINER))
            LOG.log(Level.FINER, "REC UPDATE recid={0}, value={1}, serializer={2}",new Object[]{recid,value, serializer});

        int lockPos = lockPos(recid);
        Cache cache = caches==null ? null : caches[lockPos];
        Lock lock = locks[lockPos].writeLock();
        lock.lock();
        try {
            if(cache!=null) {
                cache.put(recid, value);
            }
            LongObjectObjectMap map = writeCache[lockPos];
            map.put(recid, value, serializer);
            if(flushInThread && map.size>writeQueueSizePerSegment){
                flushWriteCacheSegment(lockPos);
            }

        } finally {
            lock.unlock();
        }
    }


    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();

        //PERF binary CAS & serialize outside lock
        final int lockPos = lockPos(recid);
        final Lock lock = locks[lockPos].writeLock();
        final Cache cache = caches==null ? null : caches[lockPos];
        LongObjectObjectMap<A,Serializer<A>> map = writeCache[lockPos];
        lock.lock();
        try{
            A oldVal = cache==null ? null : (A) cache.get(recid);
            if(oldVal == null) {
                oldVal = get2(recid, serializer);
            }else if(oldVal == Cache.NULL){
                oldVal = null;
            }
            if(oldVal==expectedOldValue || (oldVal!=null && serializer.equals(oldVal,expectedOldValue))){
                if(cache!=null) {
                    cache.put(recid, newValue);
                }
                map.put(recid,newValue,serializer);
                if(flushInThread && map.size>writeQueueSizePerSegment){
                    flushWriteCacheSegment(lockPos);
                }
                if(CC.LOG_STORE_RECORD && LOG.isLoggable(Level.FINER))
                    LOG.log(Level.FINER, "REC CAS DONE recid={0}, oldVal={1}, newVal={2},serializer={3}",new Object[]{recid,expectedOldValue, newValue, serializer});

                return true;
            }
            if(CC.LOG_STORE_RECORD && LOG.isLoggable(Level.FINER))
                LOG.log(Level.FINER, "REC CAS FAIL recid={0}, oldVal={1}, newVal={2},serializer={3}",new Object[]{recid,expectedOldValue, newValue, serializer});

            return false;
        }finally {
            lock.unlock();
        }
    }

    @Override
    void assertZeroes(long startOffset, long endOffset) {
        startOffset = Math.min(startOffset, vol.length());
        endOffset = Math.min(endOffset, vol.length());
        super.assertZeroes(startOffset, endOffset);
    }



}
