package org.mapdb;

import java.io.DataInput;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

/**
 * append only store
 */
public class StoreAppend extends Store {

    /** 2 byte store version*/
    protected static final int STORE_VERSION = 100;

    /** 4 byte file header */
    protected static final int HEADER = (0xAB3D<<16) | STORE_VERSION;


    protected static final int I_UPDATE = 1;
    protected static final int I_INSERT = 3;
    protected static final int I_DELETE = 2;
    protected static final int I_PREALLOC = 4;
    protected static final int I_SKIP_SINGLE_BYTE = 6;
    protected static final int I_SKIP_MULTI_BYTE = 7;

    protected static final int I_TX_VALID = 8;
    protected static final int I_TX_ROLLBACK = 9;

    protected static final long headerSize = 16;

    protected static final StoreAppend[] STORE_APPENDS_ZERO_ARRAY = new StoreAppend[0];


    protected Volume vol;

    /**
     * In memory table which maps recids into their offsets. Positive values are offsets.
     * Zero value indicates on-used records
     * Negative values are:
     * <pre>
     *     -1 - records was deleted, return null
     *     -2 - record has zero size
     *     -3 - null record, return null
     * </pre>
     *
     *
     */
    //TODO this is in-memory, move to temporary file or something
    protected Volume indexTable;

    //guarded by StructuralLock
    protected long eof = 0;
    protected final AtomicLong highestRecid = new AtomicLong(0);
    protected final boolean tx;

    protected final LongLongMap[] modified;

    protected final ScheduledExecutorService compactionExecutor;

    protected final Set<StoreAppend> snapshots;

    protected final boolean isSnapshot;

    protected StoreAppend(String fileName,
                          Volume.VolumeFactory volumeFactory,
                          Cache cache,
                          int lockScale,
                          int lockingStrategy,
                          boolean checksum,
                          boolean compress,
                          byte[] password,
                          boolean readonly,
                          boolean snapshotEnable,
                          boolean txDisabled,
                          ScheduledExecutorService compactionExecutor
                    ) {
        super(fileName, volumeFactory, cache, lockScale,lockingStrategy, checksum, compress, password, readonly, snapshotEnable);
        this.tx = !txDisabled;
        if(tx){
            modified = new LongLongMap[this.lockScale];
            for(int i=0;i<modified.length;i++){
                modified[i] = new LongLongMap();
            }
        }else{
            modified = null;
        }
        this.compactionExecutor = compactionExecutor;
        this.snapshots = Collections.synchronizedSet(new HashSet<StoreAppend>());
        this.isSnapshot = false;
    }

    public StoreAppend(String fileName) {
        this(fileName,
                fileName==null? CC.DEFAULT_MEMORY_VOLUME_FACTORY : CC.DEFAULT_FILE_VOLUME_FACTORY,
                null,
                CC.DEFAULT_LOCK_SCALE,
                0,
                false,
                false,
                null,
                false,
                false,
                false,
                null
        );
    }

    /** protected constructor used to take snapshots*/
    protected StoreAppend(StoreAppend host, LongLongMap[] uncommitedData){
        super(null, null,null,
                host.lockScale,
                Store.LOCKING_STRATEGY_NOLOCK,
                host.checksum,
                host.compress,
                null, //TODO password on snapshot
                true, //snapshot is readonly
                false);

        indexTable = host.indexTable;
        vol = host.vol;

        //replace locks, so reads on snapshots are not performed while host is updated
        for(int i=0;i<locks.length;i++){
            locks[i] = host.locks[i];
        }

        tx = true;
        modified = new LongLongMap[this.lockScale];
        if(uncommitedData==null){
            for(int i=0;i<modified.length;i++) {
                modified[i] = new LongLongMap();
            }
        }else{
            for(int i=0;i<modified.length;i++) {
                Lock lock = locks[i].writeLock();
                lock.lock();
                try {
                    modified[i] = uncommitedData[i].clone();
                }finally {
                    lock.unlock();
                }
            }
        }

        this.compactionExecutor = null;
        this.snapshots = host.snapshots;
        this.isSnapshot = true;
        host.snapshots.add(StoreAppend.this);
    }

    @Override
    public void init() {
        super.init();
        structuralLock.lock();
        try {
            vol = volumeFactory.makeVolume(fileName, readonly);
            indexTable = new Volume.ByteArrayVol(CC.VOLUME_PAGE_SHIFT);
            if (!readonly)
                vol.ensureAvailable(headerSize);
            eof = headerSize;
            for (int i = 0; i <= RECID_LAST_RESERVED; i++) {
                indexTable.ensureAvailable(i * 8);
                indexTable.putLong(i * 8, -3);
            }

            if (vol.isEmpty()) {
                initCreate();
            } else {
                initOpen();
            }
        }finally {
            structuralLock.unlock();
        }
    }

    protected void initCreate() {
        highestRecid.set(RECID_LAST_RESERVED);
        vol.putInt(0,HEADER);
        long feat = makeFeaturesBitmap();
        vol.putLong(HEAD_FEATURES, feat);
        vol.sync();
    }

    protected void initOpen() {
        checkFeaturesBitmap(vol.getLong(HEAD_FEATURES));

        //replay log
        long pos = headerSize;
        final long volumeSize = vol.length();
        long lastValidPos= pos;
        long highestRecid2 = RECID_LAST_RESERVED;
        LongLongMap commitData = tx?new LongLongMap():null;

        try{

            while(true) {
                lastValidPos = pos;
                if(pos>=volumeSize)
                    break;
                final long instPos = pos;
                final int inst = vol.getUnsignedByte(pos++);

                if (inst == I_INSERT || inst == I_UPDATE) {

                    long recid = vol.getPackedLong(pos);
                    pos += recid>>>60;
                    recid =  longParityGet(recid & DataIO.PACK_LONG_RESULT_MASK);

                    highestRecid2 = Math.max(highestRecid2, recid);

                    commitData.put(recid, instPos);

                    //skip rest of the record
                    long size = vol.getPackedLong(pos);
                    long dataLen = longParityGet(size & DataIO.PACK_LONG_RESULT_MASK) - 1;
                    dataLen = Math.max(0,dataLen);
                    pos = pos + (size>>>60) + dataLen;
                } else if (inst == I_DELETE) {
                    long recid = vol.getPackedLong(pos);
                    pos += recid>>>60;
                    recid =  longParityGet(recid & DataIO.PACK_LONG_RESULT_MASK);

                    highestRecid2 = Math.max(highestRecid2, recid);

                    commitData.put(recid, -1);
                } else if (inst == I_DELETE) {
                    long recid = vol.getPackedLong(pos);
                    pos += recid>>>60;
                    recid =  longParityGet(recid & DataIO.PACK_LONG_RESULT_MASK);
                    highestRecid2 = Math.max(highestRecid2, recid);
                    commitData.put(recid,-2);

                } else if (inst == I_SKIP_SINGLE_BYTE) {
                    //do nothing, just skip single byte
                } else if (inst == I_SKIP_MULTI_BYTE) {
                    //read size and skip it
                    //skip rest of the record
                    long size = vol.getPackedLong(pos);
                    pos += (size>>>60) + longParityGet(size & DataIO.PACK_LONG_RESULT_MASK);
                } else if (inst == I_TX_VALID) {
                    if (tx){
                        //apply changes from commitData to indexTable
                        for(int i=0;i<commitData.table.length;i+=2){
                            long recidOffset = commitData.table[i]*8;
                            if(recidOffset==0)
                                continue;
                            indexTable.ensureAvailable(recidOffset + 8);
                            indexTable.putLong(recidOffset, commitData.table[i+1]);
                        }
                        commitData.clear();
                    }
                } else if (inst == I_TX_ROLLBACK) {
                    if (tx) {
                        commitData.clear();
                    }
                } else if (inst == 0) {
                    //rollback last changes if that is necessary
                    if (tx) {
                        //rollback changes in index table since last valid tx
                        commitData.clear();
                    }

                    break;
                } else {
                    //TODO log here?
                    LOG.warning("Unknown instruction " + inst);
                    break;
                }
            }
        }catch (RuntimeException e){
            //log replay finished
            //TODO log here?
            LOG.log(Level.WARNING, "Log replay finished",e);
            if(tx) {
                //rollback changes in index table since last valid tx
                commitData.clear();
            }

        }
        eof = lastValidPos;

        highestRecid.set(highestRecid2);
    }

    protected long alloc(int headSize, int totalSize){
        structuralLock.lock();
        try{
            while(eof/StoreDirect.PAGE_SIZE != (eof+headSize)/StoreDirect.PAGE_SIZE){
                //add skip instructions
                vol.ensureAvailable(eof+1);
                vol.putUnsignedByte(eof++, I_SKIP_SINGLE_BYTE);
            }
            long ret = eof;
            eof+=totalSize;
            return ret;
        }finally {
            structuralLock.unlock();
        }
    }

    @Override
    protected <A> A get2(long recid, Serializer<A> serializer) {
        if(CC.ASSERT)
            assertReadLocked(recid);

        long offset = modified[lockPos(recid)].get(recid);
        if(offset==0) {
            try {
                offset = indexTable.getLong(recid * 8);
            } catch (ArrayIndexOutOfBoundsException e) {
                //TODO this code should be aware if indexTable internals?
                throw new DBException.EngineGetVoid();
            }
        }

        if(offset==-3||offset==-1) //null, preallocated or deleted
            return null;
        if(offset == 0){ //non existent
            throw new DBException.EngineGetVoid();
        }
        if(offset == -2){
            //zero size record
            return deserialize(serializer,0,new DataIO.DataInputByteArray(new byte[0]));
        }

        final long packedRecidSize = DataIO.packLongSize(longParitySet(recid));

        if(CC.ASSERT){
            int instruction = vol.getUnsignedByte(offset);

            if(instruction!= I_UPDATE && instruction!= I_INSERT)
                throw new DBException.DataCorruption("wrong instruction "+instruction);

            long recid2 = vol.getPackedLong(offset+1);

            if(packedRecidSize!=recid2>>>60)
                throw new DBException.DataCorruption("inconsistent recid len");

            recid2 = longParityGet(recid2&DataIO.PACK_LONG_RESULT_MASK);
            if(recid!=recid2)
                throw new DBException.DataCorruption("recid does not match");
        }

        offset += 1 + //instruction size
                packedRecidSize; // recid size


        //read size
        long size = vol.getPackedLong(offset);
        offset+=size>>>60;
        size = longParityGet(size & DataIO.PACK_LONG_RESULT_MASK);

        size -= 1; //normalize size
        if(CC.ASSERT && size<=0)
            throw new DBException.DataCorruption("wrong size");

        DataInput input = vol.getDataInputOverlap(offset, (int) size);
        return deserialize(serializer, (int) size, input);
    }

    @Override
    protected void update2(long recid, DataIO.DataOutputByteArray out) {
        insertOrUpdate(recid, out, false);
    }

    private void insertOrUpdate(long recid, DataIO.DataOutputByteArray out, boolean isInsert) {
        if(CC.ASSERT)
            assertWriteLocked(lockPos(recid));

        //TODO assert indexTable state, record should already exist/not exist

        final int realSize = out==null ? 0: out.pos;
        final int shiftedSize = out==null ?0 : realSize+1;  //one additional state to indicate null
        final int headSize = 1 +  //instruction
                DataIO.packLongSize(longParitySet(recid)) + //recid
                DataIO.packLongSize(longParitySet(shiftedSize));   //length

        long offset = alloc(headSize, headSize+realSize);
        final long origOffset = offset;
        //ensure available worst case scenario
        vol.ensureAvailable(offset+headSize+realSize);
        //instruction
        vol.putUnsignedByte(offset, isInsert ? I_INSERT : I_UPDATE);
        offset++;
        //recid
        offset+=vol.putPackedLong(offset,longParitySet(recid));
        //size
        offset+=vol.putPackedLong(offset,longParitySet(shiftedSize));

        if(realSize!=0)
            vol.putDataOverlap(offset, out.buf,0,out.pos);

        // -3 is null record
        // -2 is zero size record
        indexTablePut(recid, out==null? -3 : (realSize==0) ? -2:origOffset);
    }

    @Override
    protected <A> void delete2(long recid, Serializer<A> serializer) {
        if(CC.ASSERT)
            assertWriteLocked(lockPos(recid));

        final int headSize = 1 + DataIO.packLongSize(longParitySet(recid));
        long offset = alloc(headSize,headSize);
        vol.ensureAvailable(offset + headSize);

        vol.putUnsignedByte(offset, I_DELETE); //delete instruction
        offset++;
        vol.putPackedLong(offset,longParitySet(recid));

        indexTablePut(recid, -1); // -1 is deleted record
    }

    @Override
    public long getCurrSize() {
        return 0;
    }

    @Override
    public long getFreeSize() {
        return 0;
    }

    @Override
    public long preallocate() {
        long recid = highestRecid.incrementAndGet();
        Lock lock = locks[lockPos(recid)].writeLock();
        lock.lock();
        try{
            final int headSize = 1 + DataIO.packLongSize(longParitySet(recid));
            long offset = alloc(headSize,headSize);
            vol.ensureAvailable(offset + headSize);

            vol.putUnsignedByte(offset, I_PREALLOC);
            offset++;
            vol.putPackedLong(offset, longParitySet(recid));

            indexTablePut(recid,-3);
        }finally {
            lock.unlock();
        }

        return recid;
    }

    protected void indexTablePut(long recid, long offset) {
        if(tx){
            modified[lockPos(recid)].put(recid,offset);
        }else {
            indexTable.ensureAvailable(recid*8+8);
            indexTable.putLong(recid * 8, offset);
        }
    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        DataIO.DataOutputByteArray out = serialize(value,serializer);
        long recid = highestRecid.incrementAndGet();
        int lockPos = lockPos(recid);
        Cache cache = caches==null ? null : caches[lockPos] ;
        Lock lock = locks[lockPos].writeLock();
        lock.lock();
        try{
            if(cache!=null) {
                cache.put(recid, value);
            }

            insertOrUpdate(recid,out,true);
        }finally {
            lock.unlock();
        }

        return recid;
    }

    @Override
    public void close() {
        if(closed)
            return;
        commitLock.lock();
        try {
            if(closed)
                return;

            if(isSnapshot){
                snapshots.remove(this);
                return;
            }

            vol.sync();
            vol.close();
            indexTable.close();

            if(caches!=null){
                for(Cache c:caches){
                    c.close();
                }
                Arrays.fill(caches,null);
            }
            closed = true;
        }finally{
            commitLock.unlock();
        }
    }

    @Override
    public void commit() {
        if(isSnapshot)
            return;

        if(!tx){
            vol.sync();
            return;
        }

        commitLock.lock();
        try{
            StoreAppend[] snaps = snapshots==null ?
                    STORE_APPENDS_ZERO_ARRAY :
                    snapshots.toArray(STORE_APPENDS_ZERO_ARRAY);

            for(int i=0;i<locks.length;i++) {
                Lock lock = locks[i].writeLock();
                lock.lock();
                try {
                    long[] m = modified[i].table;
                    for(int j=0;j<m.length;j+=2){
                        long recid = m[j];
                        long recidOffset = recid*8;
                        if(recidOffset==0)
                            continue;
                        indexTable.ensureAvailable(recidOffset + 8);
                        long oldVal = indexTable.getLong(recidOffset);
                        indexTable.putLong(recidOffset,m[j+1]);

                        for(StoreAppend snap:snaps){
                            LongLongMap m2 = snap.modified[i];
                            if(m2.get(recid)==0) {
                                m2.put(recid, oldVal);
                            }
                        }
                    }
                    modified[i].clear();
                }finally {
                    lock.unlock();
                }
            }
            long offset = alloc(1,1);
            vol.putUnsignedByte(offset,I_TX_VALID);
            vol.sync();
        }finally {
            commitLock.unlock();
        }
    }

    @Override
    public void rollback() throws UnsupportedOperationException {
        if(!tx || readonly || isSnapshot)
            throw new UnsupportedOperationException();
        commitLock.lock();
        try{
            for(int i=0;i<locks.length;i++) {
                Lock lock = locks[i].writeLock();
                lock.lock();
                try {
                    modified[i].clear();
                }finally {
                    lock.unlock();
                }
            }
            long offset = alloc(1,1);
            vol.putUnsignedByte(offset,I_TX_ROLLBACK);
            vol.sync();
        }finally {
            commitLock.unlock();
        }
    }



    @Override
    public boolean canRollback() {
        return tx;
    }

    @Override
    public boolean canSnapshot() {
        return true;
    }

    @Override
    public Engine snapshot() throws UnsupportedOperationException {
        commitLock.lock();
        try {
            return new StoreAppend(this, modified);
        }finally {
            commitLock.unlock();
        }
    }


    @Override
    public void compact() {
        if(isSnapshot)
            return;

    }
}
