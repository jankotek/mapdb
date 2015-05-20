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

    protected static final int I_UPDATE = 1;
    protected static final int I_INSERT = 3;
    protected static final int I_DELETE = 2;
    protected static final int I_PREALLOC = 4;
    protected static final int I_SKIP_SINGLE_BYTE = 6;

    protected static final int I_TX_VALID = 8;
    protected static final int I_TX_ROLLBACK = 9;

    protected static final long headerSize = 16;

    protected static final StoreAppend[] STORE_APPENDS_ZERO_ARRAY = new StoreAppend[0];


    protected Volume vol;
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
                indexTable.putLong(i * 8, -2);
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
        //TODO header  here
        long feat = makeFeaturesBitmap();
        vol.putLong(HEAD_FEATURES,feat);
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
                final int inst = vol.getUnsignedByte(pos++);
                if (inst == I_INSERT || inst == I_UPDATE) {

                    final long recid = vol.getSixLong(pos);
                    pos += 6;

                    highestRecid2 = Math.max(highestRecid2, recid);

                    commitData.put(recid, pos - 6 - 1);

                    //skip rest of the record
                    int size = vol.getInt(pos);
                    pos = pos + 4 + size;
                } else if (inst == I_DELETE) {
                    final long recid = vol.getSixLong(pos);
                    pos += 6;

                    highestRecid2 = Math.max(highestRecid2, recid);

                    commitData.put(recid, -1);
                } else if (inst == I_DELETE) {
                    final long recid = vol.getSixLong(pos);
                    pos += 6;

                    highestRecid2 = Math.max(highestRecid2, recid);

                    commitData.put(recid,-2);
                } else if (inst == I_SKIP_SINGLE_BYTE) {
                    //do nothing, just skip single byte
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
                    //rollback last changes if thats necessary
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
        if(offset<0)
            return null; //preallocated or deleted
        if(offset == 0){ //non existent
            throw new DBException.EngineGetVoid();
        }

        if(CC.ASSERT){
            int instruction = vol.getUnsignedByte(offset);

            if(instruction!= I_UPDATE && instruction!= I_INSERT)
                throw new RuntimeException("wrong instruction "+instruction); //TODO proper error

            long recid2 = vol.getSixLong(offset+1);
            if(recid!=recid2)
                throw new RuntimeException("recid does not match"); //TODO proper error
        }

        int size = vol.getInt(offset+1+6);
        DataInput input = vol.getDataInputOverlap(offset+1+6+4,size);
        return deserialize(serializer, size, input);
    }

    @Override
    protected void update2(long recid, DataIO.DataOutputByteArray out) {
        if(CC.ASSERT)
            assertWriteLocked(lockPos(recid));
        int len = out==null? -1:out.pos;
        long plus = 1+6+4+len;
        long offset = alloc(1+6+4, (int) plus);
        vol.ensureAvailable(offset+plus);
        vol.putUnsignedByte(offset, I_UPDATE);
        vol.putSixLong(offset + 1, recid);
        vol.putInt(offset + 1 + 6, len);
        if(len!=-1)
            vol.putDataOverlap(offset+1+6+4, out.buf,0,out.pos);

        indexTablePut(recid, len != -1 ? offset : -3);
    }

    @Override
    protected <A> void delete2(long recid, Serializer<A> serializer) {
        if(CC.ASSERT)
            assertWriteLocked(lockPos(recid));

        int plus = 1+6;
        long offset = alloc(plus,plus);

        vol.ensureAvailable(offset + plus);
        vol.putUnsignedByte(offset, I_DELETE); //delete instruction
        vol.putSixLong(offset+1, recid);

        indexTablePut(recid, -1);
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
            int plus = 1+6;
            long offset = alloc(plus,plus);
            vol.ensureAvailable(offset + plus);

            vol.putUnsignedByte(offset, I_PREALLOC);
            vol.putSixLong(offset + 1, recid);

            indexTablePut(recid,-2);
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
            long plus = 1+6+4+out.pos;
            long offset = alloc(1+6+4, (int) plus);
            vol.ensureAvailable(offset+plus);
            vol.putUnsignedByte(offset, I_INSERT);
            vol.putSixLong(offset+1,recid);
            vol.putInt(offset+1+6, out.pos);
            vol.putDataOverlap(offset+1+6+4, out.buf,0,out.pos);

            indexTablePut(recid,offset);
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
