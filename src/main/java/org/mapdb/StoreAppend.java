package org.mapdb;

import java.io.DataInput;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

/**
 * append only store
 */
public class StoreAppend extends Store {

    protected static final int IUPDATE = 1;
    protected static final int IINSERT = 3;
    protected static final int IDELETE = 2;
    protected static final int IPREALLOC = 4;
    protected static final int I_SKIP_SINGLE_BYTE = 6;

    protected static final int I_TX_VALID = 8;
    protected static final int I_TX_ROLLBACK = 9;

    protected static final long headerSize = 16;


    protected Volume vol;
    protected Volume indexTable;

    //guarded by StructuralLock
    protected long eof = 0;
    protected final AtomicLong highestRecid = new AtomicLong(0);
    protected final boolean tx;

    protected final LongLongMap[] rollback;

    protected StoreAppend(String fileName,
                          Fun.Function1<Volume, String> volumeFactory,
                          Cache cache,
                          int lockScale,
                          int lockingStrategy,
                          boolean checksum,
                          boolean compress,
                          byte[] password,
                          boolean readonly,
                          boolean txDisabled
                    ) {
        super(fileName, volumeFactory, cache, lockScale,lockingStrategy, checksum, compress, password, readonly);
        this.tx = !txDisabled;
        if(tx){
            rollback = new LongLongMap[this.lockScale];
            for(int i=0;i<rollback.length;i++){
                rollback[i] = new LongLongMap();
            }
        }else{
            rollback = null;
        }
    }

    public StoreAppend(String fileName) {
        this(fileName,
                fileName==null? Volume.memoryFactory() : Volume.fileFactory(),
                null,
                CC.DEFAULT_LOCK_SCALE,
                0,
                false,
                false,
                null,
                false,
                false);
    }

    @Override
    public void init() {
        super.init();
        structuralLock.lock();
        try {
            vol = volumeFactory.run(fileName);
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
    }

    protected void initOpen() {
        //replay log
        long pos = headerSize;
        final long volumeSize = vol.length();
        long lastValidPos= pos;
        long highestRecid2 = RECID_LAST_RESERVED;
        LongLongMap rollbackData = tx?new LongLongMap():null;

        try{

            while(true) {
                lastValidPos = pos;
                if(pos>=volumeSize)
                    break;
                final int inst = vol.getUnsignedByte(pos++);
                if (inst == IINSERT || inst == IUPDATE) {

                    final long recid = vol.getSixLong(pos);
                    pos += 6;

                    highestRecid2 = Math.max(highestRecid2, recid);

                    indexTablePut2(recid, pos - 6 - 1, rollbackData);

                    //skip rest of the record
                    int size = vol.getInt(pos);
                    pos = pos + 4 + size;
                } else if (inst == IDELETE) {
                    final long recid = vol.getSixLong(pos);
                    pos += 6;

                    highestRecid2 = Math.max(highestRecid2, recid);

                    indexTablePut2(recid, -1, rollbackData);
                } else if (inst == IDELETE) {
                    final long recid = vol.getSixLong(pos);
                    pos += 6;

                    highestRecid2 = Math.max(highestRecid2, recid);

                    indexTablePut2(recid,-2, rollbackData);
                } else if (inst == I_SKIP_SINGLE_BYTE) {
                    //do nothing, just skip single byte
                } else if (inst == I_TX_VALID) {
                    if (tx)
                        rollbackData.clear();
                } else if (inst == I_TX_ROLLBACK) {
                    if (tx) {
                        indexTableRestore(rollbackData);
                    }
                } else if (inst == 0) {
                    //rollback last changes if thats necessary
                    if (tx) {
                        //rollback changes in index table since last valid tx
                        indexTableRestore(rollbackData);
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
                indexTableRestore(rollbackData);
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
        if(CC.PARANOID)
            assertReadLocked(recid);

        long offset;
        try{
            offset = indexTable.getLong(recid*8);
        }catch(ArrayIndexOutOfBoundsException e){
            //TODO this code should be aware if indexTable internals?
            throw new DBException.EngineGetVoid();
        }
        if(offset<0)
            return null; //preallocated or deleted
        if(offset == 0){ //non existent
            throw new DBException.EngineGetVoid();
        }

        if(CC.PARANOID){
            int instruction = vol.getUnsignedByte(offset);

            if(instruction!= IUPDATE && instruction!= IINSERT)
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
        if(CC.PARANOID)
            assertWriteLocked(lockPos(recid));
        int len = out==null? -1:out.pos;
        long plus = 1+6+4+len;
        long offset = alloc(1+6+4, (int) plus);
        vol.ensureAvailable(offset+plus);
        vol.putUnsignedByte(offset, IUPDATE);
        vol.putSixLong(offset+1,recid);
        vol.putInt(offset+1+6, len);
        if(len!=-1)
            vol.putDataOverlap(offset+1+6+4, out.buf,0,out.pos);

        indexTablePut(recid,len!=-1?offset:-3);
    }

    @Override
    protected <A> void delete2(long recid, Serializer<A> serializer) {
        if(CC.PARANOID)
            assertWriteLocked(lockPos(recid));

        int plus = 1+6;
        long offset = alloc(plus,plus);

        vol.ensureAvailable(offset+plus);
        vol.putUnsignedByte(offset, IDELETE); //delete instruction
        vol.putSixLong(offset+1, recid);

        indexTablePut(recid,-1);
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
            vol.ensureAvailable(offset+plus);

            vol.putUnsignedByte(offset, IPREALLOC);
            vol.putSixLong(offset + 1, recid);

            indexTablePut(recid,-2);
        }finally {
            lock.unlock();
        }

        return recid;
    }

    protected void indexTablePut(long recid, long offset) {
        indexTable.ensureAvailable(recid*8+8);
        if(tx){
            LongLongMap map = rollback[lockPos(recid)];
            if(map.get(recid)==0) {
                long oldval = indexTable.getLong(recid*8);
                if(oldval==0)
                    oldval = Long.MIN_VALUE;
                map.put(recid, oldval);
            }
        }
        indexTable.putLong(recid*8, offset);
    }

    protected void indexTablePut2(long recid, long offset, LongLongMap rollbackData) {
        indexTable.ensureAvailable(recid*8+8);
        if(tx){
            if(rollbackData.get(recid)==0) {
                long oldval = indexTable.getLong(recid*8);
                if(oldval==0)
                    oldval = Long.MIN_VALUE;
                rollbackData.put(recid, oldval);
            }
        }
        indexTable.putLong(recid*8, offset);
    }

    protected void indexTableRestore(LongLongMap rollbackData) {
        //rollback changes in index table since last valid tx
        long[] v = rollbackData.table;
        for(int i=0;i<v.length;i+=2){
            long recid = v[i];
            if(recid==0)
                continue;
            long val = v[i+1];
            if(val==Long.MIN_VALUE)
                val = 0;
            indexTable.putLong(recid*8, val);
        }
    }


    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        DataIO.DataOutputByteArray out = serialize(value,serializer);
        long recid = highestRecid.incrementAndGet();
        int lockPos = lockPos(recid);
        Cache cache = caches[lockPos];
        Lock lock = locks[lockPos].writeLock();
        lock.lock();
        try{
            cache.put(recid,value);

            long plus = 1+6+4+out.pos;
            long offset = alloc(1+6+4, (int) plus);
            vol.ensureAvailable(offset+plus);
            vol.putUnsignedByte(offset, IINSERT);
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
        commitLock.lock();
        try {
            vol.sync();
            vol.close();
            indexTable.close();

            if(caches!=null){
                for(Cache c:caches){
                    c.close();
                }
                Arrays.fill(caches,null);
            }
        }finally{
            commitLock.unlock();
        }
    }

    @Override
    public void commit() {
        if(!tx){
            vol.sync();
            return;
        }

        commitLock.lock();
        try{
            for(int i=0;i<locks.length;i++) {
                Lock lock = locks[i].writeLock();
                lock.lock();
                try {
                    rollback[i].clear();
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
        if(!tx)
            throw new UnsupportedOperationException();
        commitLock.lock();
        try{
            for(int i=0;i<locks.length;i++) {
                Lock lock = locks[i].writeLock();
                lock.lock();
                try {
                    caches[i].clear();
                    indexTableRestore(rollback[i]);
                    rollback[i].clear();
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
        return false;
    }

    @Override
    public Engine snapshot() throws UnsupportedOperationException {
        return null;
    }


    @Override
    public void compact() {

    }
}
