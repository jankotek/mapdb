package org.mapdb;

import java.io.DataInput;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 * append only store
 */
public class StoreAppend extends Store {

    protected Volume vol;
    protected Volume indexTable;
    protected final AtomicLong eof = new AtomicLong(0);
    protected final AtomicLong highestRecid = new AtomicLong(0);

    protected StoreAppend(String fileName,
                          Fun.Function1<Volume, String> volumeFactory,
                          boolean checksum,
                          boolean compress,
                          byte[] password,
                          boolean readonly
                    ) {
        super(fileName, volumeFactory, checksum, compress, password, readonly);
    }

    public StoreAppend(String fileName) {
        this(fileName,
                fileName==null? Volume.memoryFactory() : Volume.fileFactory(),
                false,
                false,
                null,
                false);
    }

    @Override
    public void init() {
        super.init();
        vol  = volumeFactory.run(fileName);
        indexTable = new Volume.ByteArrayVol(CC.VOLUME_PAGE_SHIFT);
        for(int i=0;i<RECID_LAST_RESERVED;i++){
            indexTable.ensureAvailable(i*8);
            indexTable.putLong(i*8, -2);
        }
        highestRecid.set(RECID_LAST_RESERVED);
    }

    @Override
    protected <A> A get2(long recid, Serializer<A> serializer) {
        if(CC.PARANOID)
            assertReadLocked(recid);

        long offset = indexTable.getLong(recid*8);
        if(offset<0)
            return null; //preallocated or deleted

        if(CC.PARANOID){
            int instruction = vol.getUnsignedByte(offset);

            if(instruction!=1 && instruction!=3)
                throw new RuntimeException("wrong instruction"); //TODO proper error

            long recid2 = vol.getSixLong(offset+1);
            if(recid!=recid2)
                throw new RuntimeException("recid does not match"); //TODO proper error
        }

        int size = vol.getInt(offset+1+6);
        DataInput input = vol.getDataInput(offset+1+6+4,size);
        return deserialize(serializer, size, input);
    }

    @Override
    protected void update2(long recid, DataIO.DataOutputByteArray out) {
        if(CC.PARANOID)
            assertWriteLocked(recid);
        int len = out==null? 0:out.pos; //TODO null has different contract
        long plus = 1+6+4+len;
        long offset = eof.getAndAdd(plus);
        vol.ensureAvailable(offset+plus);
        vol.putUnsignedByte(offset, 1); //update instruction
        vol.putSixLong(offset+1,recid);
        vol.putInt(offset+1+6, len);
        if(len!=0)
            vol.putData(offset+1+6+4, out.buf,0,out.pos);

        indexTable.putLong(recid*8, offset);
    }

    @Override
    protected <A> void delete2(long recid, Serializer<A> serializer) {
        if(CC.PARANOID)
            assertWriteLocked(recid);

        long plus = 1+6;
        long offset = eof.getAndAdd(plus);

        vol.ensureAvailable(offset+plus);
        vol.putUnsignedByte(offset,2); //delete instruction
        vol.putSixLong(offset+1, recid);

        indexTable.ensureAvailable(recid*8 +8);
        indexTable.putLong(recid*8, -1);
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
            long plus = 1+6;
            long offset = eof.getAndAdd(plus);
            vol.ensureAvailable(offset+plus);

            vol.putUnsignedByte(offset, 4); //preallocate instruction
            vol.putSixLong(offset + 1, recid);
            indexTable.ensureAvailable(recid*8+8);
            indexTable.putLong(recid*8, -2);
        }finally {
            lock.unlock();
        }

        return recid;
    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        DataIO.DataOutputByteArray out = serialize(value,serializer);
        long recid = highestRecid.incrementAndGet();
        Lock lock = locks[lockPos(recid)].writeLock();
        lock.lock();
        try{
            long plus = 1+6+4+out.pos;
            long offset = eof.getAndAdd(plus);
            vol.ensureAvailable(offset+plus);
            vol.putUnsignedByte(offset, 3); //insert instruction
            vol.putSixLong(offset+1,recid);
            vol.putInt(offset+1+6, out.pos);
            vol.putData(offset+1+6+4, out.buf,0,out.pos);
            indexTable.ensureAvailable(recid*8+8);
            indexTable.putLong(recid*8, offset);
        }finally {
            lock.unlock();
        }

        return recid;
    }

    @Override
    public void close() {
        commitLock.lock();
        try {
            vol.close();
            indexTable.close();
        }finally{
            commitLock.unlock();
        }
    }

    @Override
    public void commit() {

    }

    @Override
    public void rollback() throws UnsupportedOperationException {

    }

    @Override
    public boolean canRollback() {
        return false;
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
    public void clearCache() {

    }

    @Override
    public void compact() {

    }
}
