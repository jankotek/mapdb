package org.mapdb;

import java.io.IOError;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by jan on 11/8/14.
 */
public abstract class Store implements Engine {

    protected final ReentrantLock structuralLock;

    protected final ReentrantReadWriteLock[] locks;


    protected volatile boolean closed = false;
    protected final boolean readonly;

    protected final String fileName;
    protected Fun.Function1<Volume, String> volumeFactory;
    protected boolean checksum;
    protected boolean compress;
    protected boolean encrypt;
    protected final EncryptionXTEA encryptionXTEA;
    protected final ThreadLocal<CompressLZF> LZF;


    protected Store(
            String fileName,
            Fun.Function1<Volume, String> volumeFactory,
            boolean checksum,
            boolean compress,
            byte[] password,
            boolean readonly) {
        this.fileName = fileName;
        this.volumeFactory = volumeFactory;
        structuralLock = new ReentrantLock(CC.FAIR_LOCKS);
        locks = new ReentrantReadWriteLock[CC.CONCURRENCY];
        for(int i=0;i< locks.length;i++){
            locks[i] = new ReentrantReadWriteLock(CC.FAIR_LOCKS);
        }

        this.checksum = checksum;
        this.compress = compress;
        this.encrypt =  password!=null;
        this.readonly = readonly;
        this.encryptionXTEA = !encrypt?null:new EncryptionXTEA(password);

        this.LZF = !compress?null:new ThreadLocal<CompressLZF>() {
            @Override
            protected CompressLZF initialValue() {
                return new CompressLZF();
            }
        };
    }

    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();

        final Lock lock = locks[lockPos(recid)].readLock();
        lock.lock();
        try{
            return get2(recid,serializer);
        }finally {
            lock.unlock();
        }
    }

    protected abstract <A> A get2(long recid, Serializer<A> serializer);

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();

        //serialize outside lock
        DataIO.DataOutputByteArray out = serialize(value, serializer);

        final Lock lock = locks[lockPos(recid)].writeLock();
        lock.lock();
        try{
            update2(recid,out);
        }finally {
            lock.unlock();
        }
    }

    protected <A> DataIO.DataOutputByteArray serialize(A value, Serializer<A> serializer) {
        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        try {
            serializer.serialize(out,value);
        } catch (IOException e) {
            throw new IOError(e);
        }
        return out;
    }

    protected abstract  void update2(long recid, DataIO.DataOutputByteArray out);

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();

        //TODO binary CAS
        final Lock lock = locks[lockPos(recid)].writeLock();
        lock.lock();
        try{
            A oldVal = get2(recid,serializer);
            if(oldVal==expectedOldValue || (oldVal!=null && serializer.equals(oldVal,expectedOldValue))){
                update2(recid,serialize(newValue,serializer));
                return true;
            }
            return false;
        }finally {
            lock.unlock();
        }
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();

        final Lock lock = locks[lockPos(recid)].writeLock();
        lock.lock();
        try{
            delete2(recid, serializer);
        }finally {
            lock.unlock();
        }
    }

    protected abstract <A> void delete2(long recid, Serializer<A> serializer);

    private static final int LOCK_MASK = CC.CONCURRENCY-1;

    protected static final int lockPos(final long recid) {
        return DataIO.longHash(recid) & LOCK_MASK;
    }

    protected void assertReadLocked(long recid) {

    }

    protected void assertWriteLocked(long recid) {
        if(!locks[lockPos(recid)].isWriteLockedByCurrentThread()){
            throw new AssertionError();
        }
    }


    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean isReadOnly() {
        return readonly;
    }

    /** traverses {@link EngineWrapper}s and returns underlying {@link Store}*/
    public static Store forDB(DB db){
        return forEngine(db.engine);
    }

    /** traverses {@link EngineWrapper}s and returns underlying {@link Store}*/
    public static Store forEngine(Engine e){
        if(e instanceof EngineWrapper)
            return forEngine(((EngineWrapper) e).getWrappedEngine());
        return (Store) e;
    }

    public abstract long getCurrSize();

    public abstract long getFreeSize();

}
