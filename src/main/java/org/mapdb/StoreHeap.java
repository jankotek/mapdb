package org.mapdb;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 * Store which keeps all instances on heap. It does not use serialization.
 */

//TODO thread safe
public class StoreHeap extends Store{

    protected final boolean transactionsDisabled;

    protected final LongConcurrentHashMap data;
    protected final LongConcurrentHashMap uncommited;

    protected final AtomicLong recids = new AtomicLong(Engine.RECID_FIRST);

    protected static final Object TOMBSTONE = new Object();

    public StoreHeap(boolean transactionsDisabled) {
        super(null,null,false,false,null,false);
        this.transactionsDisabled = transactionsDisabled;
        this.data = new LongConcurrentHashMap();
        this.uncommited = transactionsDisabled? null : new LongConcurrentHashMap();
    }

    protected StoreHeap(LongConcurrentHashMap m) {
        super(null,null,false,false,null,false);
        this.transactionsDisabled = true;
        this.data = m;
        this.uncommited = null;
    }


    @Override
    protected <A> A get2(long recid, Serializer<A> serializer) {
        return (A) data.get(recid);
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        final Lock lock = locks[lockPos(recid)].writeLock();
        lock.lock();
        try{
            Object old = data.put(recid,value);
            if(old!=null)
                uncommited.putIfAbsent(recid,old);
        }finally {
            lock.unlock();
        }
    }

    @Override
    protected void update2(long recid, DataIO.DataOutputByteArray out) {
        throw new IllegalAccessError();
    }

    @Override
    protected <A> void delete2(long recid, Serializer<A> serializer) {
        Object old = data.remove(recid);
        if(old!=null)
            uncommited.putIfAbsent(recid,old);
    }

    @Override
    public long getCurrSize() {
        return -1;
    }

    @Override
    public long getFreeSize() {
        return -1;
    }

    @Override
    public long preallocate() {
        return recids.getAndIncrement();
    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        long recid = recids.getAndIncrement();
        data.put(recid, value);
        uncommited.put(recid,TOMBSTONE);
        return recid;
    }

    @Override
    public void close() {
        data.clear();
        if(uncommited!=null)
            uncommited.clear();
    }

    @Override
    public void commit() {
        if(uncommited!=null)
            uncommited.clear();
    }

    @Override
    public void rollback() throws UnsupportedOperationException {
        LongMap.LongMapIterator i = uncommited.longMapIterator();
        while(i.moveToNext()) {
            Object val = i.value();
            if (val == TOMBSTONE){
                data.remove(i.key());
            }else {
                data.put(i.key(), val);
            }
            i.remove();
        }
    }

    @Override
    public boolean canRollback() {
        return !transactionsDisabled;
    }

    @Override
    public boolean canSnapshot() {
        return true;
    }

    @Override
    public Engine snapshot() throws UnsupportedOperationException {
        LongConcurrentHashMap m = new LongConcurrentHashMap();
        LongMap.LongMapIterator i = m.longMapIterator();
        while(i.moveToNext()){
            m.put(i.key(),i.value());
        }

        return new EngineWrapper.ReadOnlyEngine(new StoreHeap(m));
    }

    @Override
    public void clearCache() {
    }

    @Override
    public void compact() {
    }
}
