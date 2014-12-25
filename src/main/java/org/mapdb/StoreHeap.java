package org.mapdb;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 * Store which keeps all instances on heap. It does not use serialization.
 */

//TODO thread safe
public class StoreHeap extends Store{

    protected final LongConcurrentHashMap data;
    protected final LongConcurrentHashMap uncommited;
    protected final LongConcurrentHashMap deleted;

    protected final AtomicLong recids = new AtomicLong(Engine.RECID_FIRST);

    protected static final Object TOMBSTONE = new Object();
    protected static final Object NULL = new Object();



    public StoreHeap(boolean transactionsDisabled) {
        super(null,null,false,false,null,false);
        this.data = new LongConcurrentHashMap();
        this.uncommited = transactionsDisabled? null : new LongConcurrentHashMap();
        this.deleted = new LongConcurrentHashMap();

        //predefined recids
        for(long recid=1;recid<RECID_FIRST;recid++){
            data.put(recid,NULL);
        }
    }

    protected StoreHeap(LongConcurrentHashMap m) {
        super(null,null,false,false,null,false);
        this.data = m;
        this.uncommited = null;
        this.deleted = null;
    }

    protected Object unswapNull(Object o){
        if(o==NULL)
            return null;
        return o;
    }

    protected <A> A swapNull(A o){
        if(o==null)
            return (A) NULL;
        return o;
    }

    @Override
    protected <A> A get2(long recid, Serializer<A> serializer) {
        Object o = data.get(recid);
        if(o==null)
            throw new DBException.EngineGetVoid();
        return (A) unswapNull(o);
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();

        value = swapNull(value);
        final Lock lock = locks[lockPos(recid)].writeLock();
        lock.lock();
        try{
            Object old = data.put(recid,value);
            if(old!=null && uncommited!=null)
                uncommited.putIfAbsent(recid,old);
        }finally {
            lock.unlock();
        }
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();

        expectedOldValue = swapNull(expectedOldValue);
        newValue = swapNull(newValue);
        final Lock lock = locks[lockPos(recid)].writeLock();
        lock.lock();
        try{
            boolean r = data.replace(recid,expectedOldValue,newValue);
            if(r && uncommited!=null)
                uncommited.putIfAbsent(recid,expectedOldValue);
            return r;
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
        deleted.put(recid,TOMBSTONE);
        Object old = data.put(recid,NULL);
        if(old!=null && uncommited!=null)
            uncommited.putIfAbsent(recid,old);
    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();

        value = swapNull(value);
        long recid = recids.getAndIncrement();
        data.put(recid, value);
        if(uncommited!=null)
            uncommited.put(recid,TOMBSTONE);
        return recid;
    }

    @Override
    public long preallocate() {
        long recid = recids.getAndIncrement();
        data.put(recid,NULL);
        if(uncommited!=null)
            uncommited.put(recid,TOMBSTONE);
        return recid;
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
        if(uncommited==null)
            throw new UnsupportedOperationException();
        LongMap.LongMapIterator i = uncommited.longMapIterator();
        while(i.moveToNext()) {
            long recid = i.key();
            Object val = i.value();
            if (val == TOMBSTONE){
                data.remove(recid);
                deleted.remove(recid);
            }else {
                data.put(recid, val);
            }
            i.remove();
        }
    }

    @Override
    public boolean canRollback() {
        return uncommited!=null;
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
        LongMap.LongMapIterator i = deleted.longMapIterator();
        while (i.moveToNext()) {
            data.remove(i.key(),NULL);
            i.remove();
        }
    }
}
