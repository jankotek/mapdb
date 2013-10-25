package org.mapdb;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Store which keeps all instances on heap. It does not use serialization.
 */
public class StoreHeap extends Store implements Serializable{

    protected final static Fun.Tuple2 TOMBSTONE = Fun.t2(null,null);


    /** All commited records in store */
    protected final ConcurrentNavigableMap<Long,Fun.Tuple2> records
            = new ConcurrentSkipListMap<Long, Fun.Tuple2>();

    /** All not-yet commited records in store */
    protected final ConcurrentNavigableMap<Long,Fun.Tuple2> rollback
            = new ConcurrentSkipListMap<Long, Fun.Tuple2>();


    /** Queue of deleted recids, those are reused for new records */
    protected final Queue<Long> freeRecids = new ConcurrentLinkedQueue<Long>();

    /** Maximal returned recid, incremented if there are no free recids*/
    protected final AtomicLong maxRecid = new AtomicLong(LAST_RESERVED_RECID);

    public StoreHeap(){
        super(false,false,null);
        for(long recid=1;recid<=LAST_RESERVED_RECID;recid++){
            records.put(recid, Fun.t2(null, (Serializer)null));
        }
    }

    @Override
    public long preallocate() {
        final Lock lock  = locks[new Random().nextInt(locks.length)].writeLock();
        lock.lock();
        try{
            Long recid = freeRecids.poll();
            if(recid==null) recid = maxRecid.incrementAndGet();
            return recid;
        }finally{
            lock.unlock();
        }
    }

    @Override
    public void preallocate(long[] recids) {
        final Lock lock  = locks[new Random().nextInt(locks.length)].writeLock();
        lock.lock();
        try{
            for(int i=0;i<recids.length;i++){
                Long recid = freeRecids.poll();
                if(recid==null) recid = maxRecid.incrementAndGet();
                recids[i] = recid;
            }
        }finally{
            lock.unlock();
        }
    }
    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        assert(value!=null);
        final Lock lock  = locks[new Random().nextInt(locks.length)].writeLock();
        lock.lock();
        try{
            Long recid = freeRecids.poll();
            if(recid==null) recid = maxRecid.incrementAndGet();
            records.put(recid, Fun.<Object, Serializer>t2(value,serializer));
            rollback.put(recid, Fun.t2(TOMBSTONE,serializer ));
            assert(recid>0);
            return recid;
        }finally{
            lock.unlock();
        }
    }

    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        assert(recid>0);
        final Lock lock  = locks[Utils.lockPos(recid)].readLock();
        lock.lock();
        try{
            //get from commited records
            Fun.Tuple2 t = records.get(recid);
            return t!=null? (A) t.a : null;
        }finally{
            lock.unlock();
        }
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        assert(recid>0);
        assert(value!=null);
        assert(serializer!=null);
        assert(recid>0);
        final Lock lock  = locks[Utils.lockPos(recid)].writeLock();
        lock.lock();
        try{
            Fun.Tuple2 old = records.put(recid, Fun.<Object, Serializer>t2(value,serializer));
            if(old!=null) //TODO null if record was preallocated
                rollback.putIfAbsent(recid,old);
        }finally{
            lock.unlock();
        }
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        assert(recid>0);
        assert(expectedOldValue!=null && newValue!=null);
        final Lock lock  = locks[Utils.lockPos(recid)].writeLock();
        lock.lock();
        try{
            Fun.Tuple2 old = Fun.t2(expectedOldValue, serializer);
            boolean ret =  records.replace(recid, old, Fun.t2(newValue, serializer));
            if(ret) rollback.putIfAbsent(recid,old);
            return ret;
        }finally{
            lock.unlock();
        }
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        assert(recid>0);
        final Lock lock  = locks[Utils.lockPos(recid)].writeLock();
        lock.lock();
        try{
            Fun.Tuple2 t2 = records.remove(recid);
            if(t2!=null) rollback.putIfAbsent(recid,t2);
            freeRecids.add(recid);
        }finally{
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lockAllWrite();
        try{
            records.clear();
            freeRecids.clear();
            rollback.clear();
        }finally{
            unlockAllWrite();
        }
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void commit() {
        lockAllWrite();
        try{
            rollback.clear();
        }finally{
            unlockAllWrite();
        }
    }

    @Override
    public void rollback() throws UnsupportedOperationException {
        lockAllWrite();
        try{
            //put all stuff from `rollback` into `records`
            for(Map.Entry<Long,Fun.Tuple2> e:rollback.entrySet()){
                Long recid = e.getKey();
                Fun.Tuple2 val = e.getValue();
                if(val == TOMBSTONE) records.remove(recid);
                else records.put(recid, val);
            }
            rollback.clear();
        }finally{
            unlockAllWrite();
        }
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void clearCache() {
    }

    @Override
    public void compact() {
    }

    @Override
    public boolean canRollback(){
        return true;
    }


    @Override
    public long getMaxRecid() {
        return maxRecid.get();
    }

    @Override
    public ByteBuffer getRaw(long recid) {
        Fun.Tuple2 t = records.get(recid);
        if(t==null||t.a == null) return null;
        return ByteBuffer.wrap(serialize(t.a, (Serializer<Object>) t.b).copyBytes());
    }

    @Override
    public Iterator<Long> getFreeRecids() {
        return Collections.unmodifiableCollection(freeRecids).iterator();
    }

    @Override
    public void updateRaw(long recid, ByteBuffer data) {
        throw new UnsupportedOperationException("can not put raw data into StoreHeap");
    }

    @Override
    public long getSizeLimit() {
        return 0;
    }

    @Override
    public long getCurrSize() {
        return records.size();
    }

    @Override
    public long getFreeSize() {
        return 0;
    }

    @Override
    public String calculateStatistics() {
        return null;
    }
}
