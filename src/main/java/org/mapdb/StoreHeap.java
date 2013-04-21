package org.mapdb;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Queue;
import java.io.Serializable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Store which keeps all instances on heap. It does not use serialization.
 */
public class StoreHeap implements Store, Serializable{

    protected final ConcurrentNavigableMap<Long,Fun.Tuple2> records
            = new ConcurrentSkipListMap<Long, Fun.Tuple2>();

    protected final Queue<Long> freeRecids = new ConcurrentLinkedQueue<Long>();

    protected final AtomicLong maxRecid = new AtomicLong(LAST_RESERVED_RECID);

    public StoreHeap(){
        for(long recid=1;recid<=LAST_RESERVED_RECID;recid++){
            records.put(recid, Fun.t2(null, (Serializer)null));
        }
    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        Long recid = freeRecids.poll();
        if(recid==null) recid = maxRecid.incrementAndGet();
        records.put(recid, Fun.<Object, Serializer>t2(value,serializer));
        return recid;
    }

    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        Fun.Tuple2 t = records.get(recid);
        return t!=null? (A) t.a : null;
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        records.put(recid, Fun.<Object, Serializer>t2(value,serializer));
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        return records.replace(recid, Fun.t2(expectedOldValue, serializer), Fun.t2(newValue, serializer));
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        records.remove(recid);
        freeRecids.add(recid);
    }

    @Override
    public void close() {
        records.clear();
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void commit() {
    }

    @Override
    public void rollback() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("rollback not supported");
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
    public long getMaxRecid() {
        return maxRecid.get();
    }

    @Override
    public ByteBuffer getRaw(long recid) {
        Fun.Tuple2 t = records.get(recid);
        if(t==null||t.a == null) return null;
        return ByteBuffer.wrap(Utils.serializer((Serializer<Object>) t.b, t.a).copyBytes());
    }

    @Override
    public Iterator<Long> getFreeRecids() {
        return Collections.unmodifiableCollection(freeRecids).iterator();
    }

    @Override
    public void updateRaw(long recid, ByteBuffer data) {
        throw new UnsupportedOperationException("can not put raw data into StoreHeap");
    }
}
