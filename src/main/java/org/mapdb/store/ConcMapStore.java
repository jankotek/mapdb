package org.mapdb.store;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DBException;
import org.mapdb.ser.Serializer;
import org.mapdb.util.MonoRef;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class ConcMapStore implements Store{

    private static final Object PREALLOCATED = 1293091239012L;

    private final ConcurrentNavigableMap<Long, Object> m;
    private final ConcurrentLinkedQueue<Long> freeRecids = new ConcurrentLinkedQueue<>();

    private final AtomicLong maxRecid = new AtomicLong(0);

    public ConcMapStore() {
        this.m = new ConcurrentSkipListMap<>();
    }

    private long allocateRecid(){
        Long recid = freeRecids.poll();
        if(recid!=null)
            return recid;
        return maxRecid.incrementAndGet();
    }

    @Override
    public long preallocate() {
        long recid = allocateRecid();
        Object o = m.put(recid, PREALLOCATED);
        assert(o==null);
        return recid;
    }

    @Override
    public <R> void preallocatePut(long recid, @NotNull Serializer<R> serializer, @NotNull R record) {
        boolean updated = m.replace(recid, PREALLOCATED, record);
        if(!updated)
            throw new DBException.RecordNotPreallocated();
    }

    @Override
    public <R> @NotNull long put(@NotNull R record, @NotNull Serializer<R> serializer) {
        long recid = allocateRecid();
        Object old = m.putIfAbsent(recid, record);
        assert(old==null);
        return recid;
    }

    @Override
    public <R> void update(long recid, @NotNull Serializer<R> serializer, @NotNull R updatedRecord) {
        Object old = m.computeIfPresent(recid, (recid2, oldVal) ->{
            if(oldVal == PREALLOCATED)
                throw new DBException.PreallocRecordAccess();
            return updatedRecord;
        });
        if(old==null)
            throw new DBException.RecordNotFound();
    }

    @Override
    public <R> @NotNull R getAndUpdate(long recid, @NotNull Serializer<R> serializer, @NotNull R updatedRecord) {
        Object old = m.computeIfPresent(recid, (recid2, oldVal) ->{
            if(oldVal == PREALLOCATED)
                throw new DBException.PreallocRecordAccess();
            return updatedRecord;
        });
        if(old==null)
            throw new DBException.RecordNotFound();
        return (R) old;
    }

    @Override
    public void verify() {

    }

    @Override
    public void commit() {

    }

    @Override
    public void compact() {

    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    @Override
    public <R> @NotNull R updateAndGet(long recid, @NotNull Serializer<R> serializer, @NotNull Transform<R> t) {
        Object newVal =  m.computeIfPresent(recid, (recid2, oldVal) ->{
            if(oldVal == PREALLOCATED)
                throw new DBException.PreallocRecordAccess();

            return t.transform((R) oldVal);
        });
        if(newVal == null)
            throw new DBException.RecordNotFound();
        return (R) newVal;
    }

    @Override
    public <R> void updateAtomic(long recid, @NotNull Serializer<R> serializer, @NotNull Transform<R> r) {
        updateAndGet(recid, serializer, r);
    }

    @Override
    public <R> boolean compareAndUpdate(long recid, @NotNull Serializer<R> serializer, @NotNull R expectedOldRecord, @NotNull R updatedRecord) {
        Object ret = m.computeIfPresent(recid, (recid2, oldVal) -> {
            if (oldVal == PREALLOCATED)
                throw new DBException.PreallocRecordAccess();
            return serializer.equals(expectedOldRecord, (R) oldVal) ? updatedRecord : oldVal;
        });
        if(ret == null)
            throw new DBException.RecordNotFound();
        return ret == updatedRecord;
    }

    @Override
    public <R> boolean compareAndDelete(long recid, @NotNull Serializer<R> serializer, @NotNull R expectedOldRecord) {
        MonoRef<Boolean> deleted = new MonoRef(false);
        Object ret = m.computeIfPresent(recid, (recid2, oldVal) -> {
            if (oldVal == PREALLOCATED)
                throw new DBException.PreallocRecordAccess();
            if(!serializer.equals(expectedOldRecord, (R) oldVal)){
                return oldVal;
            }
            deleted.ref = true;
            return null;

        });
        if(ret == null && !deleted.ref)
            throw new DBException.RecordNotFound();
        return deleted.ref;
    }

    @Override
    public <R> void delete(long recid, @NotNull Serializer<R> serializer) {
        getAndDelete(recid, serializer);
    }

    @Override
    public <R> @NotNull R getAndDelete(long recid, @NotNull Serializer<R> serializer) {
        MonoRef oldVal2 = new MonoRef();
        m.computeIfPresent(recid, (rec, oldVal) -> {
            if(oldVal==PREALLOCATED)
                throw new DBException.PreallocRecordAccess();
            oldVal2.ref = oldVal;
            return null;
        });
        if(oldVal2.ref==null)
            throw new DBException.RecordNotFound();
        freeRecids.add(recid);
        return (R) oldVal2.ref;
    }

    @Override
    public <R> @NotNull R getAndUpdateAtomic(long recid, @NotNull Serializer<R> serializer, @NotNull Transform<R> t) {
        MonoRef oldVal2 = new MonoRef();
        Object newVal =  m.computeIfPresent(recid, (recid2, oldVal) ->{
            if(oldVal == PREALLOCATED)
                throw new DBException.PreallocRecordAccess();
            oldVal2.ref = oldVal;

            return t.transform((R) oldVal);
        });
        if(newVal == null)
            throw new DBException.RecordNotFound();
        return (R) oldVal2.ref;
    }

    @Override
    public <K> @NotNull K get(long recid, @NotNull Serializer<K> ser) {
        Object old = m.get(recid);
        if(old==null)
            throw new DBException.RecordNotFound();
        if(old==PREALLOCATED)
            throw new DBException.PreallocRecordAccess();
        return (K) old;
    }

    @Override
    public void close() {

    }

    @Override
    public void getAll(@NotNull GetAllCallback callback) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean isEmpty() {
        return m.isEmpty();
    }
}
