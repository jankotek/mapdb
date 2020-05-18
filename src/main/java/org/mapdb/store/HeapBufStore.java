package org.mapdb.store;

import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DBException;
import org.mapdb.io.DataInput2ByteArray;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;

public class HeapBufStore implements Store {

    protected static final byte[] PREALLOC_RECORD = new byte[]{1,2,4};

    //-newRWLOCK

    protected final LongObjectHashMap<byte[]> records = LongObjectHashMap.newMap();

    protected final LongArrayList freeRecids = new LongArrayList();


    protected long maxRecid = 0L;

    @Override
    public long preallocate() {
        //-WLOCK
        return preallocate2();
        //-WUNLOCK
    }

    @Override
    public <R> void preallocatePut(long recid, @NotNull Serializer<R> serializer, @NotNull R record) {
        byte[] data = serialize(serializer, record);
        //-WLOCK
        byte[] old = records.get(recid);
        if(old==null)
            throw new DBException.RecordNotPreallocated();
        if(old!=PREALLOC_RECORD)
            throw new DBException.RecordNotPreallocated();
        records.put(recid, data);
        //-WUNLOCK
    }

    protected long preallocate2(){
        //-AWLOCKED
        long recid =
                freeRecids.isEmpty()?
                    ++maxRecid :
                    freeRecids.removeAtIndex(freeRecids.size()-1);
        records.put(recid, PREALLOC_RECORD);
        return recid;
    }

    @Override
    public <K> long put(K record, Serializer<K> serializer) {
        byte[] data = serialize(serializer, record);
        //-WLOCK
        long recid = preallocate2();
        records.put(recid, data);
        return recid;
        //-WUNLOCK
    }

    protected <K> byte[] serialize(Serializer<K> serializer, K record) {
        if(record == null)
            throw new NullPointerException();
        return Serializers.serializeToByteArray(record, serializer);
    }

    protected <E> E deser(Serializer<E> ser, byte[] value){
        if(value == PREALLOC_RECORD)
            throw new DBException.PreallocRecordAccess();
        if(value == null)
            throw new DBException.RecordNotFound();
        return ser.deserialize(new DataInput2ByteArray(value));
    }


    private byte[] checkExists(long recid) {
        byte[] old = records.get(recid);
        if(old == PREALLOC_RECORD)
            throw new DBException.PreallocRecordAccess();
        if(old == null)
            throw new DBException.RecordNotFound();

        return old;
    }


    @Override
    public <K> void update(long recid, Serializer<K> serializer, K updatedRecord) {
        byte[] newData = serialize(serializer, updatedRecord);
        //-WLOCK
        checkExists(recid);
        records.put(recid, newData);
        //-WUNLOCK
    }


    @Override
    public <R> void updateAtomic(long recid, Serializer<R> serializer, Transform<R> r) {
        //-WLOCK
        checkExists(recid);

        R oldRec = deser(serializer, records.get(recid));
        R newRec = r.transform(oldRec);
        byte[] newVal = serialize(serializer, newRec);

        records.put(recid, newVal);
        //-WUNLOCK
    }


    @Override
    public <R> boolean compareAndUpdate(long recid, Serializer<R> serializer, R expectedOldRecord, R updatedRecord) {
        //-WLOCK
        byte[] b = checkExists(recid);
        R rec = deser(serializer, b);
        if(!serializer.equals(rec, expectedOldRecord))
            return false;
        b = serialize(serializer, updatedRecord);
        records.put(recid, b);
        return true;
        //-WUNLOCK
    }

    @Override
    public <R> boolean compareAndDelete(long recid, Serializer<R> serializer, R expectedOldRecord) {
        //-WLOCK
        byte[] b = checkExists(recid);
        R rec = deser(serializer, b);
        if(!serializer.equals(rec, expectedOldRecord))
            return false;
        delete2(recid);
        return true;
        //-WUNLOCK
    }

    @Override
    public <R> void delete(long recid, Serializer<R> serializer) {
        //-WLOCK
        delete2(recid);
        //-WUNLOCK
    }

    protected void delete2(long recid) {
        //-ARLOCKED
        byte[] buf = records.removeKey(recid);
        if(buf == null)
            throw new DBException.RecordNotFound();
        if(buf == PREALLOC_RECORD) {
            records.put(recid, PREALLOC_RECORD);
            throw new DBException.PreallocRecordAccess();
        }

        freeRecids.add(recid);
    }

    @Override
    public <R> R getAndDelete(long recid, Serializer<R> serializer) {
        byte[] buf = null;
        //-WLOCK
        buf = checkExists(recid);
        delete2(recid);
        //-WUNLOCK
        return deser(serializer, buf);
    }

    @Override
    public <K> K get(long recid, Serializer<K> ser) {
        if(recid<=0)
            throw new DBException.RecordNotFound();
        byte[] buf = null;
        //--RLOCK
        buf = checkExists(recid);
        //-RUNLOCK

        return deser(ser, buf);
    }


    @Override
    public void getAll(GetAllCallback callback) {
        //--RLOCK
        records.forEachKeyValue(
                (recid, buf) -> {
                    if (buf != PREALLOC_RECORD)
                        callback.takeOne(recid, buf);
                });
        //-RUNLOCK
    }


    @Override
    public boolean isEmpty() {
        //-RLOCK
        return records.isEmpty();
        //-RUNLOCK
    }

    @Override
    public void close() {
    }


    @Override
    public void verify() {
    }

    @Override
    public void commit() {

    }

    @Override
    public void compact() {
        //-WLOCK
        records.compact();
        freeRecids.trimToSize();
        //-WUNLOCK
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

}
