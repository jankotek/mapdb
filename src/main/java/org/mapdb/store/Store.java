package org.mapdb.store;

import org.mapdb.ser.Serializer;

public interface Store extends ReadonlyStore{


    /** allocates new null record, and returns its recid. It can be latter updated with `updateAtomic()` or `cas` */
    long preallocate();

    default void preallocate(long[] recids) {
        for(int i=0;i<recids.length;i++){
            recids[i] = preallocate();
        }
    }

    /** insert new record, returns recid under which record was stored */
    <K> long put(K record, Serializer<K> serializer);

    /** updateAtomic existing record with new value */
    <K> void update(long recid, Serializer<K> serializer, K updatedRecord);


    default <K> K getAndUpdate(long recid, Serializer<K> serializer, K updatedRecord) {
        K old = get(recid,serializer);
        update(recid, serializer, updatedRecord);
        return old; //TODO atomic

    }

    void verify();

    void commit();

    void compact();

    boolean isThreadSafe();


    interface Transform<R>{
        R transform(R r);
    }

    default <R> R updateAndGet(long recid, Serializer<R> serializer, Transform<R> t) {
        R old = get(recid,serializer);
        R newRec = t.transform(old);
        update(recid, serializer, newRec);
        return newRec; //TODO atomic
    }

    default <R> R getAndUpdateAtomic(long recid, Serializer<R> serializer, Transform<R> t) {
        R old = get(recid,serializer);
        R newRec = t.transform(old);
        update(recid, serializer, newRec);
        return old; //TODO atomic
    }


    <R> void updateAtomic(long recid, Serializer<R> serializer, Transform<R> r);

    /** atomically compares and swap records
     * @return true if compare was sucessfull and record was swapped, else false
     */
    <R> boolean compareAndUpdate(long recid, Serializer<R> serializer, R expectedOldRecord, R updatedRecord);

    <R> boolean compareAndDelete(long recid, Serializer<R> serializer, R expectedOldRecord);

    /** delete existing record */
    <R> void delete(long recid, Serializer<R> serializer);

    <R> R getAndDelete(long recid, Serializer<R> serializer);

}
