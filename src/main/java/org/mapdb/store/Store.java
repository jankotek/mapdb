package org.mapdb.store;

import org.jetbrains.annotations.NotNull;
import org.mapdb.ser.Serializer;

public interface Store extends ReadonlyStore{


    /** allocates new null record, and returns its recid. It can be latter updated with `updateAtomic()` or `cas` */
    long preallocate();

    <R> void preallocatePut(long recid, @NotNull Serializer<R> serializer, @NotNull R record);

    default void preallocate(long[] recids) {
        for(int i=0;i<recids.length;i++){
            recids[i] = preallocate();
        }
    }

    /** insert new record, returns recid under which record was stored */
    @NotNull <R> long put(@NotNull R record, @NotNull  Serializer<R> serializer);

    /** updateAtomic existing record with new value */
    <R> void update(long recid, @NotNull  Serializer<R> serializer, @NotNull  R updatedRecord);


    @NotNull default <R> R getAndUpdate(long recid, @NotNull Serializer<R> serializer, @NotNull R updatedRecord) {
        R old = get(recid,serializer);
        update(recid, serializer, updatedRecord);
        return old; //TODO atomic

    }

    void verify();

    void commit();

    void compact();

    boolean isThreadSafe();




    interface Transform<R>{
        @NotNull R transform(@NotNull R r);
    }

    @NotNull default <R> R updateAndGet(long recid, @NotNull Serializer<R> serializer, @NotNull Transform<R> t) {
        R old = get(recid,serializer);
        R newRec = t.transform(old);
        update(recid, serializer, newRec);
        return newRec; //TODO atomic
    }

    @NotNull default <R> R getAndUpdateAtomic(long recid, @NotNull Serializer<R> serializer, @NotNull Transform<R> t) {
        R old = get(recid,serializer);
        R newRec = t.transform(old);
        update(recid, serializer, newRec);
        return old; //TODO atomic
    }


    <R> void updateAtomic(long recid, @NotNull Serializer<R> serializer, @NotNull Transform<R> r);

    /** atomically compares and swap records
     * @return true if compare was sucessfull and record was swapped, else false
     */
    <R> boolean compareAndUpdate(long recid, @NotNull Serializer<R> serializer, @NotNull R expectedOldRecord, @NotNull R updatedRecord);

    <R> boolean compareAndDelete(long recid, @NotNull Serializer<R> serializer, @NotNull R expectedOldRecord);

    /** delete existing record */
    <R> void delete(long recid, @NotNull Serializer<R> serializer);

    @NotNull <R> R getAndDelete(long recid, @NotNull Serializer<R> serializer);

    default int maxRecordSize() {
        return Integer.MAX_VALUE;
    }

}
