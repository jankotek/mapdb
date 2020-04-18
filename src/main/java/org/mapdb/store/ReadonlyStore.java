package org.mapdb.store;

import org.jetbrains.annotations.NotNull;
import org.mapdb.ser.Serializer;

import java.io.Closeable;

public interface ReadonlyStore extends Closeable {


    /**
     * Get existing record
     *
     * @return record or null if record was not allocated yet, or was deleted
     **/
    @NotNull <K> K get(long recid, @NotNull Serializer<K> ser);

    void close();

    /**
     * Iterates over all records in store.
     *
     * Function takes recid and binary data
     */
    void getAll(@NotNull GetAllCallback callback);

    interface GetAllCallback{
        void takeOne(long recid, @NotNull byte[] data);
    }

    /**
     * Returns true if store does not contain any data and no recids were allocated yet.
     * Store is usually empty just after creation.
     */
    boolean isEmpty();

}
