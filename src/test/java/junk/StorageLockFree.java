package junk;

import org.mapdb.Engine;
import org.mapdb.Serializer;
import org.mapdb.Storage;

import java.util.Arrays;

/**
 * Direct Storage (no transactions) which does not require global lock.
 * Is considered experimental.
 *
 * <p> Storage is divided into segments, each with separate write lock.
 * Index file is marked with negative value so it can use the same structure.
 */
public class StorageLockFree implements Engine {


    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        return 0;
    }

    @Override
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        return null;
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {

    }

    @Override
    public void recordDelete(long recid) {

    }

    @Override
    public Long getNamedRecid(String name) {
        return null;
    }

    @Override
    public void setNamedRecid(String name, Long recid) {

    }

    @Override
    public void close() {

    }

    @Override
    public void commit() {

    }

    @Override
    public void rollback() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long serializerRecid() {
        return 0;
    }
}
