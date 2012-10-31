package net.kotek.jdbm;

/**
 * Wraps an <code>RecordManager</code> and throws
 * <code>UnsupportedOperationException("Read-only")</code>
 * on any modification attempt.
 */
public class ReadOnlyWrapper implements RecordManager{

    protected RecordManager recman;

    public ReadOnlyWrapper(RecordManager recman){
        this.recman = recman;
    }

    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        throw new UnsupportedOperationException("Read-only");
    }

    @Override
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        return recman.recordGet(recid, serializer);
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        throw new UnsupportedOperationException("Read-only");
    }

    @Override
    public void recordDelete(long recid) {
        throw new UnsupportedOperationException("Read-only");
    }

    @Override
    public Long getNamedRecid(String name) {
        return recman.getNamedRecid(name);
    }

    @Override
    public void setNamedRecid(String name, Long recid) {
        throw new UnsupportedOperationException("Read-only");
    }

    @Override
    public void close() {
        recman.close();
        recman = null;
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException("Read-only");
    }

    @Override
    public void rollback() {
        throw new UnsupportedOperationException("Read-only");
    }

    @Override
    public long serializerRecid() {
        return recman.serializerRecid();
    }

}
