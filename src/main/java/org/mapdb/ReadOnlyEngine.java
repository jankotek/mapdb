package org.mapdb;

/**
 * Wraps an <code>Engine</code> and throws
 * <code>UnsupportedOperationException("Read-only")</code>
 * on any modification attempt.
 */
public class ReadOnlyEngine implements Engine {

    protected Engine engine;

    public ReadOnlyEngine(Engine engine){
        this.engine = engine;
    }

    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        throw new UnsupportedOperationException("Read-only");
    }

    @Override
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        return engine.recordGet(recid, serializer);
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
        return engine.getNamedRecid(name);
    }

    @Override
    public void setNamedRecid(String name, Long recid) {
        throw new UnsupportedOperationException("Read-only");
    }

    @Override
    public void close() {
        engine.close();
        engine = null;
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
        return engine.serializerRecid();
    }

}
