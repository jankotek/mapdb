package org.mapdb;



/**
 * Cache created objects using hard reference.
 * It auto-clears on low memory to prevent OutOfMemoryException.
 *
 * @author Jan Kotek
 */
public class CacheHardRef implements Engine {

    protected final LongConcurrentHashMap<Object> cache;

    protected static final Object NULL = new Object();

    protected final Runnable lowMemoryListener = new Runnable() {
        @Override
        public void run() {
            cache.clear();
            //TODO clear() may have high overhead, maybe just create new map instance
        }
    };
    protected final Engine engine;

    public CacheHardRef(Engine engine, int initialCapacity) {
        this.cache = new LongConcurrentHashMap<Object>(initialCapacity);
        this.engine = engine;
        MemoryLowWarningSystem.addListener(lowMemoryListener);
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        cache.put(recid, value!=null?value:NULL);
        engine.recordUpdate(recid, value, serializer);
    }

    @Override
    public void recordDelete(long recid) {
        cache.remove(recid);
        engine.recordDelete(recid);
    }


    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        final long recid = engine.recordPut(value, serializer);
        cache.put(recid,value!=null?value:NULL);
        return recid;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        A v = (A) cache.get(recid);
        if(v==NULL) return null;
        if(v!=null) return v;
        v =  engine.recordGet(recid, serializer);
        cache.put(recid, v!=null?v:NULL);
        return v;
    }

    @Override
    public void close() {
        MemoryLowWarningSystem.removeListener(lowMemoryListener);
        engine.close();
    }

    @Override
    public void commit() {
        engine.commit();
    }

    @Override
    public void rollback() {
        cache.clear();
        engine.rollback();
    }


    @Override
    public long serializerRecid() {
        return engine.serializerRecid();
    }

    @Override
    public long nameDirRecid() {
        return engine.nameDirRecid();
    }

    @Override
    public boolean isReadOnly() {
        return engine.isReadOnly();
    }


}
