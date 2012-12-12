package org.mapdb;

/**
 * Least Recently Used cache.
 * If cache is full it removes less used items to make a space
 */
public class CacheLRU extends EngineWrapper {

    protected LongMap cache;

    private static final int CONCURRENCY_FACTOR = 16;

    protected Object[] locks = new Object[CONCURRENCY_FACTOR];


    public CacheLRU(Engine engine, int cacheSize) {
        this(engine, new LongConcurrentLRUMap(cacheSize, (int) (cacheSize*0.8)));
    }

    public CacheLRU(Engine engine, LongMap cache){
        super(engine);
        for(int i=0;i<CONCURRENCY_FACTOR; i++){
            locks[i] = new Object();
        }
        this.cache = cache;
    }

    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        //no need to synchronize as recid was no propagated outside
        return super.recordPut(value, serializer);
    }

    @Override
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        Object ret = cache.get(recid);
        if(ret!=null) return (A) ret;
        synchronized (locks[((int) (recid % CONCURRENCY_FACTOR))]){
            ret = super.recordGet(recid, serializer);
            if(ret!=null) cache.put(recid, ret);
            return (A) ret;
        }
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        synchronized (locks[((int) (recid % CONCURRENCY_FACTOR))]){
            cache.put(recid, value);
            super.recordUpdate(recid, value, serializer);
        }
    }

    @Override
    public void recordDelete(long recid) {
        synchronized (locks[((int) (recid % CONCURRENCY_FACTOR))]){
            cache.remove(recid);
            super.recordDelete(recid);
        }
    }


    @Override
    public void close() {
        if(cache instanceof LongConcurrentLRUMap)
            ((LongConcurrentLRUMap)cache).destroy();
        cache = null;
        super.close();
    }

    @Override
    public void rollback() {
        //TODO locking here?
        cache.clear();
        super.rollback();
    }
}
