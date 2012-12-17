package org.mapdb;

/**
 * Least Recently Used cache.
 * If cache is full it removes less used items to make a space
 */
public class CacheLRU extends EngineWrapper {

    protected LongMap cache;

    protected final Locks.RecidLocks locks = new Locks.SegmentedRecidLocks(16);


    public CacheLRU(Engine engine, int cacheSize) {
        this(engine, new LongConcurrentLRUMap(cacheSize, (int) (cacheSize*0.8)));
    }

    public CacheLRU(Engine engine, LongMap cache){
        super(engine);
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
        try{
            locks.lock(recid);
            ret = super.recordGet(recid, serializer);
            if(ret!=null) cache.put(recid, ret);
            return (A) ret;
        }finally {
            locks.unlock(recid);
        }
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        try{
            locks.lock(recid);
            cache.put(recid, value);
            super.recordUpdate(recid, value, serializer);
        }finally {
            locks.unlock(recid);
        }
    }

    @Override
    public void recordDelete(long recid) {
        try{
            locks.lock(recid);
            cache.remove(recid);
            super.recordDelete(recid);
        }finally {
            locks.unlock(recid);
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
