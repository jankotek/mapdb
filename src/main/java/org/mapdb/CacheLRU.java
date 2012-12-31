package org.mapdb;

/**
 * Least Recently Used cache.
 * If cache is full it removes less used items to make a space
 */
public class CacheLRU extends EngineWrapper {

    /** used instead of null value */
    protected static final Object NULL = new Object();

    protected LongMap<Object> cache;

    protected final Locks.RecidLocks locks = new Locks.SegmentedRecidLocks(16);


    public CacheLRU(Engine engine, int cacheSize) {
        this(engine, new LongConcurrentLRUMap<Object>(cacheSize, (int) (cacheSize*0.8)));
    }

    public CacheLRU(Engine engine, LongMap<Object> cache){
        super(engine);
        this.cache = cache;
    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        long recid =  super.put(value, serializer);
        try{
            locks.lock(recid);
            cache.put(recid, value!=null? value : NULL);
        }finally {
            locks.unlock(recid);
        }
        return recid;
    }

    @SuppressWarnings("unchecked")
	@Override
    public <A> A get(long recid, Serializer<A> serializer) {
        Object ret = cache.get(recid);
        if(ret!=null) return ret==NULL? null: (A) ret;
        try{
            locks.lock(recid);
            ret = super.get(recid, serializer);
            if(ret!=null) cache.put(recid, ret);
            return (A) ret;
        }finally {
            locks.unlock(recid);
        }
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        try{
            locks.lock(recid);
            cache.put(recid, value==null?NULL:value);
            super.update(recid, value, serializer);
        }finally {
            locks.unlock(recid);
        }
    }

    @Override
    public void delete(long recid) {
        try{
            locks.lock(recid);
            cache.remove(recid);
            super.delete(recid);
        }finally {
            locks.unlock(recid);
        }
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        try{
            locks.lock(recid);
            Object oldValue = cache.get(recid);
            if(oldValue!=null && (oldValue == expectedOldValue || oldValue.equals(expectedOldValue)
                    || (oldValue==NULL &&newValue==null))){
                //found matching entry in cache, so just update and return true
                cache.put(recid, newValue==null?NULL:newValue);
                engine.update(recid, newValue, serializer);
                return true;
            }else{
                boolean ret = engine.compareAndSwap(recid, expectedOldValue, newValue, serializer);
                if(ret) cache.put(recid, newValue);
                return ret;
            }
        }finally {
            locks.unlock(recid);
        }
    }


    @SuppressWarnings("rawtypes")
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
