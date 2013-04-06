package org.mapdb;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Least Recently Used cache.
 * If cache is full it removes less used items to make a space
 */
public class CacheLRU extends EngineWrapper {


    protected LongMap<Object> cache;

    protected final ReentrantLock[] locks = Utils.newLocks(32);


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
            Utils.lock(locks,recid);
            checkClosed(cache).put(recid, value);
        }finally {
            Utils.unlock(locks,recid);
        }
        return recid;
    }

    @SuppressWarnings("unchecked")
	@Override
    public <A> A get(long recid, Serializer<A> serializer) {
        Object ret = cache.get(recid);
        if(ret!=null) return (A) ret;
        try{
            Utils.lock(locks,recid);
            ret = super.get(recid, serializer);
            if(ret!=null) checkClosed(cache).put(recid, ret);
            return (A) ret;
        }finally {
            Utils.unlock(locks,recid);
        }
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        try{
            Utils.lock(locks,recid);
            checkClosed(cache).put(recid, value);
            super.update(recid, value, serializer);
        }finally {
            Utils.unlock(locks,recid);
        }
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer){
        try{
            Utils.lock(locks,recid);
            checkClosed(cache).remove(recid);
            super.delete(recid,serializer);
        }finally {
            Utils.unlock(locks,recid);
        }
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        try{
            Utils.lock(locks,recid);
            Engine engine = getWrappedEngine();
            LongMap cache2 = checkClosed(cache);
            Object oldValue = cache.get(recid);
            if(oldValue == expectedOldValue || oldValue.equals(expectedOldValue)){
                //found matching entry in cache, so just update and return true
                cache2.put(recid, newValue);
                engine.update(recid, newValue, serializer);
                return true;
            }else{
                boolean ret = engine.compareAndSwap(recid, expectedOldValue, newValue, serializer);
                if(ret) cache2.put(recid, newValue);
                return ret;
            }
        }finally {
            Utils.unlock(locks,recid);
        }
    }


    @SuppressWarnings("rawtypes")
	@Override
    public void close() {
        Object cache2 = cache;
        if(cache2 instanceof LongConcurrentLRUMap)
            ((LongConcurrentLRUMap)cache2).destroy();
        cache = null;
        super.close();
    }

    @Override
    public void rollback() {
        //TODO locking here?
        checkClosed(cache).clear();
        super.rollback();
    }
}
