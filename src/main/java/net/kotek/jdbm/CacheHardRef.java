package net.kotek.jdbm;



/**
 * Cache created objects using hard reference.
 * It auto-clears on low memory to prevent OutOfMemoryException.
 *
 * @author Jan Kotek
 */
public class CacheHardRef implements RecordManager{

    protected final LongConcurrentHashMap<Object> cache;

    protected static final Object NULL = new Object();

    protected final Runnable lowMemoryListener = new Runnable() {
        @Override
        public void run() {
            cache.clear();
            //TODO clear() may have high overhead, maybe just create new map instance
        }
    };
    protected final RecordManager recman;

    public CacheHardRef(RecordManager recman, int initialCapacity) {
        this.cache = new LongConcurrentHashMap<Object>(initialCapacity);
        this.recman = recman;
        MemoryLowWarningSystem.addListener(lowMemoryListener);
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        cache.put(recid, value!=null?value:NULL);
        recman.recordUpdate(recid, value, serializer);
    }

    @Override
    public void recordDelete(long recid) {
        cache.remove(recid);
        recman.recordDelete(recid);
    }

    @Override
    public Long getNamedRecid(String name) {
        return recman.getNamedRecid(name);
    }

    @Override
    public void setNamedRecid(String name, Long recid) {
        recman.setNamedRecid(name, recid);
    }

    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        final long recid = recman.recordPut(value, serializer);
        cache.put(recid,value!=null?value:NULL);
        return recid;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        A v = (A) cache.get(recid);
        if(v==NULL) return null;
        if(v!=null) return v;
        v =  recman.recordGet(recid, serializer);
        cache.put(recid, v!=null?v:NULL);
        return v;
    }

    @Override
    public void close() {
        MemoryLowWarningSystem.removeListener(lowMemoryListener);
        recman.close();
    }

    @Override
    public void commit() {
        recman.commit();
    }

    @Override
    public void rollback() {
        cache.clear();
        recman.rollback();
    }


}
