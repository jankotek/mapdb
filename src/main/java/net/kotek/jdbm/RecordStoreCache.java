package net.kotek.jdbm;



/**
 * Store which caches created objects using hard reference.
 * It auto-clears on low memory to prevent OutOfMemoryException.
 *
 * @author Jan Kotek
 */
public class RecordStoreCache extends RecordStoreAsyncWrite{

    protected final LongConcurrentHashMap<Object> cache = new LongConcurrentHashMap<Object>();

    protected static final Object NULL = new Object();

    protected final Runnable lowMemoryListener = new Runnable() {
        @Override
        public void run() {
            cache.clear();
            //TODO clear() may have high overhead, maybe just create new map instance
        }
    };

    public RecordStoreCache(String fileName, boolean lazySerialization) {
        super(fileName, lazySerialization);
        MemoryLowWarningSystem.addListener(lowMemoryListener);
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        cache.put(recid, value!=null?value:NULL);
        super.recordUpdate(recid, value, serializer);
    }

    @Override
    public void recordDelete(long recid) {
        cache.remove(recid);
        super.recordDelete(recid);
    }

    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        final long recid = super.recordPut(value, serializer);
        cache.put(recid,value!=null?value:NULL);
        return recid;
    }

    @Override
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        A v = (A) cache.get(recid);
        if(v==NULL) return null;
        if(v!=null) return v;
        v =  super.recordGet(recid, serializer);
        cache.put(recid, v!=null?v:NULL);
        return v;
    }

    @Override
    public void close() {
        MemoryLowWarningSystem.removeListener(lowMemoryListener);
        super.close();
    }


}
