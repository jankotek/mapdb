package org.mapdb;

/**
 * Fixed size cache which uses hash table.
 * Is thread-safe and does not require any locks.
 * Items are randomly removed and replaced by hash collisions.
 * <p/>
 * This is simple, concurrent, small-overhead, random cache.
 */
public class CacheHashTable implements Engine {

    protected Engine engine;

    protected HashItem[] items;
    protected final int cacheMaxSize;

    private class HashItem {
        final long key;
        final Object val;

        private HashItem(long key, Object val) {
            this.key = key;
            this.val = val;
        }
    }



    public CacheHashTable(Engine engine, int cacheMaxSize) {
        this.engine = engine;
        this.items = new HashItem[cacheMaxSize];
        this.cacheMaxSize = cacheMaxSize;
    }

    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        final long recid = engine.recordPut(value, serializer);
        items[Math.abs(JdbmUtil.longHash(recid))%cacheMaxSize] = new HashItem(recid, value);
        return recid;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        final int pos = Math.abs(JdbmUtil.longHash(recid))%cacheMaxSize;
        HashItem item = items[pos];
        if(item!=null && recid == item.key)
            return (A) item.val;

        //not in cache, fetch and add
        final A value = engine.recordGet(recid, serializer);
        if(value!=null)
            items[pos] = new HashItem(recid, value);
        return value;
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        items[Math.abs(Math.abs(JdbmUtil.longHash(recid)))%cacheMaxSize] = new HashItem(recid, value);
        engine.recordUpdate(recid, value, serializer);
    }

    @Override
    public void recordDelete(long recid) {
        final int pos = Math.abs(JdbmUtil.longHash(recid))%cacheMaxSize;
        HashItem item = items[pos];
        if(item!=null && recid == item.key)
            items[pos] = null;
    }

    @Override
    public Long getNamedRecid(String name) {
        return engine.getNamedRecid(name);
    }

    @Override
    public void setNamedRecid(String name, Long recid) {
        engine.setNamedRecid(name, recid);
    }

    @Override
    public void close() {
        engine.close();
        //dereference to prevent memory leaks
        engine = null;
        items = null;
    }

    @Override
    public void commit() {
        engine.commit();
    }

    @Override
    public void rollback() {
        for(int i = 0;i<items.length;i++)
            items[i] = null;
        engine.rollback();
    }

    @Override
    public long serializerRecid() {
        return engine.serializerRecid();
    }


}
