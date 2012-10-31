package net.kotek.jdbm;

/**
 * Fixed size cache which uses hash table.
 * Is thread-safe and does not require any locks.
 * Items are randomly removed and replaced by hash collisions.
 * <p/>
 * This is simple, concurrent, small-overhead, random cache.
 */
public class CacheHashTable implements RecordManager {

    protected RecordManager recman;

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



    public CacheHashTable(RecordManager recman, int cacheMaxSize) {
        this.recman = recman;
        this.items = new HashItem[cacheMaxSize];
        this.cacheMaxSize = cacheMaxSize;
    }

    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        final long recid = recman.recordPut(value, serializer);
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
        final A value = recman.recordGet(recid, serializer);
        if(value!=null)
            items[pos] = new HashItem(recid, value);
        return value;
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        items[Math.abs(Math.abs(JdbmUtil.longHash(recid)))%cacheMaxSize] = new HashItem(recid, value);
        recman.recordUpdate(recid, value, serializer);
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
        return recman.getNamedRecid(name);
    }

    @Override
    public void setNamedRecid(String name, Long recid) {
        recman.setNamedRecid(name, recid);
    }

    @Override
    public void close() {
        recman.close();
        //dereference to prevent memory leaks
        recman = null;
        items = null;
    }

    @Override
    public void commit() {
        recman.commit();
    }

    @Override
    public void rollback() {
        for(int i = 0;i<items.length;i++)
            items[i] = null;
        recman.rollback();
    }

    @Override
    public long serializerRecid() {
        return recman.serializerRecid();
    }


}
