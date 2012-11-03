package org.mapdb;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;

/**
 * Instance cache which uses <code>SoftReference</code> or <code>WeakReference</code>
 * Items can be removed from cache by Garbage Collector if
 */
public class CacheWeakSoftRef implements RecordManager{



    protected interface CacheItem{
        long getRecid();
        Object get();
    }

    protected static final class CacheWeakItem extends WeakReference implements CacheItem{

        final long recid;

        public CacheWeakItem(Object referent, ReferenceQueue q, long recid) {
            super(referent, q);
            this.recid = recid;
        }

        @Override
        public long getRecid() {
            return recid;
        }
    }

    protected static final class CacheSoftItem extends SoftReference implements CacheItem{

        final long recid;

        public CacheSoftItem(Object referent, ReferenceQueue q, long recid) {
            super(referent, q);
            this.recid = recid;
        }

        @Override
        public long getRecid() {
            return recid;
        }
    }

    protected ReferenceQueue<CacheItem> queue = new ReferenceQueue<CacheItem>();

    protected Thread queueThread = new Thread("JDBM GC collector"){
        public void run(){
            runRefQueue();
        };
    };


    protected LongConcurrentHashMap<CacheItem> items = new LongConcurrentHashMap<CacheItem>();


    protected RecordManager recman;
    final protected boolean useWeakRef;

    public CacheWeakSoftRef(RecordManager recman, boolean useWeakRef){
        this.recman = recman;
        this.useWeakRef = useWeakRef;

        queueThread.setDaemon(true);
        queueThread.start();
    }


    /** Collects items from GC and removes them from cache */
    protected void runRefQueue(){
        try{
            final ReferenceQueue<CacheItem> queue = this.queue;
            final LongConcurrentHashMap<CacheItem> items = this.items;

            while(true){
                CacheItem item = (CacheItem) queue.remove();
                items.remove(item.getRecid(), item);
                if(Thread.interrupted()) return;
            }
        }catch(InterruptedException e){
            //this is expected, so just silently exit thread
        }
    }

    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        long recid = recman.recordPut(value, serializer);
        items.put(recid, useWeakRef?
                new CacheWeakItem(value, queue, recid) :
                new CacheSoftItem(value, queue, recid));
        return recid;
    }

    @Override
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        CacheItem item = items.get(recid);
        if(item!=null){
            Object o = item.get();
            if(o == null)
                items.remove(recid);
            else{
                return (A) o;
            }
        }

        Object value = recman.recordGet(recid, serializer);
        if(value!=null){
            items.put(recid, useWeakRef?
                    new CacheWeakItem(value, queue, recid) :
                    new CacheSoftItem(value, queue, recid));
        }
        return (A) value;
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        items.put(recid, useWeakRef?
                new CacheWeakItem(value, queue, recid) :
                new CacheSoftItem(value, queue, recid));
        recman.recordUpdate(recid, value, serializer);
    }

    @Override
    public void recordDelete(long recid) {
        items.remove(recid);
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
    public void close() {
        recman = null;
        items = null;
        queue = null;
        queueThread.interrupt();
        queueThread = null;
    }

    @Override
    public void commit() {
        recman.commit();
    }

    @Override
    public void rollback() {
        items.clear();
        recman.rollback();
    }

    @Override
    public long serializerRecid() {
        return recman.serializerRecid();
    }

}
