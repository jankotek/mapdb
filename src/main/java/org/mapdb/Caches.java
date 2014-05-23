package org.mapdb;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Contains various instance cache implementations
 */
public final class Caches {

    private Caches(){}


    /**
     * Least Recently Used cache.
     * If cache is full it removes less used items to make a space
     */
    public static class LRU extends EngineWrapper {


        protected LongMap<Object> cache;

        protected final ReentrantLock[] locks = new ReentrantLock[CC.CONCURRENCY];
        {
            for(int i=0;i<locks.length;i++)
                locks[i] = new ReentrantLock(CC.FAIR_LOCKS);
        }


        public LRU(Engine engine, int cacheSize, boolean disableLocks) {
            this(engine, new LongConcurrentLRUMap<Object>(cacheSize, (int) (cacheSize*0.8)),disableLocks);
        }

        public LRU(Engine engine, LongMap<Object> cache, boolean disableLocks){
            super(engine);
            this.cache = cache;
        }

        @Override
        public <A> long put(A value, Serializer<A> serializer) {
            long recid =  super.put(value, serializer);
            final LongMap<Object> cache2 = checkClosed(cache);
            final Lock lock  = locks[Store.lockPos(recid)];
            lock.lock();
            try{
                cache2.put(recid, value);
            }finally {
                lock.unlock();
            }
            return recid;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            final LongMap<Object> cache2 = checkClosed(cache);
            Object ret = cache2.get(recid);
            if(ret!=null)
                return (A) ret;

            final Lock lock  = locks[Store.lockPos(recid)];
            lock.lock();
            try{
                ret = super.get(recid, serializer);
                if(ret!=null)
                    cache2.put(recid, ret);
                return (A) ret;
            }finally {
                lock.unlock();
            }
        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            final LongMap<Object> cache2 = checkClosed(cache);

            final Lock lock  = locks[Store.lockPos(recid)];
            lock.lock();
            try{
                cache2.put(recid, value);
                super.update(recid, value, serializer);
            }finally {
                lock.unlock();
            }
        }

        @Override
        public <A> void delete(long recid, Serializer<A> serializer){
            final LongMap<Object> cache2 = checkClosed(cache);

            final Lock lock  = locks[Store.lockPos(recid)];
            lock.lock();
            try{
                cache2.remove(recid);
                super.delete(recid,serializer);
            }finally {
                lock.unlock();
            }
        }

        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            Engine engine = getWrappedEngine();
            LongMap cache2 = checkClosed(cache);

            final Lock lock  = locks[Store.lockPos(recid)];
            lock.lock();
            try{
                Object oldValue = cache2.get(recid);
                if(oldValue == expectedOldValue || (oldValue!=null&&oldValue.equals(expectedOldValue))){
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
                lock.unlock();
            }
        }


        @Override
        public void close() {
            cache = null;
            super.close();
        }

        @Override
        public void rollback() {
            //TODO locking here?
            checkClosed(cache).clear();
            super.rollback();
        }

        @Override
        public void clearCache() {
            cache.clear();
            super.clearCache();
        }
    }

    /**
     * Fixed size cache which uses hash table.
     * Is thread-safe and requires only minimal locking.
     * Items are randomly removed and replaced by hash collisions.
     * <p/>
     * This is simple, concurrent, small-overhead, random cache.
     *
     * @author Jan Kotek
     */
    public static class HashTable extends EngineWrapper implements Engine {


        protected final ReentrantLock[] locks = new ReentrantLock[CC.CONCURRENCY];
        {
            for(int i=0;i<locks.length;i++)
                locks[i] = new ReentrantLock(CC.FAIR_LOCKS);
        }


        protected HashItem[] items;
        protected final int cacheMaxSize;
        protected final int cacheMaxSizeMask;

        /**
         * Salt added to keys before hashing, so it is harder to trigger hash collision attack.
         */
        protected final long hashSalt = new Random().nextLong();


        private static class HashItem {
            final long key;
            final Object val;

            private HashItem(long key, Object val) {
                this.key = key;
                this.val = val;
            }
        }



        public HashTable(Engine engine, int cacheMaxSize, boolean disableLocks) {
            super(engine);
            this.items = new HashItem[cacheMaxSize];
            this.cacheMaxSize = 1 << (32 - Integer.numberOfLeadingZeros(cacheMaxSize - 1)); //next pow of two
            this.cacheMaxSizeMask = cacheMaxSize-1;
        }

        @Override
        public <A> long put(A value, Serializer<A> serializer) {
            final long recid = getWrappedEngine().put(value, serializer);
            HashItem[] items2 = checkClosed(items);

            final Lock lock  = locks[Store.lockPos(recid)];
            lock.lock();
            try{
                items2[position(recid)] = new HashItem(recid, value);
            }finally{
                lock.unlock();
            }
            return recid;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <A> A get(long recid, Serializer<A> serializer) {
            final int pos = position(recid);
            HashItem[] items2 = checkClosed(items);
            HashItem item = items2[pos];
            if(item!=null && recid == item.key)
                return (A) item.val;

            Engine engine = getWrappedEngine();

            final Lock lock  = locks[Store.lockPos(recid)];
            lock.lock();

            try{
                //not in cache, fetch and add
                final A value = engine.get(recid, serializer);
                if(value!=null)
                    items2[pos] = new HashItem(recid, value);
                return value;
            }finally{
                lock.unlock();
            }
        }

        private int position(long recid) {
            return LongHashMap.longHash(recid^hashSalt)&cacheMaxSizeMask;
        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            final int pos = position(recid);
            HashItem[] items2 = checkClosed(items);
            HashItem item = new HashItem(recid,value);
            Engine engine = getWrappedEngine();

            final Lock lock  = locks[Store.lockPos(recid)];
            lock.lock();
            try{
                items2[pos] = item;
                engine.update(recid, value, serializer);
            }finally {
                lock.unlock();
            }
        }

        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            final int pos = position(recid);
            HashItem[] items2 = checkClosed(items);
            Engine engine = getWrappedEngine();

            final Lock lock  = locks[Store.lockPos(recid)];
            lock.lock();
            try{
                HashItem item = items2[pos];
                if(item!=null && item.key == recid){
                    //found in cache, so compare values
                    if(item.val == expectedOldValue || item.val.equals(expectedOldValue)){
                        //found matching entry in cache, so just update and return true
                        items2[pos] = new HashItem(recid, newValue);
                        engine.update(recid, newValue, serializer);
                        return true;
                    }else{
                        return false;
                    }
                }else{
                    boolean ret = engine.compareAndSwap(recid, expectedOldValue, newValue, serializer);
                    if(ret) items2[pos] = new HashItem(recid, newValue);
                    return ret;
                }
            }finally {
                lock.unlock();
            }
        }

        @Override
        public <A> void delete(long recid, Serializer<A> serializer){
            final int pos = position(recid);
            HashItem[] items2 = checkClosed(items);
            Engine engine = getWrappedEngine();

            final Lock lock  = locks[Store.lockPos(recid)];
            lock.lock();
            try{
                engine.delete(recid,serializer);
                HashItem item = items2[pos];
                if(item!=null && recid == item.key)
                items[pos] = null;
            }finally {
                lock.unlock();
            }

        }


        @Override
        public void close() {
            super.close();
            //dereference to prevent memory leaks
            items = null;
        }

        @Override
        public void rollback() {
            //TODO lock all in caches on rollback/commit?
            for(int i = 0;i<items.length;i++)
                items[i] = null;
            super.rollback();
        }

        @Override
        public void clearCache() {
            Arrays.fill(items, null);
            super.clearCache();
        }


    }

    /**
     * Instance cache which uses <code>SoftReference</code> or <code>WeakReference</code>
     * Items can be removed from cache by Garbage Collector if
     *
     * @author Jan Kotek
     */
    public static class WeakSoftRef extends EngineWrapper implements Engine {


        protected final ReentrantLock[] locks = new ReentrantLock[CC.CONCURRENCY];
        {
            for(int i=0;i<locks.length;i++)
                locks[i] = new ReentrantLock(CC.FAIR_LOCKS);
        }


        protected interface CacheItem{
            long getRecid();
            Object get();
            void clear();
        }

        protected static final class CacheWeakItem<A> extends WeakReference<A> implements CacheItem {

            final long recid;

            public CacheWeakItem(A referent, ReferenceQueue<A> q, long recid) {
                super(referent, q);
                this.recid = recid;
            }

            @Override
            public long getRecid() {
                return recid;
            }
        }

        protected static final class CacheSoftItem<A> extends SoftReference<A> implements CacheItem {

            final long recid;

            public CacheSoftItem(A referent, ReferenceQueue<A> q, long recid) {
                super(referent, q);
                this.recid = recid;
            }

            @Override
            public long getRecid() {
                return recid;
            }
        }

        protected ReferenceQueue<Object> queue = new ReferenceQueue<Object>();

        protected Thread queueThread = new Thread("MapDB GC collector"){
            @Override
            public void run(){
                runRefQueue();
            }
        };


        protected LongConcurrentHashMap<CacheItem> items = new LongConcurrentHashMap<CacheItem>();


        final protected boolean useWeakRef;

        public WeakSoftRef(Engine engine, boolean useWeakRef, boolean disableLocks){
            super(engine);
            this.useWeakRef = useWeakRef;

            queueThread.setDaemon(true);
            queueThread.start();
        }


        /** Collects items from GC and removes them from cache */
        protected void runRefQueue(){
            try{
                final ReferenceQueue<?> queue = this.queue;
                if(queue == null)return;
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
        public <A> long put(A value, Serializer<A> serializer) {
            long recid = getWrappedEngine().put(value, serializer);
            ReferenceQueue<A> q = (ReferenceQueue<A>) checkClosed(queue);
            LongConcurrentHashMap<CacheItem> items2 = checkClosed(items);
            CacheItem item = useWeakRef?
                    new CacheWeakItem(value, q, recid) :
                    new CacheSoftItem(value, q, recid);

            final Lock lock  = locks[Store.lockPos(recid)];
            lock.lock();
            try{
                CacheItem old = items2.put(recid,item);
                if(old!=null)
                    old.clear();
            }finally{
                lock.unlock();
            }
            return recid;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            LongConcurrentHashMap<CacheItem> items2 = checkClosed(items);
            CacheItem item = items2.get(recid);
            if(item!=null){
                Object o = item.get();
                if(o == null)
                    items2.remove(recid);
                else{
                    return (A) o;
                }
            }

            Engine engine = getWrappedEngine();

            final Lock lock  = locks[Store.lockPos(recid)];
            lock.lock();

            try{
                Object value = engine.get(recid, serializer);
                if(value!=null){
                    ReferenceQueue<A> q = (ReferenceQueue<A>) checkClosed(queue);
                    item = useWeakRef?
                            new CacheWeakItem(value, q, recid) :
                            new CacheSoftItem(value, q, recid);
                    CacheItem old = items2.put(recid,item);
                    if(old!=null)
                        old.clear();
                }

                return (A) value;
            }finally{
                lock.unlock();
            }

        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            Engine engine = getWrappedEngine();
            ReferenceQueue<A> q = (ReferenceQueue<A>) checkClosed(queue);
            LongConcurrentHashMap<CacheItem> items2 = checkClosed(items);
            CacheItem item = useWeakRef?
                    new CacheWeakItem<A>(value, q, recid) :
                    new CacheSoftItem<A>(value, q, recid);

            final Lock lock  = locks[Store.lockPos(recid)];
            lock.lock();
            try{
                CacheItem old = items2.put(recid,item);
                if(old!=null)
                    old.clear();
                engine.update(recid, value, serializer);
            }finally {
                lock.unlock();
            }
        }


        @Override
        public <A> void delete(long recid, Serializer<A> serializer){
            Engine engine = getWrappedEngine();
            LongMap<CacheItem> items2 = checkClosed(items);

            final Lock lock  = locks[Store.lockPos(recid)];
            lock.lock();
            try{
                CacheItem old = items2.remove(recid);
                if(old!=null)
                    old.clear();
                engine.delete(recid,serializer);
            }finally {
                lock.unlock();
            }

        }

        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            Engine engine = getWrappedEngine();
            LongMap<CacheItem> items2 = checkClosed(items);
            ReferenceQueue<A> q = (ReferenceQueue<A>) checkClosed(queue);


            final Lock lock  = locks[Store.lockPos(recid)];
            lock.lock();
            try{
                CacheItem item = items2.get(recid);
                Object oldValue = item==null? null: item.get() ;
                if(item!=null && item.getRecid() == recid &&
                        (oldValue == expectedOldValue || (oldValue!=null && oldValue.equals(expectedOldValue)))){
                    //found matching entry in cache, so just update and return true
                    CacheItem old = items2.put(recid,useWeakRef?
                            new CacheWeakItem<A>(newValue, q, recid) :
                            new CacheSoftItem<A>(newValue, q, recid));
                    if(old!=null)
                        old.clear();
                    engine.update(recid, newValue, serializer);
                    return true;
                }else{
                    boolean ret = engine.compareAndSwap(recid, expectedOldValue, newValue, serializer);
                    if(ret){
                        CacheItem old = items2.put(recid,useWeakRef?
                                new CacheWeakItem<A>(newValue, q, recid) :
                                new CacheSoftItem<A>(newValue, q, recid));
                        if(old!=null)
                            old.clear();
                    }
                    return ret;
                }
            }finally {
                lock.unlock();
            }
        }


        @Override
        public void close() {
            super.close();
            items = null;
            queue = null;

            if (queueThread != null) {
                queueThread.interrupt();
                queueThread = null;
            }
        }


        @Override
        public void rollback() {
            items.clear();
            super.rollback();
        }

        @Override
        public void clearCache() {
            // release all items so those are not passed to Queue
            LongMap.LongMapIterator<CacheItem> iter = items.longMapIterator();
            while(iter.moveToNext()){
                CacheItem i =  iter.value();
                if(i!=null)
                    i.clear();
            }

            items.clear();
            super.clearCache();
        }

    }

    /**
     * Cache created objects using hard reference.
     * It checks free memory every N operations (1024*10). If free memory is bellow 75% it clears the cache
     *
     * @author Jan Kotek
     */
    public static class HardRef extends LRU {

        final static int CHECK_EVERY_N = 10000;

        int counter = 0;

        public HardRef(Engine engine, int initialCapacity, boolean disableLocks) {
            super(engine, new LongConcurrentHashMap<Object>(initialCapacity),disableLocks);
        }

        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            checkFreeMem();
            return super.get(recid, serializer);
        }

        private void checkFreeMem() {
            if((counter++)%CHECK_EVERY_N==0 ){

                Runtime r = Runtime.getRuntime();
                long max = r.maxMemory();
                if(max == Long.MAX_VALUE)
                    return;

                double free = r.freeMemory();
                double total = r.totalMemory();
                //We believe that free refers to total not max.
                //Increasing heap size to max would increase to max
                free = free + (max-total);

                //TODO logging
//                if(CC.LOG_TRACE)
//                    Utils.LOG.fine("DBCache: freemem = " +free + " = "+(free/max)+"%");

                if(free<1e7 || free*4 <max){
                    checkClosed(cache).clear();
                }
            }
        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            checkFreeMem();
            super.update(recid, value, serializer);
        }

        @Override
        public <A> void delete(long recid, Serializer<A> serializer){
            checkFreeMem();
            super.delete(recid,serializer);
        }

        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            checkFreeMem();
            return super.compareAndSwap(recid, expectedOldValue, newValue, serializer);
        }
    }
}
