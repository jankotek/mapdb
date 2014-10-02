/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.mapdb;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

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

        protected final Fun.RecordCondition condition;

        protected final ReentrantLock[] locks;


        public LRU(Engine engine, int cacheSize,  Fun.RecordCondition condition) {
            this(engine, new LongConcurrentLRUMap<Object>(cacheSize, (int) (cacheSize*0.8)), condition);
        }

        public LRU(Engine engine, LongMap<Object> cache,  Fun.RecordCondition condition){
            super(engine);

            locks = new ReentrantLock[CC.CONCURRENCY];
            for(int i=0;i<locks.length;i++) {
                locks[i] = new ReentrantLock(CC.FAIR_LOCKS);
            }

            this.cache = cache;
            this.condition = condition!=null? condition : Fun.RECORD_ALWAYS_TRUE;
        }

        @Override
        public <A> long put(A value, Serializer<A> serializer) {
            //$DELAY$
            long recid =  super.put(value, serializer);

            if(!condition.run(recid, value, serializer))
                return recid;
            //$DELAY$
            final LongMap<Object> cache2 = checkClosed(cache);
            final Lock lock = locks[Store.lockPos(recid)];
            lock.lock();
            try{
                //$DELAY$
                cache2.put(recid, value);
            }finally {

                lock.unlock();

            }
            //$DELAY$
            return recid;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            final LongMap<Object> cache2 = checkClosed(cache);
            //$DELAY$
            Object ret = cache2.get(recid);
            if(ret!=null)
                return (A) ret;
            //$DELAY$
            final Lock lock = locks[Store.lockPos(recid)];
            lock.lock();
            //$DELAY$
            try{
                //$DELAY$
                ret = super.get(recid, serializer);
                if(ret!=null && condition.run(recid, ret, serializer)) {
                    //$DELAY$
                    cache2.put(recid, ret);
                }
                //$DELAY$
                return (A) ret;
            }finally {
                lock.unlock();
            }
        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            //$DELAY$
            if(!condition.run(recid, value, serializer)){
                //$DELAY$
                super.update(recid,value,serializer);
                return;
            }


            final LongMap<Object> cache2 = checkClosed(cache);
            //$DELAY$
            final Lock lock = locks[Store.lockPos(recid)];
            lock.lock();
            //$DELAY$
            try{
                cache2.put(recid, value);
                //$DELAY$
                super.update(recid, value, serializer);
            }finally {
                lock.unlock();
            }
            //$DELAY$
        }

        @Override
        public <A> void delete(long recid, Serializer<A> serializer){
            final LongMap<Object> cache2 = checkClosed(cache);
            //$DELAY$
            final Lock lock = locks[Store.lockPos(recid)];
            lock.lock();
            try{
                //$DELAY$
                cache2.remove(recid);
                //$DELAY$
                super.delete(recid,serializer);
            }finally {
                lock.unlock();
            }
        }

        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            if(!condition.run(recid, newValue, serializer)){
                //$DELAY$
                return super.compareAndSwap(recid,expectedOldValue,newValue,serializer);
            }

            Engine engine = getWrappedEngine();
            LongMap cache2 = checkClosed(cache);
            //$DELAY$
            final Lock lock = locks[Store.lockPos(recid)];
            lock.lock();
            //$DELAY$
            try{
                Object oldValue = cache2.get(recid);
                //$DELAY$
                if(oldValue == expectedOldValue || (oldValue!=null&&oldValue.equals(expectedOldValue))){
                    //found matching entry in cache, so just update and return true
                    cache2.put(recid, newValue);
                    //$DELAY$
                    engine.update(recid, newValue, serializer);
                    //$DELAY$
                    return true;
                }else{
                    //$DELAY$
                    boolean ret = engine.compareAndSwap(recid, expectedOldValue, newValue, serializer);
                    //$DELAY$
                    if(ret) cache2.put(recid, newValue);
                    //$DELAY$
                    return ret;
                }
            }finally {
                lock.unlock();
            }
            //$DELAY$
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
     * <p>
     * This is simple, concurrent, small-overhead, random cache.
     *
     * @author Jan Kotek
     */
    public static class HashTable extends EngineWrapper implements Engine {


        protected final ReentrantLock[] locks;

        protected HashItem[] items;
        protected final int cacheMaxSize;
        protected final int cacheMaxSizeMask;

        /**
         * Salt added to keys before hashing, so it is harder to trigger hash collision attack.
         */
        protected final long hashSalt = new Random().nextLong();

        protected final Fun.RecordCondition condition;

        private static final class HashItem {
            final long key;
            final Object val;

            private HashItem(long key, Object val) {
                this.key = key;
                this.val = val;
            }
        }


        public HashTable(Engine engine, int cacheMaxSize,  Fun.RecordCondition condition) {
            super(engine);
            locks = new ReentrantLock[CC.CONCURRENCY];
            for(int i=0;i<locks.length;i++) {
                locks[i] = new ReentrantLock(CC.FAIR_LOCKS);

            }
            this.items = new HashItem[cacheMaxSize];
            this.cacheMaxSize = 1 << (32 - Integer.numberOfLeadingZeros(cacheMaxSize - 1)); //next pow of two
            this.cacheMaxSizeMask = cacheMaxSize-1;

            this.condition = condition!=null? condition : Fun.RECORD_ALWAYS_TRUE;
        }

        @Override
        public <A> long put(A value, Serializer<A> serializer) {
            //$DELAY$
            final long recid = getWrappedEngine().put(value, serializer);
            HashItem[] items2 = checkClosed(items);
            //$DELAY$
            if(!condition.run(recid, value, serializer))
                return recid;

            final Lock lock = locks[Store.lockPos(recid)];
            lock.lock();
            //$DELAY$
            try{
                items2[position(recid)] = new HashItem(recid, value);
                //$DELAY$
            }finally{
                lock.unlock();

            }
            //$DELAY$
            return recid;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <A> A get(long recid, Serializer<A> serializer) {
            //$DELAY$
            final int pos = position(recid);
            HashItem[] items2 = checkClosed(items);
            HashItem item = items2[pos]; //TODO race condition? non volatile access
            if(item!=null && recid == item.key)
                return (A) item.val;
            //$DELAY$
            Engine engine = getWrappedEngine();


            final Lock lock = locks[Store.lockPos(recid)];
            lock.lock();
            //$DELAY$
            try{
                //not in cache, fetch and add
                final A value = engine.get(recid, serializer);
                if(value!=null && condition.run(recid, value, serializer))
                    items2[pos] = new HashItem(recid, value);
                //$DELAY$
                return value;

            }finally{
                lock.unlock();
            }
            //$DELAY$
        }

        private int position(long recid) {
            return DataIO.longHash(recid ^ hashSalt)&cacheMaxSizeMask;
        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            if(!condition.run(recid, value, serializer)){
                super.update(recid,value,serializer);
                return;
            }

            //$DELAY$
            final int pos = position(recid);
            HashItem[] items2 = checkClosed(items);
            HashItem item = new HashItem(recid,value);
            Engine engine = getWrappedEngine();
            //$DELAY$
            final Lock lock = locks[Store.lockPos(recid)];
            lock.lock();
            //$DELAY$
            try{
                items2[pos] = item;
                engine.update(recid, value, serializer);
            }finally {
                lock.unlock();
            }
            //$DELAY$
        }

        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            if(!condition.run(recid, newValue, serializer)){
                return super.compareAndSwap(recid,expectedOldValue,newValue,serializer);
            }
            //$DELAY$

            final int pos = position(recid);
            HashItem[] items2 = checkClosed(items);
            Engine engine = getWrappedEngine();
            //$DELAY$
            final Lock lock = locks[Store.lockPos(recid)];
            lock.lock();
            //$DELAY$
            try{
                HashItem item = items2[pos];
                if(item!=null && item.key == recid){
                    //found in cache, so compare values
                    if(item.val == expectedOldValue || item.val.equals(expectedOldValue)){
                        //$DELAY$
                        //found matching entry in cache, so just update and return true
                        items2[pos] = new HashItem(recid, newValue);
                        engine.update(recid, newValue, serializer);
                        //$DELAY$
                        return true;
                    }else{
                        return false;
                    }
                }else{
                    boolean ret = engine.compareAndSwap(recid, expectedOldValue, newValue, serializer);
                    if(ret) items2[pos] = new HashItem(recid, newValue);
                    //$DELAY$
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
            //$DELAY$
            final Lock lock = locks[Store.lockPos(recid)];
            lock.lock();
            //$DELAY$
            try{
                engine.delete(recid,serializer);
                HashItem item = items2[pos];
                //$DELAY$
                if(item!=null && recid == item.key)
                    items[pos] = null;
            }finally {
                lock.unlock();
            }
            //$DELAY$
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
            //$DELAY$
            for(int i = 0;i<items.length;i++)
                items[i] = null;
            super.rollback();
        }

        @Override
        public void clearCache() {
            Arrays.fill(items, null);
            //$DELAY$
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


        protected final ReentrantLock[] locks;
        protected final Fun.RecordCondition condition;

        protected final CountDownLatch cleanerFinished;

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

        protected LongConcurrentHashMap<CacheItem> items = new LongConcurrentHashMap<CacheItem>();


        final protected boolean useWeakRef;
        protected boolean shutdown = false;

        public WeakSoftRef(Engine engine, boolean useWeakRef,
                           Fun.RecordCondition condition, Fun.ThreadFactory threadFactory){
            super(engine);
            locks = new ReentrantLock[CC.CONCURRENCY];
            for(int i=0;i<locks.length;i++) {
                locks[i] = new ReentrantLock(CC.FAIR_LOCKS);
            }

            this.useWeakRef = useWeakRef;
            this.condition = condition!=null? condition : Fun.RECORD_ALWAYS_TRUE;

            this.cleanerFinished = new CountDownLatch(1);

            threadFactory.newThread("MapDB: WeakCache cleaner", new Runnable() {
                @Override
                public void run() {
                    runRefQueue();
                }
            });
        }


        /** Collects items from GC and removes them from cache */
        protected void runRefQueue(){
            try {
                final ReferenceQueue<?> queue = this.queue;
                final LongConcurrentHashMap<CacheItem> items = this.items;
                if (queue == null || items==null)
                    return;
                //$DELAY$
                while (!shutdown) {
                    CacheItem item = (CacheItem) queue.remove(200);
                    if(item==null)
                        continue;
                    //$DELAY$
                    items.remove(item.getRecid(), item);
                }
                //$DELAY$
                items.clear();
            }catch(InterruptedException e){
                //this is expected, so just silently exit thread
            }finally {
                cleanerFinished.countDown();
            }
            //$DELAY$
        }

        @Override
        public <A> long put(A value, Serializer<A> serializer) {
            long recid = getWrappedEngine().put(value, serializer);
            //$DELAY$
            if(!condition.run(recid, value, serializer))
                return recid;
            //$DELAY$
            ReferenceQueue<A> q = (ReferenceQueue<A>) checkClosed(queue);
            LongConcurrentHashMap<CacheItem> items2 = checkClosed(items);
            CacheItem item = useWeakRef?
                    new CacheWeakItem(value, q, recid) :
                    new CacheSoftItem(value, q, recid);
            //$DELAY$
            final Lock lock = locks[Store.lockPos(recid)];
            lock.lock();
            //$DELAY$
            try{
                CacheItem old = items2.put(recid,item);
                if(old!=null)
                    old.clear();
                //$DELAY$
            }finally{
                lock.unlock();
            }
            //$DELAY$
            return recid;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            //$DELAY$
            LongConcurrentHashMap<CacheItem> items2 = checkClosed(items);
            CacheItem item = items2.get(recid);
            //$DELAY$
            if(item!=null){
                Object o = item.get();
                //$DELAY$
                if(o == null)
                    items2.remove(recid);
                else{
                    return (A) o;
                }
            }

            Engine engine = getWrappedEngine();

            final Lock lock = locks[Store.lockPos(recid)];
            lock.lock();
            //$DELAY$
            try{
                Object value = engine.get(recid, serializer);
                if(value!=null && condition.run(recid, value, serializer)){
                    ReferenceQueue<A> q = (ReferenceQueue<A>) checkClosed(queue);
                    //$DELAY$
                    item = useWeakRef?
                            new CacheWeakItem(value, q, recid) :
                            new CacheSoftItem(value, q, recid);
                    CacheItem old = items2.put(recid,item);
                    //$DELAY$
                    if(old!=null)
                        old.clear();
                }
                //$DELAY$
                return (A) value;
            }finally{
                lock.unlock();
            }
            //$DELAY$

        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            //$DELAY$
            if(!condition.run(recid, value, serializer)){
                //$DELAY$
                super.update(recid,value,serializer);
                return;
            }
            //$DELAY$

            Engine engine = getWrappedEngine();
            ReferenceQueue<A> q = (ReferenceQueue<A>) checkClosed(queue);
            LongConcurrentHashMap<CacheItem> items2 = checkClosed(items);
            //$DELAY$
            CacheItem item = useWeakRef?
                    new CacheWeakItem<A>(value, q, recid) :
                    new CacheSoftItem<A>(value, q, recid);
            //$DELAY$
            final Lock lock = locks[Store.lockPos(recid)];
            lock.lock();
            //$DELAY$
            try{
                CacheItem old = items2.put(recid,item);
                if(old!=null)
                    old.clear();
                //$DELAY$
                engine.update(recid, value, serializer);
            }finally {
                lock.unlock();
            }
            //$DELAY$
        }


        @Override
        public <A> void delete(long recid, Serializer<A> serializer){
            Engine engine = getWrappedEngine();
            LongMap<CacheItem> items2 = checkClosed(items);
            //$DELAY$
            final Lock lock = locks[Store.lockPos(recid)];
            lock.lock();
            //$DELAY$
            try{
                CacheItem old = items2.remove(recid);
                if(old!=null)
                    old.clear();
                //$DELAY$
                engine.delete(recid,serializer);
            }finally {
                lock.unlock();
            }
            //$DELAY$
        }

        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            //$DELAY$
            if(!condition.run(recid, newValue, serializer)){
                //$DELAY$
                return super.compareAndSwap(recid,expectedOldValue,newValue,serializer);
            }
            //$DELAY$
            Engine engine = getWrappedEngine();
            LongMap<CacheItem> items2 = checkClosed(items);
            ReferenceQueue<A> q = (ReferenceQueue<A>) checkClosed(queue);


            final Lock lock = locks[Store.lockPos(recid)];
            lock.lock();
            //$DELAY$
            try{
                CacheItem item = items2.get(recid);
                Object oldValue = item==null? null: item.get() ;
                //$DELAY$
                if(item!=null && item.getRecid() == recid &&
                        (oldValue == expectedOldValue || (oldValue!=null && oldValue.equals(expectedOldValue)))){
                    //found matching entry in cache, so just update and return true
                    //$DELAY$
                    CacheItem old = items2.put(recid,useWeakRef?
                            new CacheWeakItem<A>(newValue, q, recid) :
                            new CacheSoftItem<A>(newValue, q, recid));
                    //$DELAY$
                    if(old!=null)
                        old.clear();
                    engine.update(recid, newValue, serializer);
                    //$DELAY$
                    return true;
                }else{
                    boolean ret = engine.compareAndSwap(recid, expectedOldValue, newValue, serializer);
                    if(ret){
                        //$DELAY$
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
            //$DELAY$
        }


        @Override
        public void close() {
            shutdown = true;
            super.close();
            items = null;
            queue = null;
            try {
                cleanerFinished.await();
                //TODO should we wait for cleaner threads to shutdown? I guess it prevents memory leaks
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
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
            //$DELAY$
            while(iter.moveToNext()){
                //$DELAY$
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

        final static int CHECK_EVERY_N = 0xFFFF;

        int counter = 0;

        public HardRef(Engine engine, int initialCapacity, Fun.RecordCondition condition) {
            super(engine, new LongConcurrentHashMap<Object>(initialCapacity), condition);
        }


        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            //$DELAY$
            if(((counter++)& CHECK_EVERY_N)==0 ) {
                checkFreeMem();
            }
            return super.get(recid, serializer);
        }

        private void checkFreeMem() {
            Runtime r = Runtime.getRuntime();
            long max = r.maxMemory();
            if(max == Long.MAX_VALUE)
                return;

            double free = r.freeMemory();
            double total = r.totalMemory();
            //We believe that free refers to total not max.
            //Increasing heap size to max would increase to max
            free = free + (max-total);

            if(CC.LOG_EWRAP && LOG.isLoggable(Level.FINE))
                LOG.fine("HardRefCache: freemem = " +free + " = "+(free/max)+"%");
            //$DELAY$
            if(free<1e7 || free*4 <max){
                checkClosed(cache).clear();
                if(CC.LOG_EWRAP && LOG.isLoggable(Level.FINE))
                    LOG.fine("Clear HardRef cache");
            }
        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            if(((counter++)& CHECK_EVERY_N)==0 ) {
                checkFreeMem();
            }
            //$DELAY$
            super.update(recid, value, serializer);
        }

        @Override
        public <A> void delete(long recid, Serializer<A> serializer){
            if(((counter++)& CHECK_EVERY_N)==0 ) {
                checkFreeMem();
            }

            super.delete(recid,serializer);
        }

        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            if(((counter++)& CHECK_EVERY_N)==0 ) {
                checkFreeMem();
            }
            return super.compareAndSwap(recid, expectedOldValue, newValue, serializer);
        }
    }
}
