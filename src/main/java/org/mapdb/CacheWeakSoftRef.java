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
import java.util.concurrent.locks.ReentrantLock;

/**
 * Instance cache which uses <code>SoftReference</code> or <code>WeakReference</code>
 * Items can be removed from cache by Garbage Collector if
 *
 * @author Jan Kotek
 */
public class CacheWeakSoftRef extends EngineWrapper implements Engine {


    protected final ReentrantLock[] locks = Utils.newLocks(32);

    protected interface CacheItem{
        long getRecid();
        Object get();
    }

    protected static final class CacheWeakItem<A> extends WeakReference<A> implements CacheItem{

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

    protected static final class CacheSoftItem<A> extends SoftReference<A> implements CacheItem{

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

    @SuppressWarnings("rawtypes")
	protected ReferenceQueue queue = new ReferenceQueue();

    protected Thread queueThread = new Thread("MapDB GC collector"){
        @Override
		public void run(){
            runRefQueue();
        }
    };


    protected LongConcurrentHashMap<CacheItem> items = new LongConcurrentHashMap<CacheItem>();


    final protected boolean useWeakRef;

    public CacheWeakSoftRef(Engine engine, boolean useWeakRef){
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
        putItemIntoCache(recid, value);
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

        try{
            Utils.lock(locks,recid);
            Object value = getWrappedEngine().get(recid, serializer);
            if(value!=null) putItemIntoCache(recid, value);

            return (A) value;
        }finally{
            Utils.unlock(locks,recid);
        }

    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        try{
            Utils.lock(locks,recid);
            putItemIntoCache(recid, value);
            getWrappedEngine().update(recid, value, serializer);
        }finally {
            Utils.unlock(locks,recid);
        }
    }

    @SuppressWarnings("unchecked")
	private <A> void putItemIntoCache(long recid, A value) {
        ReferenceQueue<A> q = checkClosed(queue);
        checkClosed(items).put(recid, useWeakRef?
            new CacheWeakItem<A>(value, q, recid) :
            new CacheSoftItem<A>(value, q, recid));
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer){
        try{
            Utils.lock(locks,recid);
            checkClosed(items).remove(recid);
            getWrappedEngine().delete(recid,serializer);
        }finally {
            Utils.unlock(locks,recid);
        }

    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        try{
            Utils.lock(locks,recid);
            CacheItem item = checkClosed(items).get(recid);
            Object oldValue = item==null? null: item.get() ;
            if(item!=null && item.getRecid() == recid &&
                    (oldValue == expectedOldValue || oldValue.equals(expectedOldValue))){
                //found matching entry in cache, so just update and return true
                putItemIntoCache(recid, newValue);
                getWrappedEngine().update(recid, newValue, serializer);
                return true;
            }else{
                boolean ret = getWrappedEngine().compareAndSwap(recid, expectedOldValue, newValue, serializer);
                if(ret) putItemIntoCache(recid, newValue);
                return ret;
            }
        }finally {
            Utils.unlock(locks,recid);
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

}
