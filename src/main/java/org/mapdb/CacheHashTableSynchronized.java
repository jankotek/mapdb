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

/**
 * Fixed size cache which uses hash table.
 * Is thread-safe and requires only minimal locking.
 * Items are randomly removed and replaced by hash collisions.
 * <p/>
 * This is simple, concurrent, small-overhead, random cache.
 * <p/>
 * This uses `synchronized unlike `CacheHashTable` which uses `ReentrantLock`
 *
 * @author Jan Kotek
 */
public class CacheHashTableSynchronized extends EngineWrapper implements Engine {

    private static final int CONCURRENCY_FACTOR = 16;

    protected final Object[] locks;

    protected HashItem[] items;
    protected final int cacheMaxSize;

    private static class HashItem {
        final long key;
        final Object val;

        private HashItem(long key, Object val) {
            this.key = key;
            this.val = val;
        }
    }



    public CacheHashTableSynchronized(Engine engine, int cacheMaxSize) {
        super(engine);
        this.items = new HashItem[cacheMaxSize];
        this.cacheMaxSize = cacheMaxSize;
        this.locks = new Object[CONCURRENCY_FACTOR];
        for(int i=0;i<locks.length;i++)
            locks[i] = new Object();

    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        //no need for locking, as recid is not propagated outside of method yet
        final long recid = engine.put(value, serializer);
        items[position(recid)] = new HashItem(recid, value);
        return recid;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <A> A get(long recid, Serializer<A> serializer) {
        final int pos = position(recid);
        HashItem item = items[pos];
        if(item!=null && recid == item.key)
            return (A) item.val;

        synchronized (locks[pos % CONCURRENCY_FACTOR]){
            //not in cache, fetch and add
            final A value = engine.get(recid, serializer);
            if(value!=null)
                items[pos] = new HashItem(recid, value);
            return value;
        }
    }

    private int position(long recid) {
        return Math.abs(Utils.longHash(recid))%cacheMaxSize;
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        final int pos = position(recid);
        synchronized (locks[pos % CONCURRENCY_FACTOR]){
            items[pos] = new HashItem(recid, value);
            engine.update(recid, value, serializer);
        }
    }

    @Override
    public void delete(long recid) {
        final int pos = position(recid);
        synchronized (locks[pos % CONCURRENCY_FACTOR]){
            engine.delete(recid);
            HashItem item = items[pos];
            if(item!=null && recid == item.key)
                items[pos] = null;
        }
    }


    @Override
    public void close() {
        engine.close();
        //dereference to prevent memory leaks
        engine = null;
        items = null;
    }

    @Override
    public void rollback() {
        for(int i = 0;i<items.length;i++)
            items[i] = null;
        engine.rollback();
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        final int pos = position(recid);
        synchronized (locks[pos % CONCURRENCY_FACTOR]){
            HashItem item = items[pos];
            if(item!=null && item.key == recid && (item.val == expectedOldValue || item.val.equals(expectedOldValue))){
                //found matching entry in cache, so just update and return true
                items[pos] = new HashItem(recid, newValue);
                engine.update(recid, newValue, serializer);
                return true;
            }else{
                boolean ret = engine.compareAndSwap(recid, expectedOldValue, newValue, serializer);
                if(ret) items[pos] = new HashItem(recid, newValue);
                return ret;
            }
        }
    }

}
