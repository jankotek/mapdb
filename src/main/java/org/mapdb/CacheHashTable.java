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

import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Fixed size cache which uses hash table.
 * Is thread-safe and requires only minimal locking.
 * Items are randomly removed and replaced by hash collisions.
 * <p/>
 * This is simple, concurrent, small-overhead, random cache.
 *
 * @author Jan Kotek
 */
public class CacheHashTable extends EngineWrapper implements Engine {


    protected final ReentrantLock[] locks = Utils.newLocks();


    protected HashItem[] items;
    protected final int cacheMaxSize;
    protected final int cacheMaxSizeMask;

    /**
     * Salt added to keys before hashing, so it is harder to trigger hash collision attack.
     */
    protected final long hashSalt = Utils.RANDOM.nextLong();


    private static class HashItem {
        final long key;
        final Object val;

        private HashItem(long key, Object val) {
            this.key = key;
            this.val = val;
        }
    }



    public CacheHashTable(Engine engine, int cacheMaxSize) {
        super(engine);
        this.items = new HashItem[cacheMaxSize];
        this.cacheMaxSize = Utils.nextPowTwo(cacheMaxSize);
        this.cacheMaxSizeMask = cacheMaxSize-1;
    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        final long recid = getWrappedEngine().put(value, serializer);
        final Lock lock  = locks[Utils.longHash(recid)&Utils.LOCK_MASK];
        lock.lock();

        try{
            checkClosed(items)[position(recid)] = new HashItem(recid, value);
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

        final Lock lock  = locks[Utils.longHash(recid)&Utils.LOCK_MASK];
        lock.lock();

        try{
            //not in cache, fetch and add
            final A value = getWrappedEngine().get(recid, serializer);
            if(value!=null)
                items2[pos] = new HashItem(recid, value);
            return value;
        }finally{
            lock.unlock();
        }
    }

    private int position(long recid) {
        return Utils.longHash(recid^hashSalt)&cacheMaxSizeMask;
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        final int pos = position(recid);
        final Lock lock  = locks[Utils.longHash(recid)&Utils.LOCK_MASK];
        lock.lock();

        try{
            checkClosed(items)[pos] = new HashItem(recid, value);
            getWrappedEngine().update(recid, value, serializer);
        }finally {
            lock.unlock();
        }
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        final int pos = position(recid);
        final Lock lock  = locks[Utils.longHash(recid)&Utils.LOCK_MASK];
        lock.lock();

        try{
            HashItem[] items2 = checkClosed(items);
            HashItem item = items2[pos];
            if(item!=null && item.key == recid){
                //found in cache, so compare values
                if(item.val == expectedOldValue || item.val.equals(expectedOldValue)){
                    //found matching entry in cache, so just update and return true
                    items2[pos] = new HashItem(recid, newValue);
                    getWrappedEngine().update(recid, newValue, serializer);
                    return true;
                }else{
                    return false;
                }
            }else{
                boolean ret = getWrappedEngine().compareAndSwap(recid, expectedOldValue, newValue, serializer);
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
        final Lock lock  = locks[Utils.longHash(recid)&Utils.LOCK_MASK];
        lock.lock();

        try{
            getWrappedEngine().delete(recid,serializer);
            HashItem[] items2 = checkClosed(items);
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
        Arrays.fill(items,null);
        super.clearCache();
    }


}
