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
 *
 * @author Jan Kotek
 */
public class CacheHashTable extends EngineWrapper implements Engine {

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



    public CacheHashTable(Engine engine, int cacheMaxSize) {
        super(engine);
        this.items = new HashItem[cacheMaxSize];
        this.cacheMaxSize = cacheMaxSize;
        this.locks = new Object[CONCURRENCY_FACTOR];
        for(int i=0;i<locks.length;i++)
            locks[i] = new Object();

    }

    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        //no need for locking, as recid is not propagated outside of method yet
        final long recid = engine.recordPut(value, serializer);
        items[Math.abs(Utils.longHash(recid))%cacheMaxSize] = new HashItem(recid, value);
        return recid;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        final int pos = Math.abs(Utils.longHash(recid))%cacheMaxSize;
        HashItem item = items[pos];
        if(item!=null && recid == item.key)
            return (A) item.val;

        synchronized (locks[pos % CONCURRENCY_FACTOR]){
            //not in cache, fetch and add
            final A value = engine.recordGet(recid, serializer);
            if(value!=null)
                items[pos] = new HashItem(recid, value);
            return value;
        }
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        final int pos = Math.abs(Utils.longHash(recid))%cacheMaxSize;
        synchronized (locks[pos % CONCURRENCY_FACTOR]){
            items[pos] = new HashItem(recid, value);
            engine.recordUpdate(recid, value, serializer);
        }
    }

    @Override
    public void recordDelete(long recid) {
        final int pos = Math.abs(Utils.longHash(recid))%cacheMaxSize;
        synchronized (locks[pos % CONCURRENCY_FACTOR]){
            engine.recordDelete(recid);
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


}
