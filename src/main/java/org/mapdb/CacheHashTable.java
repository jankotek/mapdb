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

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Fixed size cache which uses hash table.
 * Is thread-safe and does not require any locks.
 * Items are randomly removed and replaced by hash collisions.
 * <p/>
 * This is simple, concurrent, small-overhead, random cache.
 *
 * @author Jan Kotek
 */
public class CacheHashTable implements Engine {

    protected Engine engine;

    protected AtomicReferenceArray<HashItem> items;
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
        this.items = new AtomicReferenceArray(cacheMaxSize);
        this.cacheMaxSize = cacheMaxSize;
    }

    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        final long recid = engine.recordPut(value, serializer);
        final int pos = Math.abs(Utils.longHash(recid))%cacheMaxSize;
        items.set(pos,new HashItem(recid, value));
        return recid;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        final int pos = Math.abs(Utils.longHash(recid))%cacheMaxSize;
        HashItem item = items.get(pos);
        if(item!=null && recid == item.key)
            return (A) item.val;

        //not in cache, fetch and add
        final A value = engine.recordGet(recid, serializer);
        if(value!=null)
            items.compareAndSet(pos,item, new HashItem(recid, value));
        return value;
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        final int pos = Math.abs(Math.abs(Utils.longHash(recid)))%cacheMaxSize;
        HashItem expected = items.get(pos);
        engine.recordUpdate(recid, value, serializer);
        items.compareAndSet(pos,expected, new HashItem(recid, value));
    }

    @Override
    public void recordDelete(long recid) {
        final int pos = Math.abs(Utils.longHash(recid))%cacheMaxSize;
        HashItem expected = items.get(pos);
        engine.recordDelete(recid);
        if(expected!=null && recid == expected.key)
            items.compareAndSet(pos,expected,null);
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
        for(int i = 0;i<cacheMaxSize;i++)
            items.set(i,null);
        engine.rollback();
    }

    @Override
    public long serializerRecid() {
        return engine.serializerRecid();
    }

    @Override
    public long nameDirRecid() {
        return engine.nameDirRecid();
    }

    @Override
    public boolean isReadOnly() {
        return engine.isReadOnly();
    }

}
