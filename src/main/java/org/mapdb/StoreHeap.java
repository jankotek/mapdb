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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 * Store which keeps all instances on heap. It does not use serialization.
 */
public class StoreHeap extends Store implements Serializable{

    protected final static Fun.Pair TOMBSTONE = new Fun.Pair(null,null);

    protected final static Object NULL = new Object();
    private static final long serialVersionUID = 150060834534309445L;

    /** All commited records in store */
    protected final ConcurrentNavigableMap<Long,Fun.Pair> records
            = new ConcurrentSkipListMap<Long, Fun.Pair>();

    /** All not-yet commited records in store */
    protected final ConcurrentNavigableMap<Long,Fun.Pair> rollback
            = new ConcurrentSkipListMap<Long, Fun.Pair>();


    /** Queue of deleted recids, those are reused for new records */
    protected final Queue<Long> freeRecids = new ConcurrentLinkedQueue<Long>();

    /** Maximal returned recid, incremented if there are no free recids*/
    protected final AtomicLong maxRecid = new AtomicLong(RECID_LAST_RESERVED);

    public StoreHeap(){
        super(null, null, false,false,null);
        for(long recid=1;recid<=RECID_LAST_RESERVED;recid++){
            records.put(recid, TOMBSTONE);
        }
    }



    @Override
    public long preallocate() {
        Long recid = freeRecids.poll();
        if(recid==null) recid = maxRecid.incrementAndGet();
        records.put(recid,TOMBSTONE);
        return recid;
    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        if(serializer == null)
            throw new NullPointerException();
        if(value==null) value= (A) NULL;
        final Lock lock = locks[new Random().nextInt(locks.length)].writeLock();
        lock.lock();

        try{
            Long recid = freeRecids.poll();
            if(recid==null) recid = maxRecid.incrementAndGet();
            records.put(recid, new Fun.Pair<Object, Serializer>(value,serializer));
            rollback.put(recid, new Fun.Pair(TOMBSTONE,serializer ));
            if(CC.PARANOID && ! (recid>0))
                throw new AssertionError();
            return recid;
        }finally{
            lock.unlock();
        }
    }

    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        if(serializer == null)
            throw new NullPointerException();
        if(CC.PARANOID && ! (recid>0))
            throw new AssertionError();
        final Lock lock = locks[Store.lockPos(recid)].readLock();
        lock.lock();

        try{
            //get from commited records
            Fun.Pair t = records.get(recid);
            if(t==null)
                throw new DBException(DBException.Code.ENGINE_GET_VOID);

            if(t.a==NULL)
                return null;
            return (A) t.a;
        }finally{
            lock.unlock();
        }
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        if(serializer == null)
            throw new NullPointerException();
        if(CC.PARANOID && ! (recid>0))
            throw new AssertionError();
        if(CC.PARANOID && ! (serializer!=null))
            throw new AssertionError();
        if(CC.PARANOID && ! (recid>0))
            throw new AssertionError();
        if(value==null) value= (A) NULL;
        final Lock lock = locks[Store.lockPos(recid)].writeLock();
        lock.lock();

        try{
            Fun.Pair old = records.put(recid, new Fun.Pair<Object, Serializer>(value,serializer));
            if(old!=null) //TODO null if record was preallocated
                rollback.putIfAbsent(recid,old);
        }finally{
             lock.unlock();
        }
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        if(serializer == null)
            throw new NullPointerException();
        if(CC.PARANOID && ! (recid>0))
            throw new AssertionError();
        final Lock lock = locks[Store.lockPos(recid)].writeLock();
        lock.lock();

        try{
            Fun.Pair old = expectedOldValue==null? TOMBSTONE : new Fun.Pair(expectedOldValue, serializer);
            Fun.Pair newPair = newValue==null? TOMBSTONE : new Fun.Pair(newValue,serializer);
            boolean ret =  records.replace(recid, old, newPair);
            if(ret) rollback.putIfAbsent(recid,old);
            return ret;
        }finally{
            lock.unlock();
        }
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        if(serializer == null)
            throw new NullPointerException();
        if(CC.PARANOID && ! (recid>0))
            throw new AssertionError();
        final Lock lock = locks[Store.lockPos(recid)].writeLock();
        lock.lock();

        try{
            Fun.Pair t2 = records.put(recid,TOMBSTONE);
            if(t2!=null) rollback.putIfAbsent(recid,t2);
        }finally{
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lockAllWrite();
        try{
            records.clear();
            freeRecids.clear();
            rollback.clear();
        }finally{
            unlockAllWrite();
        }
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void commit() {
        lockAllWrite();
        try{
            rollback.clear();
        }finally{
            unlockAllWrite();
        }
    }

    @Override
    public void rollback() throws UnsupportedOperationException {
        lockAllWrite();
        try{
            //put all stuff from `rollback` into `records`
            for(Map.Entry<Long,Fun.Pair> e:rollback.entrySet()){
                Long recid = e.getKey();
                Fun.Pair val = e.getValue();
                if(val == TOMBSTONE) records.remove(recid);
                else records.put(recid, val);
            }
            rollback.clear();
        }finally{
            unlockAllWrite();
        }
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void clearCache() {
    }

    @Override
    public void compact() {
        lockAllWrite();
        try {
            if(!rollback.isEmpty()) {
                throw new DBException(DBException.Code.ENGINE_COMPACT_UNCOMMITED);
            }
            Iterator<Map.Entry<Long, Fun.Pair>> iter = records.entrySet().iterator();
            while (iter.hasNext()) {
                if (TOMBSTONE == iter.next().getValue())
                    iter.remove();
            }
        }finally {
            unlockAllWrite();
        }
    }

    @Override
    public boolean canRollback(){
        return true;
    }


    @Override
    public long getMaxRecid() {
        return maxRecid.get();
    }

    @Override
    public ByteBuffer getRaw(long recid) {
        Fun.Pair t = records.get(recid);
        if(t==null||t.a == null) return null;
        return ByteBuffer.wrap(serialize(t.a, (Serializer<Object>) t.b).copyBytes());
    }

    @Override
    public Iterator<Long> getFreeRecids() {
        return Collections.unmodifiableCollection(freeRecids).iterator();
    }

    @Override
    public void updateRaw(long recid, ByteBuffer data) {
        throw new UnsupportedOperationException("can not put raw data into StoreHeap");
    }

    @Override
    public long getSizeLimit() {
        return 0;
    }

    @Override
    public long getCurrSize() {
        return records.size();
    }

    @Override
    public long getFreeSize() {
        return 0;
    }

    @Override
    public String calculateStatistics() {
        return null;
    }
}
