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

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Naive implementation of Snapshots on top of StorageEngine.
 * On update it takes old value and stores it aside.
 * <p>
 * TODO merge snapshots down with Storage for best performance
 *
 * @author Jan Kotek
 */
public class TxEngine extends EngineWrapper {

    protected static final Object TOMBSTONE = new Object();

    protected final ReentrantReadWriteLock commitLock = new ReentrantReadWriteLock(CC.FAIR_LOCKS);
    protected final ReentrantReadWriteLock[] locks = new ReentrantReadWriteLock[CC.CONCURRENCY];
    {
        for(int i=0;i<locks.length;i++) locks[i] = new ReentrantReadWriteLock(CC.FAIR_LOCKS);
    }


    protected volatile boolean uncommitedData = false;

    protected Set<Reference<Tx>> txs = new LinkedHashSet<Reference<Tx>>();
    protected ReferenceQueue<Tx> txQueue = new ReferenceQueue<Tx>();

    protected final boolean fullTx;

    protected final Queue<Long> preallocRecids;

    protected final int PREALLOC_RECID_SIZE = 128;

    protected TxEngine(Engine engine, boolean fullTx) {
        super(engine);
        this.fullTx = fullTx;
        this.preallocRecids = fullTx ? new ArrayBlockingQueue<Long>(PREALLOC_RECID_SIZE) : null;
    }

    protected Long preallocRecidTake() {
        if(CC.PARANOID && ! (commitLock.isWriteLockedByCurrentThread()))
            throw new AssertionError();
        Long recid = preallocRecids.poll();
        if(recid!=null) return recid;

        if(uncommitedData)
            throw new IllegalAccessError("uncommited data");

        for(int i=0;i<PREALLOC_RECID_SIZE;i++){
            preallocRecids.add(super.preallocate());
        }
        recid = super.preallocate();
        super.commit();
        uncommitedData = false;
        return recid;
    }

    public static Engine createSnapshotFor(Engine engine) {
        if(engine.isReadOnly())
            return engine;
        if(engine instanceof TxEngine)
            return ((TxEngine)engine).snapshot();
        if(engine instanceof EngineWrapper)
            createSnapshotFor(((EngineWrapper) engine).getWrappedEngine());
        throw new UnsupportedOperationException("Snapshots are not enabled, use DBMaker.snapshotEnable()");
    }

    @Override
    public boolean canSnapshot() {
        return true;
    }

    @Override
    public Engine snapshot() {
        commitLock.writeLock().lock();
        try {
            cleanTxQueue();
            if(uncommitedData && canRollback())
                throw new IllegalAccessError("Can not create snapshot with uncommited data");
            return new Tx();
        } finally {
            commitLock.writeLock().unlock();
        }
    }

    protected void cleanTxQueue(){
        if(CC.PARANOID && ! (commitLock.writeLock().isHeldByCurrentThread()))
            throw new AssertionError();
        for(Reference<? extends Tx> ref = txQueue.poll(); ref!=null; ref=txQueue.poll()){
            txs.remove(ref);
        }
    }

    @Override
    public long preallocate() {
        commitLock.writeLock().lock();
        try {
            uncommitedData = true;
            long recid =  super.preallocate();
            Lock lock = locks[Store.lockPos(recid)].writeLock();
            lock.lock();
            try{
                for(Reference<Tx> txr:txs){
                    Tx tx = txr.get();
                    if(tx==null) continue;
                    tx.old.putIfAbsent(recid,TOMBSTONE);
                }
            }finally {
                lock.unlock();
            }
            return recid;
        } finally {
            commitLock.writeLock().unlock();
        }
    }

    @Override
    public void preallocate(long[] recids) {
        commitLock.writeLock().lock();
        try {
            uncommitedData = true;
            super.preallocate(recids);
            for(long recid:recids){
                Lock lock = locks[Store.lockPos(recid)].writeLock();
                lock.lock();
                try{
                    for(Reference<Tx> txr:txs){
                        Tx tx = txr.get();
                        if(tx==null) continue;
                        tx.old.putIfAbsent(recid,TOMBSTONE);
                    }
                }finally {
                    lock.unlock();
                }
            }
        } finally {
            commitLock.writeLock().unlock();
        }
    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        commitLock.readLock().lock();
        try {
            uncommitedData = true;
            long recid = super.put(value, serializer);
            Lock lock = locks[Store.lockPos(recid)].writeLock();
            lock.lock();
            try{
                for(Reference<Tx> txr:txs){
                    Tx tx = txr.get();
                    if(tx==null) continue;
                    tx.old.putIfAbsent(recid,TOMBSTONE);
                }
            }finally {
                lock.unlock();
            }

            return recid;
        } finally {
            commitLock.readLock().unlock();
        }
    }


    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        commitLock.readLock().lock();
        try {
            return super.get(recid, serializer);
        } finally {
            commitLock.readLock().unlock();
        }
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        commitLock.readLock().lock();
        try {
            uncommitedData = true;
            Lock lock = locks[Store.lockPos(recid)].writeLock();
            lock.lock();
            try{
                Object old = get(recid,serializer);
                for(Reference<Tx> txr:txs){
                    Tx tx = txr.get();
                    if(tx==null) continue;
                    tx.old.putIfAbsent(recid,old);
                }
                super.update(recid, value, serializer);
            }finally {
                lock.unlock();
            }
        } finally {
            commitLock.readLock().unlock();
        }

    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        commitLock.readLock().lock();
        try {
            uncommitedData = true;
            Lock lock = locks[Store.lockPos(recid)].writeLock();
            lock.lock();
            try{
                boolean ret = super.compareAndSwap(recid, expectedOldValue, newValue, serializer);
                if(ret){
                    for(Reference<Tx> txr:txs){
                        Tx tx = txr.get();
                        if(tx==null) continue;
                        tx.old.putIfAbsent(recid,expectedOldValue);
                    }
                }
                return ret;
            }finally {
                lock.unlock();
            }
        } finally {
            commitLock.readLock().unlock();
        }

    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        commitLock.readLock().lock();
        try {
            uncommitedData = true;
            Lock lock = locks[Store.lockPos(recid)].writeLock();
            lock.lock();
            try{
                Object old = get(recid,serializer);
                for(Reference<Tx> txr:txs){
                    Tx tx = txr.get();
                    if(tx==null) continue;
                    tx.old.putIfAbsent(recid,old);
                }
                super.delete(recid, serializer);
            }finally {
                lock.unlock();
            }
        } finally {
            commitLock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        commitLock.writeLock().lock();
        try {
            super.close();
        } finally {
            commitLock.writeLock().unlock();
        }

    }

    @Override
    public void commit() {
        commitLock.writeLock().lock();
        try {
            cleanTxQueue();
            super.commit();
            uncommitedData = false;
        } finally {
            commitLock.writeLock().unlock();
        }

    }

    @Override
    public void rollback() {
        commitLock.writeLock().lock();
        try {
            cleanTxQueue();
            super.rollback();
            uncommitedData = false;
        } finally {
            commitLock.writeLock().unlock();
        }

    }

    protected void superCommit() {
        if(CC.PARANOID && ! (commitLock.isWriteLockedByCurrentThread()))
            throw new AssertionError();
        super.commit();
    }

    protected <A> void superUpdate(long recid, A value, Serializer<A> serializer) {
        if(CC.PARANOID && ! (commitLock.isWriteLockedByCurrentThread()))
            throw new AssertionError();
        super.update(recid,value,serializer);
    }

    protected <A> void superDelete(long recid, Serializer<A> serializer) {
        if(CC.PARANOID && ! (commitLock.isWriteLockedByCurrentThread()))
            throw new AssertionError();
        super.delete(recid,serializer);
    }

    protected <A> A superGet(long recid, Serializer<A> serializer) {
        if(CC.PARANOID && ! (commitLock.isWriteLockedByCurrentThread()))
            throw new AssertionError();
        return super.get(recid,serializer);
    }

    public class Tx implements Engine{

        protected LongConcurrentHashMap old = new LongConcurrentHashMap();
        protected LongConcurrentHashMap<Fun.Pair> mod =
                fullTx ? new LongConcurrentHashMap<Fun.Pair>() : null;

        protected Collection<Long> usedPreallocatedRecids =
                fullTx ? new ArrayList<Long>() : null;

        protected final Reference<Tx> ref = new WeakReference<Tx>(this,txQueue);

        protected boolean closed = false;
        private Store parentEngine;

        public Tx(){
            if(CC.PARANOID && ! (commitLock.isWriteLockedByCurrentThread()))
                throw new AssertionError();
            txs.add(ref);
        }

        @Override
        public long preallocate() {
            if(!fullTx)
                throw new UnsupportedOperationException("read-only");

            commitLock.writeLock().lock();
            try{
                Long recid = preallocRecidTake();
                usedPreallocatedRecids.add(recid);
                return recid;
            }finally {
                commitLock.writeLock().unlock();
            }
        }

        @Override
        public void preallocate(long[] recids) {
            if(!fullTx)
                throw new UnsupportedOperationException("read-only");

            commitLock.writeLock().lock();
            try{
                for(int i=0;i<recids.length;i++){
                    Long recid = preallocRecidTake();
                    usedPreallocatedRecids.add(recid);
                    recids[i] = recid;
                }
            }finally {
                commitLock.writeLock().unlock();
            }
        }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        if(!fullTx)
            throw new UnsupportedOperationException("read-only");
        commitLock.writeLock().lock();
        try{
            Long recid = preallocRecidTake();
            usedPreallocatedRecids.add(recid);
            mod.put(recid, new Fun.Pair(value,serializer));
            return recid;
        }finally {
            commitLock.writeLock().unlock();
        }
    }

    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        commitLock.readLock().lock();
        try{
            if(closed) throw new IllegalAccessError("closed");
            Lock lock = locks[Store.lockPos(recid)].readLock();
            lock.lock();
            try{
                return getNoLock(recid, serializer);
            }finally {
                lock.unlock();
            }
        }finally {
            commitLock.readLock().unlock();
        }
    }

    private <A> A getNoLock(long recid, Serializer<A> serializer) {
        if(fullTx){
            Fun.Pair tu = mod.get(recid);
            if(tu!=null){
                if(tu.a==TOMBSTONE)
                    return null;
                return (A) tu.a;
            }
        }

        Object oldVal = old.get(recid);
        if(oldVal!=null){
            if(oldVal==TOMBSTONE)
                return null;
            return (A) oldVal;
        }
        return TxEngine.this.get(recid, serializer);
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        if(!fullTx)
            throw new UnsupportedOperationException("read-only");
        commitLock.readLock().lock();
        try{
            mod.put(recid, new Fun.Pair(value,serializer));
        }finally {
            commitLock.readLock().unlock();
        }
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        if(!fullTx)
            throw new UnsupportedOperationException("read-only");

        commitLock.readLock().lock();
        try{

            Lock lock = locks[Store.lockPos(recid)].writeLock();
            lock.lock();
            try{
                A oldVal = getNoLock(recid, serializer);
                boolean ret = oldVal!=null && oldVal.equals(expectedOldValue);
                if(ret){
                    mod.put(recid,new Fun.Pair(newValue,serializer));
                }
                return ret;
            }finally {
                lock.unlock();
            }
        }finally {
            commitLock.readLock().unlock();
        }
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        if(!fullTx)
            throw new UnsupportedOperationException("read-only");

        commitLock.readLock().lock();
        try{
            mod.put(recid,new Fun.Pair(TOMBSTONE,serializer));
        }finally {
            commitLock.readLock().unlock();
        }

    }

    @Override
    public void close() {
        closed = true;
        old.clear();
        ref.clear();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void commit() {
        if(!fullTx)
            throw new UnsupportedOperationException("read-only");

        commitLock.writeLock().lock();
        try{
            if(closed) return;
            if(uncommitedData)
                throw new IllegalAccessError("uncommitted data");
            txs.remove(ref);
            cleanTxQueue();

            if(pojo.hasUnsavedChanges())
                pojo.save(this);

            //check no other TX has modified our data
            LongMap.LongMapIterator oldIter = old.longMapIterator();
            while(oldIter.moveToNext()){
                long recid = oldIter.key();
                for(Reference<Tx> ref2:txs){
                    Tx tx = ref2.get();
                    if(tx==this||tx==null) continue;
                    if(tx.mod.containsKey(recid)){
                        close();
                        throw new TxRollbackException();
                    }
                }
            }

            LongMap.LongMapIterator<Fun.Pair> iter = mod.longMapIterator();
            while(iter.moveToNext()){
                long recid = iter.key();
                if(old.containsKey(recid)){
                    close();
                    throw new TxRollbackException();
                }
            }

            iter = mod.longMapIterator();
            while(iter.moveToNext()){
                long recid = iter.key();

                Fun.Pair val = iter.value();
                Serializer ser = (Serializer) val.b;
                Object old = superGet(recid,ser);
                if(old==null)
                    old = TOMBSTONE;
                for(Reference<Tx> txr:txs){
                    Tx tx = txr.get();
                    if(tx==null||tx==this) continue;
                    tx.old.putIfAbsent(recid,old);

                }

                if(val.a==TOMBSTONE){
                    superDelete(recid, ser);
                }else {
                    superUpdate(recid, val.a, ser);
                }
            }

            //there are no conflicts, so update the POJO in parent
            //TODO sort of hack, is it thread safe?
            getWrappedEngine().getSerializerPojo().registered = pojo.registered;
            superCommit();

            close();
        }finally {
            commitLock.writeLock().unlock();
        }
    }

    @Override
    public void rollback() throws UnsupportedOperationException {
        if(!fullTx)
            throw new UnsupportedOperationException("read-only");

        commitLock.writeLock().lock();
        try{
            if(closed) return;
            if(uncommitedData)
                throw new IllegalAccessError("uncommitted data");

            txs.remove(ref);
            cleanTxQueue();

            for(Long prealloc:usedPreallocatedRecids){
                TxEngine.this.superDelete(prealloc,null);
            }
            TxEngine.this.superCommit();

            close();
        }finally {
            commitLock.writeLock().unlock();
        }
    }

    @Override
    public boolean isReadOnly() {
        return !fullTx;
    }

    @Override
    public boolean canRollback() {
        return fullTx;
    }

        @Override
        public boolean canSnapshot() {
            return false;
        }

        @Override
        public Engine snapshot() throws UnsupportedOperationException {
            throw new UnsupportedOperationException();
            //TODO see Issue #281
        }

        @Override
    public void clearCache() {
    }

    @Override
    public void compact() {
    }


    SerializerPojo pojo = new SerializerPojo((CopyOnWriteArrayList<SerializerPojo.ClassInfo>) TxEngine.this.getSerializerPojo().registered.clone());

    @Override
    public SerializerPojo getSerializerPojo() {
        return pojo;
    }


        public Engine getWrappedEngine() {
        return TxEngine.this.getWrappedEngine();
    }

    }

}
