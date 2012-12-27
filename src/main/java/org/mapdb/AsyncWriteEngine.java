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

import java.lang.ref.WeakReference;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 *
 * @author Jan Kotek
 */
public class AsyncWriteEngine extends EngineWrapper implements Engine {

    protected final boolean powerSavingMode;

    protected static final class WriteItem{
        final long recid;
        final Object value;
        final Serializer serializer;

        public WriteItem(long recid, Object value, Serializer serializer) {
            this.recid = recid;
            this.value = value;
            this.serializer = serializer;
        }

        @Override public boolean equals(Object obj){
            if(obj instanceof WriteItem){
                WriteItem obj2 = (WriteItem) obj;
                return obj2.serializer == serializer && obj2.value == value;
            }else{
                return false;
            }
        }
    }

    protected static final Object DELETED = new Object();
    protected static final WriteItem SHUTDOWN =  new WriteItem(-2, null, null);

    protected static final AtomicInteger threadCounter = new AtomicInteger();
    protected final Thread writerThread = new Thread("MapDB writer #"+threadCounter.incrementAndGet()){
        @Override
        public void run() {
            try{
                for(;;){
                    WriteItem item = powerSavingMode?
                            writeQueue.take() :
                            writeQueue.poll(1000, TimeUnit.SECONDS);
                    if(item == SHUTDOWN) return;
                    if(item != null)try{
                        grandLock.readLock().lock();

                        //get the latest version of this item
                        item = writeCache.get(item.recid);
                        if(item == null){
                            //item was already written, do nothing
                        }else{
                            if(item.value == DELETED) engine.recordDelete(item.recid);
                            else engine.recordUpdate(item.recid, item.value, item.serializer);
                            if(!writeCache.remove(item.recid, item)){ //remove if was not modified while updating store
                                //was not removed, so schedule next round
                                writeQueue.put(item);
                            }
                        }

                    }finally{
                        grandLock.readLock().unlock();
                    }

                    if(parentEngineWeakRef!=null && parentEngineWeakRef.get()==null && writeQueue.isEmpty()){
                        //parent engine was GCed, no more items will be added and backlog is empty.
                        //No point to live anymore, so lets kill writer thread
                        throwed = new Error("Parent engine was GCed. No more items should be added");
                        return;
                    }
                }
            }catch(Throwable e){
                throwed = e;
            }finally{
                writerThreadDown.countDown();
            }

        }
    };

    /** signals that writer thread quit*/
    protected final CountDownLatch writerThreadDown = new CountDownLatch(1);
    protected final AtomicBoolean writerThreadRunning = new AtomicBoolean(false);
    protected Throwable throwed = null;
    protected final ReentrantReadWriteLock grandLock = new ReentrantReadWriteLock();


    protected final LongConcurrentHashMap<WriteItem> writeCache = new LongConcurrentHashMap<WriteItem>();
    protected final BlockingQueue<WriteItem> writeQueue = new LinkedTransferQueue<WriteItem>();



    public AsyncWriteEngine(Engine engine, boolean asyncThreadDaemon, boolean powerSavingMode) {
        super(engine);
        this.powerSavingMode = powerSavingMode;
        writerThread.setDaemon(asyncThreadDaemon);
    }

    protected void checkAndStartWriter(){
        if(throwed!=null) throw new RuntimeException("Writer Thread failed with an exception.",throwed);

        if(!writerThreadRunning.get() && writerThreadRunning.compareAndSet(false, true)){
            writerThread.start();
        }
    }

    @Override
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        grandLock.readLock().lock();
        try{
            WriteItem item = writeCache.get(recid);
            if(item == null){
                A a =  super.recordGet(recid, serializer);
                item = writeCache.get(recid); //check one more time for update
                return item==null? a : (A) item.value;
            }else if(item.value == DELETED){
                return null;
            }else if(item.serializer == serializer){
                return (A) item.value;
            }else{
                //pause until item is in cache
                //TODO this just sucks
                while(writeCache.containsKey(recid)){}
                return recordGet(recid,serializer);
            }
        }finally{
            grandLock.readLock().unlock();
        }
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        checkAndStartWriter();
        WriteItem item = new WriteItem(recid, value, serializer);
        grandLock.readLock().lock();
        try{
            writeCache.put(recid, item);
            writeQueue.add(item);
        }finally{
            grandLock.readLock().unlock();
        }


    }

    @Override
    public <A> boolean recordCompareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        checkAndStartWriter();
        final WriteItem expectedEntry = new WriteItem(recid, expectedOldValue, serializer);
        final WriteItem newEntry = new WriteItem(recid, newValue, serializer);
        if(writeCache.replace(recid, expectedEntry, newEntry)) return true;

        //simple SWAP would not work, so lock down the world and do it hard way
        grandLock.writeLock().lock();
        try{
            //check writeCache again
            if(writeCache.containsKey(recid)){
                return writeCache.replace(recid, expectedEntry, newEntry);
            }
            //no, do it hard (binary way
            return super.recordCompareAndSwap(recid, expectedOldValue, newValue, serializer);
        }finally{
            grandLock.writeLock().unlock();
        }
    }

    @Override
    public void recordDelete(long recid) {
        recordUpdate(recid, DELETED, null);
    }

    @Override
    public void commit() {
        //TODO flush write cache
        super.commit();
    }

    @Override
    public void close() {
        if(writerThreadRunning.get()){
            writeQueue.add(SHUTDOWN);
            try {
                writerThreadDown.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        super.close();
    }

    @Override
    public void rollback() {
        super.rollback();
    }


    protected WeakReference parentEngineWeakRef = null;

    public void setParentEngineReference(Engine parentEngineReference) {
        parentEngineWeakRef = new WeakReference<Engine>(parentEngineReference);
    }

}
