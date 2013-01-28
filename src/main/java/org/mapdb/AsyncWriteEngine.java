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
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * {@link Engine} wrapper which provides asynchronous serialization and write.
 *  This class takes an object instance, passes it to background thread (using Queue)
 *  where it is serialized and written to disk.
 * <p/>
 * Async write does not affect commit durability, write queue is flushed before each commit.
 *
 * @author Jan Kotek
 */
public class AsyncWriteEngine extends EngineWrapper implements Engine {

    protected final boolean powerSavingMode;
    protected final int flushDelay;

    @SuppressWarnings({ "rawtypes" })
    protected static final class WriteItem{
        protected volatile Object value;
        protected volatile Serializer serializer;

        public WriteItem(Object value, Serializer serializer) {
            this.value = value;
            this.serializer = serializer;
        }
    }

    /** thread naming utility */
    protected static final AtomicInteger threadCounter = new AtomicInteger();

    private final int threadNum = threadCounter.incrementAndGet();

    /** signals that object was deleted */
    protected static final Object DELETED = new Object();
    protected static final Object DONE = new Object();


    /** background Writer Thread */
    protected final Thread writerThread = new Thread("MapDB writer #"+threadNum){

                ;

        @SuppressWarnings("unchecked")
		@Override
        public void run() {
            try{
                for(;;){
                    if(throwed!=null) return; //second thread failed

                    LongMap.LongMapIterator<WriteItem> iter = writeCache.longMapIterator();
                    if(!iter.moveToNext()){
                        LockSupport.parkNanos(10000); //TODO power saving notification here
                    }else do{

                        final long recid = iter.key();
                        //get the latest version of this item
                        final WriteItem item = iter.value();
                        synchronized ( item){
                            if(item.value == DONE) throw new InternalError();
                            if(item.value == DELETED){
                                engine.delete(recid); //item was deleted in main thread
                            }else{
                                engine.update(recid, item.value, item.serializer);
                            }
                            item.value = DONE;
                            iter.remove();
                        }

                    }while(iter.moveToNext());

                    //check if we can exit, see javadoc at setParentEngineReference
                    if(parentEngineWeakRef!=null && parentEngineWeakRef.get()==null && writeCache.isEmpty()){
                        //parent engine was GCed, no more items will be added and backlog is empty.
                        //No point to live anymore, so lets kill writer thread
                        throwed = new Error("Parent engine was GCed. No more items should be added");
                        return;
                    }

                    if(flushDelay>0)
                        Thread.sleep(flushDelay);
                }
            }catch(Throwable e){
                //store reason why we failed, so user can be notified
                //TODO logging here?
                throwed = e;
            }finally{
                //signal Close method that Writer Thread is down
                writerThreadDown.countDown();
            }

        }
    };

    BlockingQueue<Long> preallocRecids = new ArrayBlockingQueue<Long>(128);

    /** thread which preallocate recid for `put` operation */
    protected final Thread preallocThread = new Thread("MapDB prealloc #"+threadNum){
        {
            setDaemon(true);
            //TODO better way to shutdown this thread
            //TODO preallocated recids should be reclaimed on Engine.close()
        }

        @Override public void run() {
            try{
                for(;;){
                    if(throwed!=null) return;
                    Long recid = AsyncWriteEngine.super.put(null, Serializer.NULL_SERIALIZER);
                    preallocRecids.put(recid);
                }
            }catch(Throwable e){
                //store reason why we failed, so user can be notified
                //TODO logging here?
                throwed = e;
            }
    }
    };

    /** signals that Writer Thread quit*/
    protected final CountDownLatch writerThreadDown = new CountDownLatch(1);

    /** indicates if Writer Thread has been started, used for lazy thread start */
    protected final AtomicBoolean writerThreadRunning = new AtomicBoolean(false);

    /**
     * If exception is thrown in Writer Thread, it dies and exception goes here.
     * Then it is forwarded to user every time it tries to modify records.
     */
    protected Throwable throwed = null;

    /** lock used with commit/rollback/close operation */
    protected final ReentrantReadWriteLock commitLock = new ReentrantReadWriteLock();

    /**
     * Items which are queued for writing.
     */
    protected final LongConcurrentHashMap<WriteItem> writeCache = new LongConcurrentHashMap<WriteItem>();



    /**
     * @param engine into which writes will be forward to
     * @param asyncThreadDaemon passed to {@ling Thread@setDaemon(boolean)} on Writer Thread
     * @param powerSavingMode if true, disable periodic checks if parent engine was GCed.
     * @param flushDelay when Write Queue becomes empty, pause Writer Thread for given delay
     */
    public AsyncWriteEngine(Engine engine, boolean asyncThreadDaemon, boolean powerSavingMode, int flushDelay) {
        super(engine);
        this.powerSavingMode = powerSavingMode;
        this.flushDelay = flushDelay;
        writerThread.setDaemon(asyncThreadDaemon);
    }

    /**
     * Check if Writer Thread was started, if not start it.
     */
    protected void checkAndStartWriter(){
        if(throwed!=null) throw new RuntimeException("Writer Thread failed with an exception.",throwed);

        if(!writerThreadRunning.get() && writerThreadRunning.compareAndSet(false, true)){
            writerThread.start();
            preallocThread.start();
        }
    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        checkAndStartWriter();
        try {
            long recid = preallocRecids.take();
            update(recid, value,serializer);
            return recid;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
	@Override
    public <A> A get(long recid, Serializer<A> serializer) {
        commitLock.readLock().lock();
        try{
            for(;;){
                WriteItem item = writeCache.get(recid);
                if(item == null){
                    return super.get(recid, serializer);
                }else synchronized (item){
                    if(item.value==DONE || item.serializer!=serializer) continue;
                    else if(item.value==DELETED) return null;
                    else return (A) item.value;
                }
            }
        }finally{
            commitLock.readLock().unlock();
        }
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        checkAndStartWriter();
        commitLock.readLock().lock();
        try{ for(;;){
            WriteItem item = writeCache.get(recid);
            if(item == null){
                if(writeCache.putIfAbsent(recid, new WriteItem(value, serializer)) == null){
                    return;
                }else{
                    continue; //there was conflict, so try again latter
                }
            }else{
                synchronized (item){
                    if(item.value==DONE) continue;
                    item.serializer = serializer;
                    item.value = value;
                    return;
                }
            }
        }

        }finally{
            commitLock.readLock().unlock();
        }
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        checkAndStartWriter();

        commitLock.readLock().lock();
        try{
            for(;;){
                WriteItem item = writeCache.get(recid);
                if(item == null){
                    //not in write queue, so get old value
                    A oldValue = super.get(recid, serializer);
                    if(oldValue == expectedOldValue || (oldValue!=null && oldValue.equals(expectedOldValue))){
                        if(writeCache.putIfAbsent(recid, new WriteItem(newValue, serializer)) == null){
                            return true;
                        }else{
                            continue; //there was conflict, so try again latter
                        }
                    }else{
                        //old value did not matched, so fail
                        return false;
                    }
                }else{
                    //previous value was found in write cache, try to update it
                    synchronized (item){
                        if(item.value==DONE) continue;
                        if(item.value == expectedOldValue || (item.value!=null && item.value.equals(expectedOldValue))){
                            //match, update stuff
                            item.serializer = serializer;
                            item.value = newValue;
                            return true;
                        }else{
                            return false;
                        }
                    }
                }

            }
        }finally{
            commitLock.writeLock().unlock();
        }
    }

    @Override
    public void delete(long recid) {
        update(recid, DELETED, null );
    }

    @Override
    public void commit() {
        commitLock.writeLock().lock();
        try{
            while(!writeCache.isEmpty())
                LockSupport.parkNanos(100);
            super.commit();
        }finally{
            commitLock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        commitLock.writeLock().lock();
        try{
            while(!writeCache.isEmpty())
                LockSupport.parkNanos(100);
            //TODO close thread
            super.close();
        }finally{
            commitLock.writeLock().unlock();
        }
    }

    @Override
    public void rollback() {
        commitLock.writeLock().lock();
        try{
            while(!writeCache.isEmpty())
                LockSupport.parkNanos(100);
            //TODO clear cache directly?
            super.rollback();
        }finally{
            commitLock.writeLock().unlock();
        }
    }


    protected WeakReference<Engine> parentEngineWeakRef = null;

    /**
     * Main thread may die, leaving Writer Thread orphaned.
     * To prevent this we periodically check if WeakReference was GCed.
     * This method sets WeakReference to user facing Engine,
     * if this instance if GCed it means that user may no longer manage
     * and we can exit Writer Thread.
     *
     * @param parentEngineReference reference to user facing Engine
     */
    public void setParentEngineReference(Engine parentEngineReference) {
        parentEngineWeakRef = new WeakReference<Engine>(parentEngineReference);
    }

}
