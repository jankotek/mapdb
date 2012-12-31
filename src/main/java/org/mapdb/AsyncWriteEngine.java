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

    @SuppressWarnings({ "rawtypes" })
    protected static final class WriteItem{
        final Object value;
        final Serializer serializer;

        public WriteItem(Object value, Serializer serializer) {

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

    /** signals that object was deleted */
    protected static final WriteItem DELETED = new WriteItem(null, null);
    /** signals that <code>Engine</code> has been closed and Write Thread should terminate. */
    protected static final Long SHUTDOWN = Long.MIN_VALUE;

    /** thread naming utility */
    protected static final AtomicInteger threadCounter = new AtomicInteger();

    /** background Writer Thread */
    protected final Thread writerThread = new Thread("MapDB writer #"+threadCounter.incrementAndGet()){
        @SuppressWarnings("unchecked")
		@Override
        public void run() {
            try{
                for(;;){
                    // Take item from Queue
                    Long recid = (powerSavingMode || parentEngineWeakRef==null)?
                            writeQueue.take() :
                            writeQueue.poll(1000, TimeUnit.SECONDS);
                    //check if this Engine was closed, in that case exit this thread
                    if(recid == SHUTDOWN) return;

                    //may be null if timeout expired
                    if(recid != null)try{
                        grandLock.readLock().lock();

                        //get the latest version of this item
                        WriteItem item = writeCache.get(recid);
                        if(item == null){
                            //item was already written, do nothing
                        }else{

                            if(item == DELETED){
                                engine.delete(recid); //item was deleted in main thread
                            }else{
                                engine.update(recid, item.value, item.serializer);
                            }
                            //now remove item from writeCache,
                            // but only if it was not modified from Main Thread,
                            // while we wrote it into engine.
                            if(!writeCache.remove(recid, item)){
                                //item was modified , so schedule new write
                                writeQueue.put(recid);
                            }
                        }

                    }finally{
                        grandLock.readLock().unlock();
                    }

                    //check if we can exit, see javadoc at setParentEngineReference
                    if(parentEngineWeakRef!=null && parentEngineWeakRef.get()==null && writeQueue.isEmpty()){
                        //parent engine was GCed, no more items will be added and backlog is empty.
                        //No point to live anymore, so lets kill writer thread
                        throwed = new Error("Parent engine was GCed. No more items should be added");
                        return;
                    }
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

    /** signals that Writer Thread quit*/
    protected final CountDownLatch writerThreadDown = new CountDownLatch(1);

    /** indicates if Writer Thread has been started, used for lazy thread start */
    protected final AtomicBoolean writerThreadRunning = new AtomicBoolean(false);

    /**
     * If exception is thrown in Writer Thread, it dies and exception goes here.
     * Then it is forwarded to user every time it tries to modify records.
     */
    protected Throwable throwed = null;

    /** Grand lock, used for CAS and other stuff. */
    protected final ReentrantReadWriteLock grandLock = new ReentrantReadWriteLock();

    /**
     * Items which are queued for writing.
     */
    protected final LongConcurrentHashMap<WriteItem> writeCache = new LongConcurrentHashMap<WriteItem>();

    /**
     * Queue of recids scheduled for writing
     */
    protected final BlockingQueue<Long> writeQueue = new LinkedTransferQueue<Long>();


    /**
     * @param engine into which writes will be forward to
     * @param asyncThreadDaemon passed to {@ling Thread@setDaemon(boolean)} on Writer Thread
     * @param powerSavingMode if true, disable periodic checks if parent engine was GCed.
     */
    public AsyncWriteEngine(Engine engine, boolean asyncThreadDaemon, boolean powerSavingMode) {
        super(engine);
        this.powerSavingMode = powerSavingMode;
        writerThread.setDaemon(asyncThreadDaemon);
    }

    /**
     * Check if Writer Thread was started, if not start it.
     */
    protected void checkAndStartWriter(){
        if(throwed!=null) throw new RuntimeException("Writer Thread failed with an exception.",throwed);

        if(!writerThreadRunning.get() && writerThreadRunning.compareAndSet(false, true)){
            writerThread.start();
        }
    }

    @SuppressWarnings("unchecked")
	@Override
    public <A> A get(long recid, Serializer<A> serializer) {
        grandLock.readLock().lock();
        try{
            WriteItem item = writeCache.get(recid);
            if(item == null){
                A a =  super.get(recid, serializer);
                item = writeCache.get(recid); //check one more time for update
                return item==null? a : (A) item.value;
            }else if(item == DELETED){
                return null;
            }else if(item.serializer == serializer){
                return (A) item.value;
            }else{
                //pause until item is in cache
                //TODO this just sucks
                while(writeCache.containsKey(recid)){}
                return get(recid, serializer);
            }
        }finally{
            grandLock.readLock().unlock();
        }
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        checkAndStartWriter();
        WriteItem item = new WriteItem(value, serializer);
        grandLock.readLock().lock();
        try{
            writeCache.put(recid, item);
            writeQueue.add(recid);
        }finally{
            grandLock.readLock().unlock();
        }
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        checkAndStartWriter();
        final WriteItem expectedEntry = new WriteItem(expectedOldValue, serializer);
        final WriteItem newEntry = new WriteItem(newValue, serializer);
        if(writeCache.replace(recid, expectedEntry, newEntry)) return true;

        //simple SWAP would not work, so lock down the world and do it hard way
        grandLock.writeLock().lock();
        try{
            //check writeCache again
            if(writeCache.containsKey(recid)){
                return writeCache.replace(recid, expectedEntry, newEntry);
            }
            //no, do it hard (binary way
            return super.compareAndSwap(recid, expectedOldValue, newValue, serializer);
        }finally{
            grandLock.writeLock().unlock();
        }
    }

    @Override
    public void delete(long recid) {
        checkAndStartWriter();
        grandLock.readLock().lock();
        try{
            writeCache.put(recid, DELETED);
            writeQueue.add(recid);
        }finally{
            grandLock.readLock().unlock();
        }
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
        //TODO drop write cache here
        super.rollback();
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
