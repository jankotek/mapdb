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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;

/**
 * {@link Engine} wrapper which provides asynchronous serialization and asynchronous write.
 * This class takes an object instance, passes it to background writer thread (using Write Queue)
 * where it is serialized and written to disk. Async write does not affect commit durability,
 * Write Queue is flushed into disk on each commit. Modified records are held in small instance cache,
 * until they are written into disk.
 *
 * This feature is disabled by default and can be enabled by calling {@link DBMaker#asyncWriteEnable()}.
 * Write Cache is flushed in regular intervals or when it becomes full. Flush interval is 100 ms by default and
 * can be controlled by {@link DBMaker#asyncWriteFlushDelay(int)}. Increasing this interval may improve performance
 * in scenarios where frequently modified items should be cached, typically {@link BTreeMap} import where keys
 * are presorted.
 *
 * Asynchronous write does not affect commit durability. Write Queue is flushed during each commit, rollback and close call.
 * Those method also block until all records are written.
 * You may flush Write Queue manually by using {@link org.mapdb.AsyncWriteEngine#clearCache()}  method.
 * There is global lock which prevents record being updated while commit is in progress.
 *
 * This wrapper starts one threads named {@code MapDB writer #N} (where N is static counter).
 * Async Writer takes modified records from Write Queue and writes them into store.
 * It also preallocates new recids, as finding empty {@code recids} takes time so small stash is pre-allocated.
 * It runs as {@code daemon}, so it does not prevent JVM to exit.
 *
 * Asynchronous Writes have several advantages (especially for single threaded user). But there are two things
 * user should be aware of:
 *
 *  * Because data are serialized on back-ground thread, they need to be thread safe or better immutable.
 *    When you insert record into MapDB and modify it latter, this modification may happen before item
 *    was serialized and you may not be sure what version was persisted
 *
 *  * Inter-thread communication has some overhead.
 *    There is also only single Writer Thread, which may create  single bottle-neck.
 *    This usually not issue for
 *    single or two threads, but in multi-threaded environment it may decrease performance.
 *    So in truly concurrent environments with many updates (network servers, parallel computing )
 *    you should keep Asynchronous Writes disabled.
 *
 *
 * @see Engine
 * @see EngineWrapper
 *
 * @author Jan Kotek
 *
 *
 *
 */
public class AsyncWriteEngine extends EngineWrapper implements Engine {

    /** ensures thread name is followed by number */
    protected static final AtomicLong threadCounter = new AtomicLong();


    /** used to signal that object was deleted*/
    protected static final Object TOMBSTONE = new Object();


    protected final int maxSize;

    protected final AtomicInteger size = new AtomicInteger();

//    protected final long[] newRecids = new long[CC.ASYNC_RECID_PREALLOC_QUEUE_SIZE];
//    protected int newRecidsPos = 0;
//    protected final ReentrantLock newRecidsLock = new ReentrantLock(CC.FAIR_LOCKS);


    /** Associates {@code recid} from Write Queue with record data and serializer. */
    protected final LongConcurrentHashMap<Fun.Pair<Object, Serializer>> writeCache
            = new LongConcurrentHashMap<Fun.Pair<Object, Serializer>>();

    /** Each insert to Write Queue must hold read lock.
     *  Commit, rollback and close operations must hold write lock
     */
    protected final ReentrantReadWriteLock commitLock = new ReentrantReadWriteLock(CC.FAIR_LOCKS);

    /** number of active threads running, used to await thread termination on close */
    protected final CountDownLatch activeThreadsCount = new CountDownLatch(1);

    /** If background thread fails with exception, it is stored here, and rethrown to all callers.*/
    protected volatile Throwable threadFailedException = null;

    /** indicates that {@code close()} was called and background threads are being terminated*/
    protected volatile boolean closeInProgress = false;

    /** flush Write Queue every N milliseconds  */
    protected final int asyncFlushDelay;

    protected final AtomicReference<CountDownLatch> action = new AtomicReference<CountDownLatch>(null);



    /**
     * Construct new class and starts background threads.
     * User may provide executor in which background tasks will be executed,
     * otherwise MapDB starts two daemon threads.
     *
     * @param engine which stores data.
     * @param _asyncFlushDelay flush Write Queue every N milliseconds
     * @param executor optional executor to run tasks. If null daemon threads will be created
     */
    public AsyncWriteEngine(Engine engine, int _asyncFlushDelay, int queueSize, Executor executor) {
        super(engine);
        this.asyncFlushDelay = _asyncFlushDelay;
        this.maxSize = queueSize;
        startThreads(executor);
    }

    public AsyncWriteEngine(Engine engine) {
        this(engine, CC.ASYNC_WRITE_FLUSH_DELAY, CC.ASYNC_WRITE_QUEUE_SIZE, null);
    }


    protected static final class WriterRunnable implements Runnable{

        protected final WeakReference<AsyncWriteEngine> engineRef;
        protected final long asyncFlushDelay;
        protected final AtomicInteger size;
        protected final int maxParkSize;
        private final ReentrantReadWriteLock commitLock;


        public WriterRunnable(AsyncWriteEngine engine) {
            this.engineRef = new WeakReference<AsyncWriteEngine>(engine);
            this.asyncFlushDelay = engine.asyncFlushDelay;
            this.commitLock = engine.commitLock;
            this.size = engine.size;
            this.maxParkSize = engine.maxSize/4;
        }

        @Override public void run() {
            try{
                //run in loop
                for(;;){

                    //$DELAY$
                    //if conditions are right, slow down writes a bit
                    if(asyncFlushDelay!=0 && !commitLock.isWriteLocked() && size.get()<maxParkSize){
                        //$DELAY$
                        LockSupport.parkNanos(1000L * 1000L * asyncFlushDelay);
                        //$DELAY$
                    }

                    AsyncWriteEngine engine = engineRef.get();
                    //$DELAY$
                    if(engine==null) return; //stop thread if this engine has been GCed
                    if(engine.threadFailedException !=null) return; //other thread has failed, no reason to continue
                    //$DELAY$
                    if(!engine.runWriter()) return;
                }
            } catch (Throwable e) {
                AsyncWriteEngine engine = engineRef.get();
                if(engine!=null) engine.threadFailedException = e;
            }finally {
                AsyncWriteEngine engine = engineRef.get();
                if(engine!=null) engine.activeThreadsCount.countDown();
            }
        }
    }

    /**
     * Starts background threads.
     * You may override this if you wish to start thread different way
     *
     * @param executor optional executor to run tasks, if null deamon threads will be created
     */
    protected void startThreads(Executor executor) {
        final Runnable writerRun = new WriterRunnable(this);
        //$DELAY$
        if(executor!=null){
            executor.execute(writerRun);
            return;
        }
        final long threadNum = threadCounter.incrementAndGet();
        Thread writerThread = new Thread(writerRun,"MapDB writer #"+threadNum);
        writerThread.setDaemon(true);
        //$DELAY$
        writerThread.start();
    }


    /** runs on background thread. Takes records from Write Queue, serializes and writes them.*/
    protected boolean runWriter() throws InterruptedException {
        final CountDownLatch latch = action.getAndSet(null);

        do{
            //$DELAY$
            LongMap.LongMapIterator<Fun.Pair<Object, Serializer>> iter = writeCache.longMapIterator();
            while(iter.moveToNext()){
                //$DELAY$
                //usual write
                final long recid = iter.key();
                Fun.Pair<Object, Serializer> item = iter.value();
                //$DELAY$
                if(item == null) continue; //item was already written
                if(item.a==TOMBSTONE){
                    //item was not updated, but deleted
                    AsyncWriteEngine.super.delete(recid, item.b);
                }else{
                    //call update as usual
                    AsyncWriteEngine.super.update(recid, item.a, item.b);
                }
                //record has been written to underlying Engine, so remove it from cache with CAS
                //$DELAY$
                if(writeCache.remove(recid, item)) {
                    //$DELAY$
                    size.decrementAndGet();
                }
                //$DELAY$
            }
        }while(latch!=null && !writeCache.isEmpty());


        //operations such as commit,close, compact or close needs to be executed in Writer Thread
        //for this case CountDownLatch is used, it also signals when operations has been completed
        //CountDownLatch is used as special case to signalise special operation
        if(latch!=null){
            if(CC.PARANOID && ! (writeCache.isEmpty()))
                throw new AssertionError();
            //$DELAY$
            final long count = latch.getCount();
            if(count == 0){ //close operation
                if(CC.LOG_EWRAP && LOG.isLoggable(Level.FINE))
                    LOG.fine("Async close finished");
                return false;
            }else if(count == 1){ //commit operation
                //$DELAY$
                AsyncWriteEngine.super.commit();
                if(CC.LOG_EWRAP && LOG.isLoggable(Level.FINE))
                    LOG.fine("Async commit finished");
                //$DELAY$
                latch.countDown();
            }else if(count==2){ //rollback operation
                //$DELAY$
                AsyncWriteEngine.super.rollback();
                if(CC.LOG_EWRAP && LOG.isLoggable(Level.FINE))
                    LOG.fine("Async rollback finished");
                latch.countDown();
                latch.countDown();
            }else if(count==3){ //compact operation
                AsyncWriteEngine.super.compact();
                //$DELAY$
                if(CC.LOG_EWRAP && LOG.isLoggable(Level.FINE))
                    LOG.fine("Async compact finished");
                latch.countDown();
                latch.countDown();
                latch.countDown();
                //$DELAY$
            }else{throw new AssertionError();}
        }
        //$DELAY$
        return true;
    }


    /** checks that background threads are ready and throws exception if not */
    protected void checkState() {
        //$DELAY$
        if(closeInProgress) throw new IllegalAccessError("db has been closed");
        if(threadFailedException !=null) throw new RuntimeException("Writer thread failed", threadFailedException);
        //$DELAY$
    }





    /**
     * {@inheritDoc}
     *
     * Recids are managed by underlying Engine. Finding free or allocating new recids
     * may take some time, so for this reason recids are preallocated by Writer Thread
     * and stored in queue. This method just takes preallocated recid from queue with minimal
     * delay.
     *
     * Newly inserted records are not written synchronously, but forwarded to background Writer Thread via queue.
     *
     */
    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        //$DELAY$
        int size2 = 0;
        long recid =0;
        commitLock.readLock().lock();
        try{
            //$DELAY$
            checkState();
            recid = preallocate();
            //$DELAY$
            if(writeCache.put(recid, new Fun.Pair(value, serializer))==null)
                //$DELAY$
                size2 = size.incrementAndGet();
            //$DELAY$
        }finally{
            commitLock.readLock().unlock();
        }
        //$DELAY$
        if(size2>maxSize) {
            //$DELAY$
            clearCache();
        }
        //$DELAY$
        return recid;
}


    /**
     * {@inheritDoc}
     *
     * This method first looks up into Write Cache if record is not currently being written.
     * If not it continues as usually
     *
     *
     */
    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        //$DELAY$
        commitLock.readLock().lock();
        //$DELAY$
        try{
            checkState();
            //$DELAY$
            Fun.Pair<Object,Serializer> item = writeCache.get(recid);
            if(item!=null){
                //$DELAY$
                if(item.a == TOMBSTONE) return null;
                return (A) item.a;
            }
            //$DELAY$
            return super.get(recid, serializer);
            //$DELAY$
        }finally{
            commitLock.readLock().unlock();
        }
        //$DELAY$
    }


    /**
     * {@inheritDoc}
     *
     * This methods forwards record into Writer Thread and returns asynchronously.
     *
     */
    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        int size2 = 0;
        //$DELAY$
        commitLock.readLock().lock();
        //$DELAY$
        try{
            checkState();
            if(writeCache.put(recid, new Fun.Pair(value, serializer))==null) {
                //$DELAY$
                size2 = size.incrementAndGet();
            }
        }finally{
            //$DELAY$
            commitLock.readLock().unlock();
        }
        if(size2>maxSize) {
            //$DELAY$
            clearCache();
        }
        //$DELAY$
    }

    /**
     * {@inheritDoc}
     *
     * This method first looks up Write Cache if record is not currently being written.
     * Successful modifications are forwarded to Write Thread and method returns asynchronously.
     * Asynchronicity does not affect atomicity.
     */
    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        int size2 = 0;
        boolean ret;
        //$DELAY$
        commitLock.writeLock().lock();
        //$DELAY$
        try{
            checkState();
            Fun.Pair<Object, Serializer> existing = writeCache.get(recid);
            //$DELAY$
            A oldValue = existing!=null? (A) existing.a : super.get(recid, serializer);
            //$DELAY$
            if(oldValue == expectedOldValue || (oldValue!=null && oldValue.equals(expectedOldValue))){
                //$DELAY$
                if(writeCache.put(recid, new Fun.Pair(newValue, serializer))==null) {
                    //$DELAY$
                    size2 = size.incrementAndGet();
                }
                ret = true;
            }else{
                ret = false;
            }
            //$DELAY$
        }finally{
            commitLock.writeLock().unlock();
        }
        //$DELAY$
        if(size2>maxSize) {
            clearCache();
        }
        //$DELAY$
        return ret;
    }

    /**
     * {@inheritDoc}
     *
     *  This method places 'tombstone' into Write Queue so record is eventually
     *  deleted asynchronously. However record is visible as deleted immediately.
     */
    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        update(recid, (A) TOMBSTONE, serializer);
    }

    /**
     * {@inheritDoc}
     *
     *  This method blocks until Write Queue is flushed and Writer Thread writes all records and finishes.
     *  When this method was called {@code closeInProgress} is set and no record can be modified.
     */
    @Override
    public void close() {
        //$DELAY$
        commitLock.writeLock().lock();
        try {
            //$DELAY$
            if(closeInProgress) return;
            //$DELAY$
            checkState();
            closeInProgress = true;
            //notify background threads
            if(!action.compareAndSet(null,new CountDownLatch(0)))
                throw new AssertionError();

            //wait for background threads to shutdown
            //$DELAY$
            while(!activeThreadsCount.await(1000,TimeUnit.MILLISECONDS)) {
                //$DELAY$
                //nothing here
            }

            AsyncWriteEngine.super.close();
            //$DELAY$
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            commitLock.writeLock().unlock();
        }
        //$DELAY$
    }



    protected void waitForAction(int actionNumber) {
        //$DELAY$
        commitLock.writeLock().lock();
        try{
            checkState();
            //notify background threads
            CountDownLatch msg = new CountDownLatch(actionNumber);
            //$DELAY$
            if(!action.compareAndSet(null,msg))
                throw new AssertionError();
            //$DELAY$

            //wait for response from writer thread
            while(!msg.await(100, TimeUnit.MILLISECONDS)){
                checkState();
            }
            //$DELAY$
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            commitLock.writeLock().unlock();
        }
        //$DELAY$
    }


    /**
     * {@inheritDoc}
     *
     *  This method blocks until Write Queue is flushed.
     *  All put/update/delete methods are blocked while commit is in progress (via global ReadWrite Commit Lock).
     *  After this method returns, commit lock is released and other operations may continue
     */
    @Override
    public void commit() {
        waitForAction(1);
    }

    /**
     * {@inheritDoc}
     *
     *  This method blocks until Write Queue is cleared.
     *  All put/update/delete methods are blocked while rollback is in progress (via global ReadWrite Commit Lock).
     *  After this method returns, commit lock is released and other operations may continue
     */
    @Override
    public void rollback() {
        waitForAction(2);
    }

    /**
     * {@inheritDoc}
     *
     * This method blocks all put/update/delete operations until it finishes (via global ReadWrite Commit Lock).
     *
     */
    @Override
    public void compact() {
        waitForAction(3);
    }


    /**
     * {@inheritDoc}
     *
     *  This method blocks until Write Queue is empty (written into disk).
     *  It also blocks any put/update/delete operations until it finishes (via global ReadWrite Commit Lock).
     */
    @Override
    public void clearCache() {
        //$DELAY$
        commitLock.writeLock().lock();
        try{
            checkState();
            //wait for response from writer thread
            while(!writeCache.isEmpty()){
                checkState();
                Thread.sleep(100);
            }
            //$DELAY$
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            commitLock.writeLock().unlock();
        }
        //$DELAY$
        super.clearCache();
    }

}
