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
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * {@link Engine} wrapper which provides asynchronous serialization and asynchronous write.
 * This class takes an object instance, passes it to background writer thread (using Write Cache)
 * where it is serialized and written to disk. Async write does not affect commit durability,
 * write cache is flushed into disk on each commit. Modified records are held in small instance cache,
 * until they are written into disk.
 *
 * This feature is disabled by default and can be enabled by calling {@link DBMaker#asyncWriteEnable()}.
 * Write Cache is flushed in regular intervals or when it becomes full. Flush interval is 100 ms by default and
 * can be controlled by {@link DBMaker#asyncWriteFlushDelay(int)}. Increasing this interval may improve performance
 * in scenarios where frequently modified items should be cached, typically {@link BTreeMap} import where keys
 * are presorted.
 *
 * Asynchronous write does not affect commit durability. Write Cache is flushed during each commit, rollback and close call.
 * You may also flush Write Cache manually by using {@link org.mapdb.AsyncWriteEngine#clearCache()}  method.
 * There is global lock which prevents record being updated while commit is in progress.
 *
 * This wrapper starts one threads named `MapDB writer #N` (where N is static counter).
 * Async Writer takes modified records from Write Cache and writes them into store.
 * It also preallocates new recids, as finding empty `recids` takes time so small stash is pre-allocated.
 * It runs as `daemon`, so it does not prevent JVM to exit.
 *
 * Asynchronous Writes have several advantages (especially for single threaded user). But there are two things
 * user should be aware of:
 *
 *  * Because data are serialized on back-ground thread, they need to be thread safe or better immutable.
 *    When you insert record into MapDB and modify it latter, this modification may happen before item
 *    was serialized and you may not be sure what version was persisted
 *
 *  * Asynchronous writes have some overhead and introduce single bottle-neck. This usually not issue for
 *    single or two threads, but in multi-threaded environment it may decrease performance.
 *    So in truly concurrent environments with many updates (network servers, parallel computing )
 *    you should disable Asynchronous Writes.
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

    protected final long[] newRecids = new long[CC.ASYNC_RECID_PREALLOC_QUEUE_SIZE];
    protected int newRecidsPos = 0;
    protected final ReentrantLock newRecidsLock = new ReentrantLock(CC.FAIR_LOCKS);


    /** Associates `recid` from Write Queue with record data and serializer. */
    protected final LongConcurrentHashMap<Fun.Tuple2<Object, Serializer>> writeCache
            = new LongConcurrentHashMap<Fun.Tuple2<Object, Serializer>>();

    /** Each insert to Write Queue must hold read lock.
     *  Commit, rollback and close operations must hold write lock
     */
    protected final ReentrantReadWriteLock commitLock = new ReentrantReadWriteLock(CC.FAIR_LOCKS);

    /** number of active threads running, used to await thread termination on close */
    protected final CountDownLatch activeThreadsCount = new CountDownLatch(1);

    /** If background thread fails with exception, it is stored here, and rethrown to all callers.*/
    protected volatile Throwable threadFailedException = null;

    /** indicates that `close()` was called and background threads are being terminated*/
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

                    //if conditions are right, slow down writes a bit
                    if(asyncFlushDelay!=0 && !commitLock.isWriteLocked() && size.get()<maxParkSize){
                        LockSupport.parkNanos(1000L * 1000L * asyncFlushDelay);
                    }

                    AsyncWriteEngine engine = engineRef.get();
                    if(engine==null) return; //stop thread if this engine has been GCed
                    if(engine.threadFailedException !=null) return; //other thread has failed, no reason to continue

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

        if(executor!=null){
            executor.execute(writerRun);
            return;
        }
        final long threadNum = threadCounter.incrementAndGet();
        Thread writerThread = new Thread(writerRun,"MapDB writer #"+threadNum);
        writerThread.setDaemon(true);
        writerThread.start();
    }


    /** runs on background thread. Takes records from Write Queue, serializes and writes them.*/
    protected boolean runWriter() throws InterruptedException {
        final CountDownLatch latch = action.getAndSet(null);

        int counter=0;

        do{
            LongMap.LongMapIterator<Fun.Tuple2<Object, Serializer>> iter = writeCache.longMapIterator();
            while(iter.moveToNext()){
                //usual write
                final long recid = iter.key();
                Fun.Tuple2<Object, Serializer> item = iter.value();
                if(item == null) continue; //item was already written
                if(item.a==TOMBSTONE){
                    //item was not updated, but deleted
                    AsyncWriteEngine.super.delete(recid, item.b);
                }else{
                    //call update as usual
                    AsyncWriteEngine.super.update(recid, item.a, item.b);
                }
                //record has been written to underlying Engine, so remove it from cache with CAS
                if(writeCache.remove(recid, item))
                    size.decrementAndGet();

                if(((++counter)&(63))==0){   //it is like modulo 64, but faster
                    preallocateRefill();
                }
            }
        }while(latch!=null && !writeCache.isEmpty());

        preallocateRefill();


        //operations such as commit,close, compact or close needs to be executed in Writer Thread
        //for this case CountDownLatch is used, it also signals when operations has been completed
        //CountDownLatch is used as special case to signalise special operation
        if(latch!=null){
            assert(writeCache.isEmpty());

            final long count = latch.getCount();
            if(count == 0){ //close operation
                return false;
            }else if(count == 1){ //commit operation
                AsyncWriteEngine.super.commit();
                latch.countDown();
            }else if(count==2){ //rollback operation
                AsyncWriteEngine.super.rollback();
                preallocateRollback();
                latch.countDown();
                latch.countDown();
            }else if(count==3){ //compact operation
                AsyncWriteEngine.super.compact();
                latch.countDown();
                latch.countDown();
                latch.countDown();
            }else{throw new AssertionError();}
        }
        return true;


    }

    protected void preallocateRollback(){
        newRecidsLock.lock();
        try{
            getWrappedEngine().preallocate(newRecids);
            newRecidsPos = newRecids.length;
        }finally {
            newRecidsLock.unlock();
        }
    }

    @Override
    public long preallocate() {
        commitLock.readLock().lock();
        try{
            return preallocateNoCommitLock();
        }finally{
            commitLock.readLock().unlock();
        }
    }

    @Override
    public void preallocate(long[] recids) {
        commitLock.readLock().lock();
        try{
            for(int i=0;i<recids.length;i++){
                recids[i] = preallocateNoCommitLock();
            }
        }finally{
            commitLock.readLock().unlock();
        }
    }


    protected long preallocateNoCommitLock() {
        newRecidsLock.lock();
        try{
            if(newRecidsPos==0){
                getWrappedEngine().preallocate(newRecids);
                newRecidsPos = newRecids.length;
            }

            return newRecids[--newRecidsPos];
        }finally {
            newRecidsLock.unlock();
        }
    }

    protected void preallocateRefill(){
        newRecidsLock.lock();
        try{
            if(newRecidsPos==0){
                getWrappedEngine().preallocate(newRecids);
                newRecidsPos = newRecids.length;
            }
        }finally {
            newRecidsLock.unlock();
        }
    }



    /** checks that background threads are ready and throws exception if not */
    protected void checkState() {
        if(closeInProgress) throw new IllegalAccessError("db has been closed");
        if(threadFailedException !=null) throw new RuntimeException("Writer thread failed", threadFailedException);
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
     * ![async-put](async-put.png)
     *
     @uml async-put.png
     actor user
     participant "put method" as put
     participant "Writer Thread" as wri
     note over wri: has preallocated \n recids in queue
     activate put
     user -> put: User calls put method
     wri-> put: takes preallocated recid
     put -> wri: forward record into Write Queue
     put -> user: return recid to user
     deactivate put
     note over wri: eventually\n writes record\n before commit
     */
    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        int size2 = 0;
        long recid =0;
        commitLock.readLock().lock();
        try{
            checkState();
            recid = preallocateNoCommitLock();
            if(writeCache.put(recid, new Fun.Tuple2(value, serializer))==null)
                size2 = size.incrementAndGet();
        }finally{
            commitLock.readLock().unlock();
        }
        if(size2>maxSize)
            clearCache();
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
        commitLock.readLock().lock();
        try{
            checkState();
            Fun.Tuple2<Object,Serializer> item = writeCache.get(recid);
            if(item!=null){
                if(item.a == TOMBSTONE) return null;
                return (A) item.a;
            }

            return super.get(recid, serializer);
        }finally{
            commitLock.readLock().unlock();
        }
    }


    /**
     * {@inheritDoc}
     *
     * This methods forwards record into Writer Thread and returns asynchronously.
     *
     * ![async-update](async-update.png)
     * @uml async-update.png
     * actor user
     * participant "update method" as upd
     * participant "Writer Thread" as wri
     * activate upd
     * user -> upd: User calls update method
     * upd -> wri: forward record into Write Queue
     * upd -> user: returns
     * deactivate upd
     * note over wri: eventually\n writes record\n before commit
     */
    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        int size2 = 0;
        commitLock.readLock().lock();
        try{
            checkState();
            if(writeCache.put(recid, new Fun.Tuple2(value, serializer))==null)
                size2 = size.incrementAndGet();
        }finally{
            commitLock.readLock().unlock();
        }
        if(size2>maxSize)
            clearCache();
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
        commitLock.writeLock().lock();
        try{
            checkState();
            Fun.Tuple2<Object, Serializer> existing = writeCache.get(recid);
            A oldValue = existing!=null? (A) existing.a : super.get(recid, serializer);
            if(oldValue == expectedOldValue || (oldValue!=null && oldValue.equals(expectedOldValue))){
                if(writeCache.put(recid, new Fun.Tuple2(newValue, serializer))==null)
                    size2 = size.incrementAndGet();
                ret = true;
            }else{
                ret = false;
            }
        }finally{
            commitLock.writeLock().unlock();
        }
        if(size2>maxSize)
            clearCache();
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
     *  When this method was called `closeInProgress` is set and no record can be modified.
     */
    @Override
    public void close() {
        commitLock.writeLock().lock();
        try {
            if(closeInProgress) return;
            checkState();
            closeInProgress = true;
            //notify background threads
            if(!action.compareAndSet(null,new CountDownLatch(0)))
                throw new AssertionError();

            //wait for background threads to shutdown

            activeThreadsCount.await(1000,TimeUnit.MILLISECONDS);

            //put preallocated recids back to store
            newRecidsLock.lock();
            try{
                while(newRecidsPos>0){
                    super.delete(newRecids[--newRecidsPos],Serializer.ILLEGAL_ACCESS);
                }
            }finally{
                newRecidsLock.unlock();
            }

            AsyncWriteEngine.super.close();

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            commitLock.writeLock().unlock();
        }
    }



    protected void waitForAction(int actionNumber) {
        commitLock.writeLock().lock();
        try{
            checkState();
            //notify background threads
            CountDownLatch msg = new CountDownLatch(actionNumber);
            if(!action.compareAndSet(null,msg))
                throw new AssertionError();

            //wait for response from writer thread
            while(!msg.await(100, TimeUnit.MILLISECONDS)){
                checkState();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            commitLock.writeLock().unlock();
        }
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
        commitLock.writeLock().lock();
        try{

            checkState();
            //wait for response from writer thread
            while(!writeCache.isEmpty()){
                checkState();
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            commitLock.writeLock().unlock();
        }
        super.clearCache();
    }

}
