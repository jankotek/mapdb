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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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

    protected static final AtomicLong threadCounter = new AtomicLong();
    protected final long threadNum = threadCounter.incrementAndGet();

    protected static final int QUEUE_SIZE = 1024*10;

    protected final BlockingQueue<Long> newRecids = new ArrayBlockingQueue<Long>(128);
    //Long or CountDownLatch
    protected final BlockingQueue itemsQueue = new ArrayBlockingQueue(QUEUE_SIZE);

    protected final LongConcurrentHashMap<Fun.Tuple2<Object, Serializer>> items
            = new LongConcurrentHashMap<Fun.Tuple2<Object, Serializer>>();

    protected final ReentrantReadWriteLock commitLock = new ReentrantReadWriteLock();

    protected final CountDownLatch shutdownCondition = new CountDownLatch(2);



    protected volatile Throwable writerFailedException = null;
    protected volatile boolean closeInProgress = false;

    protected static final Object TOMBSTONE = new Object();

    protected final int asyncFlushDelay;


    //TODO use thread factory here
    protected final Thread newRecidsThread = new Thread("MapDB prealloc #"+threadNum){
        @Override public void run() {
            try{
                for(;;){
                    if(closeInProgress || writerFailedException!=null) return;
                    Long newRecid = getWrappedEngine().put(Utils.EMPTY_STRING, Serializer.EMPTY_SERIALIZER);
                    newRecids.put(newRecid);
                }
            } catch (Throwable e) {
                writerFailedException = e;
            }finally {
                shutdownCondition.countDown();
            }
        }
    };

    //TODO use thread factory here
    protected final Thread writerThread = new Thread("MapDB writer #"+threadNum){
        @Override public void run() {
            try{
                for(int i=0;;i++){
                    if(writerFailedException!=null) return;
                    if(i%(QUEUE_SIZE/10)==0 && asyncFlushDelay!=0 && itemsQueue.size()<QUEUE_SIZE/4){
                        LockSupport.parkNanos(1000L*1000L*asyncFlushDelay);
                    }
                    Object taken = itemsQueue.take();
                    if(taken instanceof CountDownLatch){
                        CountDownLatch latch = (CountDownLatch) taken;
                        long count = latch.getCount();
                        if(count == 0){ //close operation
                            return;
                        }else if(count == 1){ //commit operation
                            AsyncWriteEngine.super.commit();
                            latch.countDown();
                        }else if(count==2){ //rollback operation
                            AsyncWriteEngine.super.rollback();
                            newRecids.clear();
                            latch.countDown();
                            latch.countDown();
                        }else if(count==3){ //compact operation
                            AsyncWriteEngine.super.compact();
                            latch.countDown();
                            latch.countDown();
                            latch.countDown();
                        }else{throw new InternalError();}
                    }else{
                        long recid = (Long) taken;
                        Fun.Tuple2<Object, Serializer> item = items.get(recid);
                        if(item == null) continue;
                        if(item.a==TOMBSTONE){
                            AsyncWriteEngine.super.delete(recid, item.b);
                        }else{
                            AsyncWriteEngine.super.update(recid, item.a, item.b);
                        }
                        items.remove(recid, item);
                    }
                }
            } catch (Throwable e) {
                writerFailedException = e;
            }finally {
                shutdownCondition.countDown();
            }
        }
    };



    public AsyncWriteEngine(Engine engine, int _asyncFlushDelay) {
        super(engine);

        newRecidsThread.setDaemon(true);
        writerThread.setDaemon(true);

        newRecidsThread.start();
        writerThread.start();
        asyncFlushDelay = _asyncFlushDelay;

    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        commitLock.readLock().lock();
        try{
            try {
                Long recid = newRecids.take(); //TODO possible deadlock while closing
                update(recid, value, serializer);
                return recid;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }finally{
            commitLock.readLock().unlock();
        }

    }

    protected void checkState() {
        if(closeInProgress) throw new IllegalAccessError("db has been closed");
        if(writerFailedException!=null) throw new RuntimeException("Writer thread failed", writerFailedException);
    }

    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        commitLock.readLock().lock();
        try{
            checkState();
            Fun.Tuple2<Object,Serializer> item = items.get(recid);
            if(item!=null){
                if(item.a == TOMBSTONE) return null;
                return (A) item.a;
            }

            return super.get(recid, serializer);
        }finally{
            commitLock.readLock().unlock();
        }
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        if(serializer!=SerializerPojo.serializer) commitLock.readLock().lock();
        try{
            checkState();
            items.put(recid, new Fun.Tuple2(value,serializer));
            itemsQueue.put(recid);
        }catch(InterruptedException e){
            throw new RuntimeException(e);
        }finally{
            if(serializer!=SerializerPojo.serializer) commitLock.readLock().unlock();
        }
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        commitLock.writeLock().lock();
        try{
            checkState();
            Fun.Tuple2<Object, Serializer> existing = items.get(recid);
            A oldValue = existing!=null? (A) existing.a : super.get(recid, serializer);
            if(oldValue == expectedOldValue || (oldValue!=null && oldValue.equals(expectedOldValue))){
                items.put(recid, new Fun.Tuple2(newValue,serializer));
                itemsQueue.put(recid);
                return true;
            }else{
                return false;
            }
        }catch(InterruptedException e){
            throw new RuntimeException(e);
        }finally{
            commitLock.writeLock().unlock();
        }
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        update(recid, (A) TOMBSTONE, serializer);
    }

    @Override
    public void close() {
        commitLock.writeLock().lock();
        try {
            if(closeInProgress) return;
            checkState();
            closeInProgress = true;
            //notify background threads
            itemsQueue.put(new CountDownLatch(0));
            super.delete(newRecids.take(), Serializer.EMPTY_SERIALIZER);

            //wait for background threads to shutdown
            shutdownCondition.await();

            //put preallocated recids back to store
            for(Long recid = newRecids.poll(); recid!=null; recid = newRecids.poll()){
                super.delete(recid, Serializer.EMPTY_SERIALIZER);
            }

            AsyncWriteEngine.super.close();

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            commitLock.writeLock().unlock();
        }
    }




    @Override
    public void commit() {
        commitLock.writeLock().lock();
        try{
            checkState();
            //notify background threads
            CountDownLatch msg = new CountDownLatch(1);
            itemsQueue.put(msg);

            //wait for response from writer thread
            while(!msg.await(1,TimeUnit.SECONDS)){
                checkState();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            commitLock.writeLock().unlock();
        }
    }

    @Override
    public void rollback() {
        commitLock.writeLock().lock();
        try{
            checkState();
            //notify background threads
            CountDownLatch msg = new CountDownLatch(2);
            itemsQueue.put(msg);

            //wait for response from writer thread
            while(!msg.await(1,TimeUnit.SECONDS)){
                checkState();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            commitLock.writeLock().unlock();
        }
    }

    @Override
    public void compact() {
        commitLock.writeLock().lock();
        try{
            checkState();
            //notify background threads
            CountDownLatch msg = new CountDownLatch(3);
            itemsQueue.put(msg);

            //wait for response from writer thread
            while(!msg.await(1,TimeUnit.SECONDS)){
                checkState();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            commitLock.writeLock().unlock();
        }
    }

    @Override
    public void clearCache() {
        commitLock.writeLock().lock();
        try{
            checkState();
            //wait for response from writer thread
            while(!items.isEmpty()){
                checkState();
                Thread.sleep(250);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            commitLock.writeLock().unlock();
        }
        super.clearCache();
    }


}
