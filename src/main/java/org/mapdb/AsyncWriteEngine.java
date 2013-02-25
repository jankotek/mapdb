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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
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

    protected final BlockingQueue<Long> newRecids = new ArrayBlockingQueue<Long>(128);

    protected volatile boolean closeInProgress = false;
    protected final CountDownLatch shutdownCondition = new CountDownLatch(1);

    protected final Thread newRecidsThread = new Thread("MapDB prealloc #"+threadNum){
        @Override public void run() {
            try{
                while(!closeInProgress || parentEngineWeakRef.get()==null){
                    Long newRecid = getWrappedEngine().put(Utils.EMPTY_STRING, Serializer.NULL_SERIALIZER);
                    newRecids.put(newRecid);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }finally {
                shutdownCondition.countDown();
            }

        }
    };

    protected final AtomicReference<LongConcurrentHashMap> m1 =
            new AtomicReference<LongConcurrentHashMap>(new LongConcurrentHashMap());

    protected final AtomicReference<LongConcurrentHashMap> m2 =
            new AtomicReference<LongConcurrentHashMap>(new LongConcurrentHashMap());



    protected AsyncWriteEngine(Engine engine, boolean _asyncThreadDaemon, boolean _powerSavingMode, int _asyncFlushDelay) {
        super(engine);
        if(_asyncThreadDaemon){
            newRecidsThread.setDaemon(true);
        }
        newRecidsThread.start();
    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        try {
            Long recid = newRecids.poll(100, TimeUnit.MILLISECONDS);
            if(recid==null)
                return super.put(value,serializer);
            update(recid, value, serializer);
            return recid;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        return super.get(recid, serializer);
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        super.update(recid, value, serializer);
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        return super.compareAndSwap(recid, expectedOldValue, newValue, serializer);
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        super.delete(recid, serializer);
    }

    @Override
    public void close() {
        try {
            closeInProgress = true;
            //put preallocated recids back to store
            for(Long recid = newRecids.poll(); recid!=null; recid = newRecids.poll()){
                super.delete(recid, Serializer.NULL_SERIALIZER);
            }
            //TODO commit after returning recids?

            //wait for worker threads to shutdown
            shutdownCondition.await();


            super.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
