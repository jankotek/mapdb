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

import java.io.IOError;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * StorageDirect which stores all modifications in memory.
 * All changes are written into store asynchronously in background thread.
 * <p/>
 *  This store does not provide cache. Changes are stored in memory only for
 *  queue and are written to store ASAP.
 *
 *
 * @author Jan Kotek
 */
public class AsyncWriteEngine extends EngineWrapper implements Engine {


    protected final ReentrantReadWriteLock grandLock = new ReentrantReadWriteLock();

    //private long allocatedIndexFileSize;

    protected final boolean asyncSerialization;

    protected final int flushDelay;

    /** indicates deleted record */
    protected static final Object DELETED = new Object();

    /** stores writes */
    final protected LongConcurrentHashMap<Object> writes = new LongConcurrentHashMap<Object>();

    private boolean shutdownSignal = false;
    private CountDownLatch shutdownResponse = new CountDownLatch(1);

    protected final ReentrantReadWriteLock commitLock = new ReentrantReadWriteLock();

    final protected Object writerNotify = new Object();

    /** Reference to an parent Engine.
     * If this object is Garbage Collected it means no more
     * updates and Writer thread can exit
     * TODO I ran some tests, and parent Engine is not GCed even through there are no refs pointing to it. Investigate!
     */
    protected WeakReference<Object> parentEngineWeakRef;

    protected final Thread writerThread = new Thread("JDBM writer"){
        @Override
		public void run(){
            writerThreadRun();
        }
    };
    private Throwable rethrow;

    @SuppressWarnings("unchecked")
    private void writerThreadRun() {
        long nextFlush = 0;
        while(true)try{
            while(( writes.isEmpty() || (flushDelay !=0 && nextFlush>System.currentTimeMillis()))
                    && newRecids.remainingCapacity()==0){
                if(writes.isEmpty() && (shutdownSignal||
                            (parentEngineWeakRef!=null&& parentEngineWeakRef.get()==null))){
                    //store closed, shutdown this thread
                    shutdownResponse.countDown();
                    return;
                }

                synchronized (writerNotify){
                    writerNotify.wait(1000); //check write conditions every N seconds to prevent possible deadlock
                }
            }


            try{
                grandLock.writeLock().lock();

            if(flushDelay ==0 || System.currentTimeMillis()>nextFlush){
                LongMap.LongMapIterator<Object> iter = writes.longMapIterator();
                while(iter.moveToNext()){
                    final long recid = iter.key();
                    final Object value = iter.value();
                    if(value==DELETED){
                        engine.recordDelete(recid);
                    }else{
                        byte[] data = asyncSerialization ? ((SerRec)value).serialize() : (byte[]) value;
                        engine.recordUpdate(recid, data, Serializer.BYTE_ARRAY_SERIALIZER);
                    }
                    //Record will be only removed if value was not updated.
                    //If value was updated during write, equality check will fail, and it will stay there
                    //We just collect it at next round
                    writes.remove(recid, value);
                }
            }
            if(flushDelay !=0){
                nextFlush = System.currentTimeMillis()+ flushDelay;
            }

            int toFetch = newRecids.remainingCapacity();
            for(int i=0;i<toFetch;i++){
                newRecids.put(engine.recordPut(null, Serializer.NULL_SERIALIZER));
            }

            }finally {
                grandLock.writeLock().unlock();
            }


        }catch(Throwable e){
           AsyncWriteEngine.this.rethrow = e;
           return;
        }
    }

    protected final ArrayBlockingQueue<Long> newRecids = new ArrayBlockingQueue<Long>(128);


    public AsyncWriteEngine(Engine engine, boolean asyncSerialization, int flushDelay,
                            boolean asyncThreadDaemon) {
        super(engine);
        this.asyncSerialization = asyncSerialization;
        this.flushDelay = flushDelay;
        //TODO cache index file size
        //allocatedIndexFileSize = indexValGet(RECID_CURRENT_INDEX_FILE_SIZE);

        writerThread.setDaemon(asyncThreadDaemon);
        writerThread.start();
    }

    public void setParentEngineReference(Engine e){
        this.parentEngineWeakRef = new WeakReference<Object>(e);
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        checkRethrow();
        Object v;
        if(asyncSerialization){
            v = new SerRec<A>(value, serializer);
        }else{
            DataOutput2 out = new DataOutput2();
            try {
                serializer.serialize(out, value);
            } catch (IOException e) {
                throw new IOError(e);
            }
            v = out.copyBytes();
        }
        try{
            commitLock.readLock().lock();

            Object previous = writes.put(recid,v);
            synchronized (writerNotify){
                writerNotify.notify();
            }
            if(previous== DELETED){
                throw new IllegalArgumentException("Recid was deleted: "+recid);
            }
        }finally{
            commitLock.readLock().unlock();
        }

    }

    protected void checkRethrow() {
        if(rethrow!=null) throw new RuntimeException("an error in MapDB writer thread", rethrow);
    }

    @Override
    public void recordDelete(long recid) {
        checkRethrow();
        if(CC.ASSERT&& recid == 0) throw new InternalError();
        try{
            commitLock.readLock().lock();
            writes.put(recid, DELETED);
            synchronized (writerNotify){
                writerNotify.notify();
            }
        }finally{
            commitLock.readLock().unlock();
        }
    }


    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        checkRethrow();
        try{
            Object v;
            if(asyncSerialization){
                v = new SerRec<A>(value,serializer);
            }else{
                DataOutput2 out = new DataOutput2();
                serializer.serialize(out, value);
                v= out.copyBytes();
            }

            try{
                commitLock.readLock().lock();
                Long newRecid = null;
                while(newRecid==null){
                    checkRethrow();
                    newRecid = newRecids.poll(1, TimeUnit.SECONDS);

                }
                writes.put(newRecid, v);
                synchronized (writerNotify){
                    writerNotify.notify();
                }
                return newRecid;
            }finally{
                commitLock.readLock().unlock();
            }

        } catch (IOException e) {
            throw new IOError(e);
        }catch(InterruptedException e){
            throw new RuntimeException(e);
        }


    }

    @Override
    @SuppressWarnings("unchecked")
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        checkRethrow();

        Object d = writes.get(recid);
        if(d == DELETED){
            return null;
        }else if(d!=null){
            if(asyncSerialization)
                return (A) ((SerRec)d).value;
            try {
                byte[] b = (byte[]) d;
                return serializer.deserialize(new DataInput2(ByteBuffer.wrap(b),0),b.length);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        try{
            grandLock.readLock().lock();
            return engine.recordGet(recid, serializer);
        }finally {
            grandLock.readLock().unlock();
        }

    }

    @Override
    public void close() {
        shutdownSignal = true;
        synchronized (writerNotify){
            writerNotify.notify();
        }

        //wait until writer thread finishes and exits
        try {
            shutdownResponse.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        //deallocate all unused recids
        for(long recid:newRecids){
            engine.recordDelete(recid);
        }

        //TODO commit here?
        engine.close();
        engine = null;
    }

    @Override
    public void commit() {
        try{
            commitLock.writeLock().lock();

            try{
                //wait until queue is empty
                while(!writes.isEmpty()){
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                grandLock.writeLock().lock();
                engine.commit();
            }finally{
                grandLock.writeLock().unlock();
            }

        }finally {
            commitLock.writeLock().unlock();


        }
    }

    @Override
    public void rollback() {
        //TODO drop cache here?
        try{
            grandLock.writeLock().lock();
            try{
                commitLock.writeLock().lock();
                engine.rollback();
            }finally{
                commitLock.writeLock().unlock();
            }
        }finally {
            grandLock.writeLock().unlock();
        }

    }


    protected static class SerRec<E> {

        final E value;
        final Serializer<E> serializer;

        private SerRec(E value, Serializer<E> serializer) {
            this.value = value;
            this.serializer = serializer;
        }

        byte[] serialize(){
            DataOutput2 out = new DataOutput2();
            try {
                serializer.serialize(out, value);
            } catch (IOException e) {
                throw new IOError(e);
            }
            return out.copyBytes();
        }

    }



}
