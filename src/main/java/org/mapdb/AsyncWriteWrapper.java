package org.mapdb;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * StorageDirect which stores all modifications in memory.
 * All changes are written into store asynchronously in background thread.
 * This store is nearly lock free and provides high concurrent scalability.
 * <p/>
 *  This store does not provide cache. Changes are stored in memory only for
 *  queue and are written to store ASAP.
 *
 *
 * @author Jan Kotek
 */
public class AsyncWriteWrapper implements RecordManager{


    protected final ReentrantReadWriteLock grandLock = new ReentrantReadWriteLock();

    //private long allocatedIndexFileSize;

    protected final boolean asyncSerialization;

    /** indicates deleted record */
    protected static final Object DELETED = new Object();

    /** stores writes */
    final protected LongConcurrentHashMap<Object> writes = new LongConcurrentHashMap<Object>();

    private boolean shutdownSignal = false;
    private CountDownLatch shutdownResponse = new CountDownLatch(1);

    final protected Object writerNotify = new Object();

    protected RecordManager recman;

    protected final Thread writerThread = new Thread("JDBM writer"){
        public void run(){
            writerThreadRun();
        }
    };
    private Exception rethrow;

    @SuppressWarnings("unchecked")
    private void writerThreadRun() {
        while(true)try{
            while(writes.isEmpty() && newRecids.remainingCapacity()==0){
                if(writes.isEmpty() && shutdownSignal){
                    //store closed, shutdown this thread
                    shutdownResponse.countDown();
                    return;
                }

                synchronized (writerNotify){
                    writerNotify.wait(1000); //TODO deadlock was reported here (remove 1000)
                }
            }


            try{
                grandLock.writeLock().lock();

            LongMap.LongMapIterator<Object> iter = writes.longMapIterator();
            while(iter.moveToNext()){
                final long recid = iter.key();
                final Object value = iter.value();
                if(value==DELETED){
                    recman.recordDelete(recid);
                }else{
                    byte[] data = asyncSerialization ? ((SerRec)value).serialize() : (byte[]) value;
                    recman.recordUpdate(recid, data, Serializer.BYTE_ARRAY_SERIALIZER);
                }
                //Record will be only removed if value was not updated.
                //If value was updated during write, equality check will fail, and it will stay there
                //We just collect it at next round
                writes.remove(recid, value);
            }

            int toFetch = newRecids.remainingCapacity();
            for(int i=0;i<toFetch;i++){
                newRecids.put(recman.recordPut(null, Serializer.NULL_SERIALIZER));
            }
            }finally {
                grandLock.writeLock().unlock();
            }


        }catch(Exception e){
           AsyncWriteWrapper.this.rethrow = new RuntimeException("an error in writter thread",e);
        }
    }

    private ArrayBlockingQueue<Long> newRecids = new ArrayBlockingQueue<Long>(128);


    public AsyncWriteWrapper(RecordManager recman, boolean asyncSerialization) {
        this.recman = recman;
        this.asyncSerialization = asyncSerialization;
        //TODO cache index file size
        //allocatedIndexFileSize = indexValGet(RECID_CURRENT_INDEX_FILE_SIZE);

        writerThread.setDaemon(true);
        writerThread.start();
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        if(rethrow!=null) throw new RuntimeException(rethrow);
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
        Object previous = writes.put(recid,v);
        synchronized (writerNotify){
            writerNotify.notify();
        }

        if(previous== DELETED){
            throw new IllegalArgumentException("Recid was deleted: "+recid);
        }
    }

    @Override
    public void recordDelete(long recid) {
        if(rethrow!=null) throw new RuntimeException(rethrow);
        if(CC.ASSERT&& recid == 0) throw new InternalError();
        writes.put(recid, DELETED);
        synchronized (writerNotify){
            writerNotify.notify();
        }
    }

    @Override
    public Long getNamedRecid(String name) {
    try{
        grandLock.writeLock().lock();
        return recman.getNamedRecid(name);
    }finally {
        grandLock.writeLock().unlock();
    }

}

    @Override
    public void setNamedRecid(String name, Long recid) {
        try{
            grandLock.writeLock().lock();
            recman.setNamedRecid(name, recid);
        }finally {
            grandLock.writeLock().unlock();
        }
    }

    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        if(rethrow!=null) throw new RuntimeException(rethrow);
        try{
            Object v;
            if(asyncSerialization){
                v = new SerRec<A>(value,serializer);
            }else{
                DataOutput2 out = new DataOutput2();
                serializer.serialize(out, value);
                v= out.copyBytes();
            }

            final long newRecid = newRecids.take();
            writes.put(newRecid, v);
            synchronized (writerNotify){
                writerNotify.notify();
            }
        return newRecid;
        } catch (IOException e) {
            throw new IOError(e);
        }catch(InterruptedException e){
            throw new RuntimeException(e);
        }


    }

    @Override
    @SuppressWarnings("unchecked")
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        if(rethrow!=null) throw new RuntimeException(rethrow);

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
            return recman.recordGet(recid, serializer);
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
            recman.recordDelete(recid);
        }

        //TODO commit here?
        recman.close();
        recman = null;
    }

    @Override
    public void commit() {
        try{
            grandLock.writeLock().lock();
            recman.commit();
        }finally {
            grandLock.writeLock().unlock();
        }
    }

    @Override
    public void rollback() {
        //TODO drop cache here?
        try{
            grandLock.writeLock().lock();
            recman.rollback();
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

    @Override
    public long serializerRecid() {
        return recman.serializerRecid();
    }

}
