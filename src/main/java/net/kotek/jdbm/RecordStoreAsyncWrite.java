package net.kotek.jdbm;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * RecordStore which stores all modifications in memory.
 * All changes are written into store asynchronously in background thread.
 * This store is nearly lock free and provides high concurrent scalability.
 * <p/>
 *  This store does not provide cache. Changes are stored in memory only for
 *  queue and are written to store ASAP.
 *
 *
 * @author Jan Kotek
 */
public class RecordStoreAsyncWrite extends RecordStore{


    //private long allocatedIndexFileSize;

    protected final boolean asyncSerialization;

    /** indicates deleted record */
    protected static final Object DELETED = new Object();

    /** stores writes */
    final protected LongConcurrentHashMap<Object> writes = new LongConcurrentHashMap<Object>();

    private boolean shutdownSignal = false;
    private CountDownLatch shutdownResponse = new CountDownLatch(1);

    final protected Object writerNotify = new Object();

    protected final Thread writerThread = new Thread("JDBM writer"){
        public void run(){
            writerThreadRun();
        }
    };

    @SuppressWarnings("unchecked")
    private void writerThreadRun() {
        while(true)try{
            while(writes.isEmpty() && newRecids.remainingCapacity()==0){
                if(writes.isEmpty() && shutdownSignal){
                    //store closed, shutdown this thread
                    shutdownResponse.countDown();
                    return;
                }

                //TODO this just sucks, proper notify()
                Thread.yield();
            }


            LongMap.LongMapIterator<Object> iter = writes.longMapIterator();
            while(iter.moveToNext()){
                final long recid = iter.key();
                final Object value = iter.value();
                if(value==DELETED){
                    RecordStoreAsyncWrite.super.recordDelete(recid);
                }else{
                    byte[] data = asyncSerialization ? ((SerRec)value).serialize() : (byte[]) value;
                    RecordStoreAsyncWrite.super.forceRecordUpdateOnGivenRecid(recid, data);
                }
                //Record will be only removed if value was not updated.
                //If value was updated during write, equality check will fail, and it will stay there
                //We just collect it at next round
                writes.remove(recid, value);
            }

            int toFetch = newRecids.remainingCapacity();
            try{
                writeLock_lock();
                for(int i=0;i<toFetch;i++){
                    newRecids.put(freeRecidTake());
                }
            }finally {
                writeLock_unlock();
            }


        }catch(Exception e){
            JdbmUtil.LOG.log(Level.SEVERE, "An exception in JDBM Writer thread",e);
        }
    }

    private ArrayBlockingQueue<Long> newRecids = new ArrayBlockingQueue<Long>(128);


    public RecordStoreAsyncWrite(String fileName, boolean asyncSerialization) {
        super(fileName);
        this.asyncSerialization = asyncSerialization;
        //TODO cache index file size
        //allocatedIndexFileSize = indexValGet(RECID_CURRENT_INDEX_FILE_SIZE);

        writerThread.setDaemon(true);
        writerThread.start();

    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
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

        if(previous== DELETED){
            throw new IllegalArgumentException("Recid was deleted: "+recid);
        }
    }

    @Override
    public void recordDelete(long recid) {
        if(CC.ASSERT&& recid == 0) throw new InternalError();
        writes.put(recid, DELETED);
    }

    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
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

        return super.recordGet(recid, serializer);
    }

    @Override
    public void close() {
        shutdownSignal = true;
        //wait until writer thread finishes and exits
        try {
            shutdownResponse.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        //put all remaining unused recids into free list
        try{
            writeLock_lock();
            for(long recid:newRecids){
                freeRecidPut(recid);
            }
        }finally {
            writeLock_unlock();
        }


        super.close();
    }


    @SuppressWarnings("all")
    private final AtomicInteger writeLocksCounter = CC.ASSERT? new AtomicInteger(0) : null;

    @Override
    protected void writeLock_lock() {
        if(CC.ASSERT &&writeLocksCounter!=null){
            int c = writeLocksCounter.incrementAndGet();
            if(c!=1) throw new InternalError("more then one writer");
        }
    }

    @Override
    protected void writeLock_unlock() {
        if(CC.ASSERT &&writeLocksCounter!=null){
            int c = writeLocksCounter.decrementAndGet();
            if(c!=0) throw new InternalError("more then one writer");
        }

    }

    @Override
    protected void writeLock_checkLocked() {
        if(CC.ASSERT &&writeLocksCounter!=null){
            if(writeLocksCounter.get()!=1)
                throw new InternalError("more then one writer");
        }
    }

    @Override
    protected void readLock_unlock() {
        //do nothing, background thread and cache takes care of write synchronization
    }

    @Override
    protected void readLock_lock() {
        //do nothing, background thread and cache takes care of write synchronization
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
