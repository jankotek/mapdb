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
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;


/**
 * EngineWrapper adapter. It implements all methods on Engine interface.
 *
 * @author Jan Kotek
 */
public class EngineWrapper implements Engine{

    protected static final Logger LOG = !CC.LOG_EWRAP?null :
            Logger.getLogger(EngineWrapper.class.getName());



    private Engine engine;

    protected EngineWrapper(Engine engine){
        if(engine == null) throw new IllegalArgumentException();
        this.engine = engine;
    }

    @Override
    public long preallocate(){
        return getWrappedEngine().preallocate();
    }

    @Override
    public void preallocate(long[] recids){
        getWrappedEngine().preallocate(recids);
    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        return getWrappedEngine().put(value, serializer);
    }

    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        return getWrappedEngine().get(recid, serializer);
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        getWrappedEngine().update(recid, value, serializer);
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        return getWrappedEngine().compareAndSwap(recid, expectedOldValue, newValue, serializer);
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        getWrappedEngine().delete(recid, serializer);
    }

    @Override
    public void close() {
        Engine e = engine;
        try{
            if(e!=null)
                e.close();
        } finally {
            engine = CLOSED;
        }
    }

    @Override
    public boolean isClosed() {
        return engine==CLOSED || engine==null;
    }

    @Override
    public void commit() {
        getWrappedEngine().commit();
    }

    @Override
    public void rollback() {
        getWrappedEngine().rollback();
    }


    @Override
    public boolean isReadOnly() {
        return getWrappedEngine().isReadOnly();
    }

    @Override
    public boolean canRollback() {
        return getWrappedEngine().canRollback();
    }

    @Override
    public boolean canSnapshot() {
        return getWrappedEngine().canSnapshot();
    }

    @Override
    public Engine snapshot() throws UnsupportedOperationException {
        return getWrappedEngine().snapshot();
    }

    @Override
    public void clearCache() {
        getWrappedEngine().clearCache();
    }

    @Override
    public void compact() {
        getWrappedEngine().compact();
    }

    @Override
    public  SerializerPojo getSerializerPojo() {
        return getWrappedEngine().getSerializerPojo();
    }


    public Engine getWrappedEngine(){
        return checkClosed(engine);
    }

    protected static <V> V checkClosed(V v){
        if(v==null) throw new IllegalAccessError("DB has been closed");
        return v;
    }


    /**
     * Wraps an <code>Engine</code> and throws
     * <code>UnsupportedOperationException("Read-only")</code>
     * on any modification attempt.
     */
    public static class ReadOnlyEngine extends EngineWrapper {


        public ReadOnlyEngine(Engine engine){
            super(engine);
        }


        @Override
        public long preallocate() {
            throw new UnsupportedOperationException("Read-only");
        }

        @Override
        public void preallocate(long[] recids){
            throw new UnsupportedOperationException("Read-only");
        }


        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            throw new UnsupportedOperationException("Read-only");
        }

        @Override
        public <A> long put(A value, Serializer<A> serializer) {
            throw new UnsupportedOperationException("Read-only");
        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            throw new UnsupportedOperationException("Read-only");
        }

        @Override
        public <A> void delete(long recid, Serializer<A> serializer){
            throw new UnsupportedOperationException("Read-only");
        }

        @Override
        public void commit() {
            throw new UnsupportedOperationException("Read-only");
        }

        @Override
        public void rollback() {
            throw new UnsupportedOperationException("Read-only");
        }


        @Override
        public boolean isReadOnly() {
            return true;
        }

        @Override
        public boolean canSnapshot() {
            return true;
        }

        @Override
        public Engine snapshot() throws UnsupportedOperationException {
            return this;
        }

    }


    /**
     * check if Record Instances were not modified while in cache.
     * Usuful to diagnose strange problems with Instance Cache.
     */
    public static class ImmutabilityCheckEngine extends EngineWrapper{

        protected static class Item {
            final Serializer serializer;
            final Object item;
            final int oldChecksum;

            public Item(Serializer serializer, Object item) {
                if(item==null || serializer==null) throw new AssertionError("null");
                this.serializer = serializer;
                this.item = item;
                oldChecksum = checksum();
                if(oldChecksum!=checksum()) throw new AssertionError("inconsistent serialization");
            }

            private int checksum(){
                try {
                    DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
                    serializer.serialize(out, item);
                    byte[] bb = out.copyBytes();
                    return Arrays.hashCode(bb);
                }catch(IOException e){
                    throw new IOError(e);
                }
            }

            void check(){
                int newChecksum = checksum();
                if(oldChecksum!=newChecksum) throw new AssertionError("Record instance was modified: \n  "+item+"\n  "+serializer);
            }
        }

        protected LongConcurrentHashMap<Item> items = new LongConcurrentHashMap<Item>();

        protected ImmutabilityCheckEngine(Engine engine) {
            super(engine);
        }

        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            Item item = items.get(recid);
            if(item!=null) item.check();
            A ret = super.get(recid, serializer);
            if(ret!=null) items.put(recid, new Item(serializer,ret));
            return ret;
        }

        @Override
        public <A> long put(A value, Serializer<A> serializer) {
            long ret =  super.put(value, serializer);
            if(value!=null) items.put(ret, new Item(serializer,value));
            return ret;
        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            Item item = items.get(recid);
            if(item!=null) item.check();
            super.update(recid, value, serializer);
            if(value!=null) items.put(recid, new Item(serializer,value));
        }

        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            Item item = items.get(recid);
            if(item!=null) item.check();
            boolean ret = super.compareAndSwap(recid, expectedOldValue, newValue, serializer);
            if(ret && newValue!=null) items.put(recid, new Item(serializer,item));
            return ret;
        }

        @Override
        public void close() {
            super.close();
            for(Iterator<Item> iter = items.valuesIterator(); iter.hasNext();){
                iter.next().check();
            }
            items.clear();
        }
    }
    
    
    /** Engine wrapper with all methods synchronized on global lock, useful to diagnose concurrency issues.*/ 
    public static class SynchronizedEngineWrapper extends EngineWrapper{

        protected SynchronizedEngineWrapper(Engine engine) {
            super(engine);
        }

        @Override
        synchronized public long preallocate(){
            return super.preallocate();
        }

        @Override
        synchronized public void preallocate(long[] recids){
            super.preallocate(recids);
        }


        @Override
        synchronized public <A> long put(A value, Serializer<A> serializer) {
            return super.put(value, serializer);
        }

        @Override
        synchronized public <A> A get(long recid, Serializer<A> serializer) {
            return super.get(recid, serializer);
        }

        @Override
        synchronized public <A> void update(long recid, A value, Serializer<A> serializer) {
            super.update(recid, value, serializer);
        }

        @Override
        synchronized public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            return super.compareAndSwap(recid, expectedOldValue, newValue, serializer);
        }

        @Override
        synchronized public <A> void delete(long recid, Serializer<A> serializer) {
            super.delete(recid, serializer);
        }

        @Override
        synchronized public void close() {
            super.close();
        }

        @Override
        synchronized public boolean isClosed() {
            return super.isClosed();
        }

        @Override
        synchronized public void commit() {
            super.commit();
        }

        @Override
        synchronized public void rollback() {
            super.rollback();
        }

        @Override
        synchronized public boolean isReadOnly() {
            return super.isReadOnly();
        }

        @Override
        synchronized public boolean canSnapshot() {
            return super.canSnapshot();
        }

        @Override
        synchronized public Engine snapshot() throws UnsupportedOperationException {
            return super.snapshot();
        }

        @Override
        synchronized public void compact() {
            super.compact();
        }
    }


    /** Checks that Serializer used to serialize item is the same as Serializer used to deserialize it*/
    public static class SerializerCheckEngineWrapper extends EngineWrapper{

        protected LongMap<Serializer> recid2serializer = new LongConcurrentHashMap<Serializer>();

        protected SerializerCheckEngineWrapper(Engine engine) {
            super(engine);
        }


        synchronized protected <A> void checkSerializer(long recid, Serializer<A> serializer) {
            Serializer other = recid2serializer.get(recid);
            if(other!=null){
                if( other!=serializer && other.getClass()!=serializer.getClass())
                    throw new IllegalArgumentException("Serializer does not match. \n found: "+serializer+" \n expected: "+other);
            }else
                recid2serializer.put(recid,serializer);
        }

        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            checkSerializer(recid, serializer);
            return super.get(recid, serializer);
        }


        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            checkSerializer(recid, serializer);
            super.update(recid, value, serializer);
        }

        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            checkSerializer(recid, serializer);
            return super.compareAndSwap(recid, expectedOldValue, newValue, serializer);
        }

        @Override
        public <A> void delete(long recid, Serializer<A> serializer) {
            checkSerializer(recid, serializer);
            recid2serializer.remove(recid);
            super.delete(recid, serializer);
        }
    }


    /** throws `IllegalArgumentError("already closed)` on all access */
    public static final Engine CLOSED = new Engine(){


        @Override
        public long preallocate() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public void preallocate(long[] recids) {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public <A> long put(A value, Serializer<A> serializer) {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public <A> void delete(long recid, Serializer<A> serializer) {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public void close() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public boolean isClosed() {
            return true;
        }

        @Override
        public void commit() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public void rollback() throws UnsupportedOperationException {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public boolean isReadOnly() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public boolean canRollback() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public boolean canSnapshot() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public Engine snapshot() throws UnsupportedOperationException {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public void clearCache() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public void compact() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public SerializerPojo getSerializerPojo() {
            throw new IllegalAccessError("already closed");
        }


    };

    /**
     * Closes Engine on JVM shutdown using shutdown hook: {@link Runtime#addShutdownHook(Thread)}
     * If engine was closed by user before JVM shutdown, hook is removed to save memory.
     */
    public static class CloseOnJVMShutdown extends EngineWrapper{

        final protected AtomicBoolean shutdownHappened = new AtomicBoolean(false);

        final Runnable hookRunnable = new Runnable() {
            @Override
            public void run() {
                shutdownHappened.set(true);
                CloseOnJVMShutdown.this.hook = null;
                if(CloseOnJVMShutdown.this.isClosed())
                    return;
                CloseOnJVMShutdown.this.close();
            }
        };

        Thread hook;


        public CloseOnJVMShutdown(Engine engine) {
            super(engine);
            hook = new Thread(hookRunnable,"MapDB shutdown hook");
            Runtime.getRuntime().addShutdownHook(hook);
        }

        @Override
        public void close() {
            super.close();
            if(!shutdownHappened.get() && hook!=null){
                Runtime.getRuntime().removeShutdownHook(hook);
            }
            hook = null;
        }
    }
}
