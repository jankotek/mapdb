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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * EngineWrapper adapter. It implements all methods on Engine interface.
 *
 * @author Jan Kotek
 */
public abstract class EngineWrapper implements Engine{

    private Engine engine;

    protected EngineWrapper(Engine engine){
        if(engine == null) throw new IllegalArgumentException();
        this.engine = engine;
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
        if(e!=null)
            e.close();
        engine = null;
    }

    @Override
    public boolean isClosed() {
        return engine==null;
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
    public void clearCache() {
        getWrappedEngine().clearCache();
    }

    @Override
    public void compact() {
        getWrappedEngine().compact();
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


    }

    /**
     * Wrapper which transform binary data. Useful for compression or encryption
     */
    public static class ByteTransformEngine extends  EngineWrapper {

        //TODO compare and swap

        protected Serializer<byte[]> blockSerializer;

        public ByteTransformEngine(Engine engine, Serializer<byte[]> blockSerializer) {
            super(engine);
            this.blockSerializer = blockSerializer;
        }

        @Override
        public <A> long put(A value, Serializer<A> serializer) {
            //serialize to byte array, and pass it down with alternative serializer
            try {
                Engine e = getWrappedEngine();
                Serializer<byte[]> ser = checkClosed(blockSerializer);

                if(value ==null){
                    return e.put(null, ser);
                }

                DataOutput2 out = new DataOutput2();
                serializer.serialize(out,value);
                byte[] b = out.copyBytes();

                return e.put(b, ser);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            //get decompressed array
            try {
                byte[] b = getWrappedEngine().get(recid, checkClosed(blockSerializer));
                if(b==null) return null;

                //deserialize
                DataInput2 in = new DataInput2(ByteBuffer.wrap(b),0);

                return serializer.deserialize(in,b.length);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            //serialize to byte array, and pass it down with alternative serializer
            try {
                DataOutput2 out = new DataOutput2();
                serializer.serialize(out,value);
                byte[] b = out.copyBytes();

                getWrappedEngine().update(recid, b, checkClosed(blockSerializer));
            } catch (IOException e) {
                throw new IOError(e);
            }
        }


        @Override
        public void close() {
            super.close();
            blockSerializer = null;
        }


    }

    public static class DebugEngine extends EngineWrapper{

        //TODO CAS

        final Queue<Record> records = new ConcurrentLinkedQueue<Record>();


        protected static final class Record{
            final long recid;
            final String desc;
//            final String thread = Thread.currentThread().getName();
//            final Exception stackTrace = new Exception();

            public Record(long recid, String desc) {
                this.recid = recid;
                this.desc = desc;
            }
        }

        public DebugEngine(Engine engine) {
            super(engine);
        }

        @Override
        public <A> long put(A value, Serializer<A> serializer) {
            long recid =  super.put(value, serializer);
            records.add(new Record(recid,
                    "INSERT \n  val:"+value+"\n  ser:"+serializer
            ));
            return recid;
        }

        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            A ret =  super.get(recid, serializer);
            records.add(new Record(recid,
                    "GET \n  val:"+ret+"\n  ser:"+serializer
            ));
            return ret;
        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            super.update(recid, value, serializer);
            records.add(new Record(recid,
                    "UPDATE \n  val:"+value+"\n  ser:"+serializer
            ));

        }

        @Override
        public <A> void delete(long recid, Serializer<A> serializer){
            super.delete(recid,serializer);
            records.add(new Record(recid,"DEL"));
        }
    }

    public Engine getWrappedEngine(){
        return checkClosed(engine);
    }

    protected static <V> V checkClosed(V v){
        if(v==null) throw new IllegalAccessError("DB has been closed");
        return v;
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
                    DataOutput2 out = new DataOutput2();
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
        synchronized public void compact() {
            super.compact();
        }
    }

}
