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


import java.io.*;
import java.nio.*;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * EngineWrapper adapter. It implements all methods on Engine interface.
 *
 * @author Jan Kotek
 */
public class EngineWrapper implements Engine{

    protected Engine engine;

    protected EngineWrapper(Engine engine){
        this.engine = engine;
    }

    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        return engine.recordPut(value, serializer);
    }

    @Override
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        return engine.recordGet(recid, serializer);
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        engine.recordUpdate(recid, value, serializer);
    }

    @Override
    public <A> boolean recordCompareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        return engine.recordCompareAndSwap(recid, expectedOldValue, newValue, serializer);
    }

    @Override
    public void recordDelete(long recid) {
        engine.recordDelete(recid);
    }

    @Override
    public void close() {
        engine.close();
        engine = null;
    }

    @Override
    public void commit() {
        engine.commit();
    }

    @Override
    public void rollback() {
        engine.rollback();
    }

    @Override
    public long serializerRecid() {
        return engine.serializerRecid();
    }

    @Override
    public long nameDirRecid() {
        return engine.nameDirRecid();
    }

    @Override
    public boolean isReadOnly() {
        return engine.isReadOnly();
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
        public <A> boolean recordCompareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            throw new UnsupportedOperationException("Read-only");
        }

        @Override
        public <A> long recordPut(A value, Serializer<A> serializer) {
            throw new UnsupportedOperationException("Read-only");
        }

        @Override
        public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
            throw new UnsupportedOperationException("Read-only");
        }

        @Override
        public void recordDelete(long recid) {
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
        public <A> long recordPut(A value, Serializer<A> serializer) {
            //serialize to byte array, and pass it down with alternative serializer
            try {
                if(value ==null){
                    return engine.recordPut(null, blockSerializer);
                }

                DataOutput2 out = new DataOutput2();
                serializer.serialize(out,value);
                byte[] b = out.copyBytes();

                return engine.recordPut(b, blockSerializer);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public <A> A recordGet(long recid, Serializer<A> serializer) {
            //get decompressed array
            try {
                byte[] b = engine.recordGet(recid, blockSerializer);
                if(b==null) return null;

                //deserialize
                DataInput2 in = new DataInput2(ByteBuffer.wrap(b),0);

                return serializer.deserialize(in,b.length);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
            //serialize to byte array, and pass it down with alternative serializer
            try {
                DataOutput2 out = new DataOutput2();
                serializer.serialize(out,value);
                byte[] b = out.copyBytes();

                engine.recordUpdate(recid, b, blockSerializer);
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
        public <A> long recordPut(A value, Serializer<A> serializer) {
            long recid =  super.recordPut(value, serializer);
            records.add(new Record(recid,
                    "INSERT \n  val:"+value+"\n  ser:"+serializer
            ));
            return recid;
        }

        @Override
        public <A> A recordGet(long recid, Serializer<A> serializer) {
            A ret =  super.recordGet(recid, serializer);
            records.add(new Record(recid,
                    "GET \n  val:"+ret+"\n  ser:"+serializer
            ));
            return ret;
        }

        @Override
        public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
            super.recordUpdate(recid, value, serializer);
            records.add(new Record(recid,
                    "UPDATE \n  val:"+value+"\n  ser:"+serializer
            ));

        }

        @Override
        public void recordDelete(long recid) {
            super.recordDelete(recid);
            records.add(new Record(recid,"DEL"));
        }
    }

    public Engine getWrappedEngine(){
        return engine;
    }


}
