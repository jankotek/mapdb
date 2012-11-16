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

/**
 * Wrapper which transform binary data. Useful for compression or encryption
 *
 * @author Jan Kotek
 */
public class ByteTransformEngine implements Engine {


    protected Engine engine;
    protected Serializer<byte[]> blockSerializer;

    public ByteTransformEngine(Engine engine, Serializer<byte[]> blockSerializer) {
        this.engine = engine;
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
    public void recordDelete(long recid) {
        engine.recordDelete(recid);
    }


    @Override
    public void close() {
        engine.close();
        engine = null;
        blockSerializer = null;
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

}
