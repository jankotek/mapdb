package org.mapdb;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Wrapper which transform binary data. Useful for compression or encryption
 *
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
    public Long getNamedRecid(String name) {
        return engine.getNamedRecid(name);
    }

    @Override
    public void setNamedRecid(String name, Long recid) {
        engine.setNamedRecid(name, recid);
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

}
