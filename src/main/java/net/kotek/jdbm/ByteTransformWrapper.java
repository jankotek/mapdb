package net.kotek.jdbm;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Wrapper which transform binary data. Useful for compression or encryption
 *
 */

public class ByteTransformWrapper implements RecordManager {


    protected RecordManager recman;
    protected Serializer<byte[]> blockSerializer;

    public ByteTransformWrapper(RecordManager recman, Serializer<byte[]> blockSerializer) {
        this.recman = recman;
        this.blockSerializer = blockSerializer;
    }

    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        //serialize to byte array, and pass it down with alternative serializer
        try {
            if(value ==null){
                return recman.recordPut(null, blockSerializer);
            }

            DataOutput2 out = new DataOutput2();
            serializer.serialize(out,value);
            byte[] b = out.copyBytes();

            return recman.recordPut(b, blockSerializer);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        //get decompressed array
        try {
            byte[] b = recman.recordGet(recid, blockSerializer);
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

            recman.recordUpdate(recid, b, blockSerializer);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public void recordDelete(long recid) {
        recman.recordDelete(recid);
    }

    @Override
    public Long getNamedRecid(String name) {
        return recman.getNamedRecid(name);
    }

    @Override
    public void setNamedRecid(String name, Long recid) {
        recman.setNamedRecid(name, recid);
    }

    @Override
    public void close() {
        recman.close();
        recman = null;
        blockSerializer = null;
    }

    @Override
    public void commit() {
        recman.commit();
    }

    @Override
    public void rollback() {
        recman.rollback();
    }

}
