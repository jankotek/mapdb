package org.mapdb.serializer;

import org.mapdb.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

/** wraps another serializer and (de)compresses its output/input*/
public final class SerializerCompressionWrapper<E> implements GroupSerializer<E>, Serializable {

    private static final long serialVersionUID = 4440826457939614346L;
    protected final GroupSerializer<E> serializer;
    protected final ThreadLocal<CompressLZF> LZF = new ThreadLocal<CompressLZF>() {
        @Override protected CompressLZF initialValue() {
            return new CompressLZF();
        }
    };

    public SerializerCompressionWrapper(GroupSerializer<E> serializer) {
        this.serializer = serializer;
    }



//        /** used for deserialization */
//        @SuppressWarnings("unchecked")
//		protected SerializerCompressionWrapper(SerializerBase serializerBase, DataInput2 is, SerializerBase.FastArrayList<Object> objectStack, boolean compressValues) throws IOException {
//            objectStack.add(this);
//            this.serializer = (Serializer<E>) serializerBase.deserialize(is,objectStack);
//            this.compressValues = compressValues;
//        }


    @Override
    public void serialize(DataOutput2 out, E value) throws IOException {
        DataOutput2 out2 = new DataOutput2();
        serializer.serialize(out2,value);

        byte[] tmp = new byte[out2.pos+41];
        int newLen;
        try{
            newLen = LZF.get().compress(out2.buf,out2.pos,tmp,0);
        }catch(IndexOutOfBoundsException e){
            newLen=0; //larger after compression
        }
        if(newLen>=out2.pos||newLen==0){
            //compression adds size, so do not compress
            out.packInt(0);
            out.write(out2.buf,0,out2.pos);
            return;
        }

        out.packInt( out2.pos+1); //unpacked size, zero indicates no compression
        out.write(tmp,0,newLen);
    }

    @Override
    public E deserialize(DataInput2 in, int available) throws IOException {
        final int unpackedSize = in.unpackInt()-1;
        if(unpackedSize==-1){
            //was not compressed
            return serializer.deserialize(in, available>0?available-1:available);
        }

        byte[] unpacked = new byte[unpackedSize];
        LZF.get().expand(in,unpacked,0,unpackedSize);
        DataInput2.ByteArray in2 = new DataInput2.ByteArray(unpacked);
        E ret =  serializer.deserialize(in2,unpackedSize);
        if(CC.ASSERT && ! (in2.pos==unpackedSize))
            throw new DBException.DataCorruption( "data were not fully read");
        return ret;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SerializerCompressionWrapper that = (SerializerCompressionWrapper) o;
        return serializer.equals(that.serializer);
    }

    @Override
    public int hashCode() {
        return serializer.hashCode();
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public int valueArraySearch(Object keys, E key) {
        return serializer.valueArraySearch(keys, key);
    }

    @Override
    public int valueArraySearch(Object keys, E key, Comparator comparator) {
        return serializer.valueArraySearch(keys, key, comparator);
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, Object vals) throws IOException {

        DataOutput2 out2 = new DataOutput2();
        serializer.valueArraySerialize(out2, vals);

        if(out2.pos==0)
            return;


        byte[] tmp = new byte[out2.pos+41];
        int newLen;
        try{
            newLen = LZF.get().compress(out2.buf,out2.pos,tmp,0);
        }catch(IndexOutOfBoundsException e){
            newLen=0; //larger after compression
        }
        if(newLen>=out2.pos||newLen==0){
            //compression adds size, so do not compress
            out.packInt(0);
            out.write(out2.buf,0,out2.pos);
            return;
        }

        out.packInt( out2.pos+1); //unpacked size, zero indicates no compression
        out.write(tmp,0,newLen);
    }

    @Override
    public Object valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        if(size==0)
            return serializer.valueArrayEmpty();

        final int unpackedSize = in.unpackInt()-1;
        if(unpackedSize==-1){
            //was not compressed
            return serializer.valueArrayDeserialize(in,size);
        }

        byte[] unpacked = new byte[unpackedSize];
        LZF.get().expand(in,unpacked,0,unpackedSize);
        DataInput2.ByteArray in2 = new DataInput2.ByteArray(unpacked);
        Object ret =  serializer.valueArrayDeserialize(in2, size);
        if(CC.ASSERT && ! (in2.pos==unpackedSize))
            throw new DBException.DataCorruption( "data were not fully read");
        return ret;
    }

    @Override
    public E valueArrayGet(Object vals, int pos) {
        return serializer.valueArrayGet(vals, pos);
    }

    @Override
    public int valueArraySize(Object vals) {
        return serializer.valueArraySize(vals);
    }

    @Override
    public Object valueArrayEmpty() {
        return serializer.valueArrayEmpty();
    }

    @Override
    public Object valueArrayPut(Object vals, int pos, E newValue) {
        return serializer.valueArrayPut(vals, pos, newValue);
    }

    @Override
    public Object valueArrayUpdateVal(Object vals, int pos, E newValue) {
        return serializer.valueArrayUpdateVal(vals, pos, newValue);
    }

    @Override
    public Object valueArrayFromArray(Object[] objects) {
        return serializer.valueArrayFromArray(objects);
    }

    @Override
    public Object valueArrayCopyOfRange(Object vals, int from, int to) {
        return  serializer.valueArrayCopyOfRange(vals, from, to);
    }

    @Override
    public Object valueArrayDeleteValue(Object vals, int pos) {
        return serializer.valueArrayDeleteValue(vals, pos);
    }

    @Override
    public boolean equals(E a1, E a2) {
        return serializer.equals(a1, a2);
    }

    @Override
    public int hashCode(E e, int seed) {
        return serializer.hashCode(e, seed);
    }

    @Override
    public int compare(E o1, E o2) {
        return serializer.compare(o1, o2);
    }
}
