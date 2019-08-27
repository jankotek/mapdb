package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.CC;
import org.mapdb.DBException;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataInput2ByteArray;
import org.mapdb.io.DataOutput2;
import org.mapdb.io.DataOutput2ByteArray;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

/** wraps another serializer and (de)compresses its output/input*/
public final class CompressionSerializerWrapper<E,G> implements GroupSerializer<E,G>, Serializable {

    private static final long serialVersionUID = 4440826457939614346L;
    protected final GroupSerializer<E,G> serializer;
    protected final ThreadLocal<CompressLZF> LZF = new ThreadLocal<CompressLZF>() {
        @Override protected CompressLZF initialValue() {
            return new CompressLZF();
        }
    };

    public CompressionSerializerWrapper(GroupSerializer<E,G> serializer) {
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
        DataOutput2ByteArray out2 = new DataOutput2ByteArray();
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

    @Nullable
    @Override
    public Class serializedType() {
        return serializer.serializedType();
    }


    @Override
    public E deserialize(DataInput2 in) throws IOException {
        final int unpackedSize = in.unpackInt()-1;
        if(unpackedSize==-1){
            //was not compressed
            return serializer.deserialize(in);
        }

        byte[] unpacked = new byte[unpackedSize];
        LZF.get().expand(in,unpacked,0,unpackedSize);
        DataInput2ByteArray in2 = new DataInput2ByteArray(unpacked);
        E ret =  serializer.deserialize(in2,unpackedSize);
        if(CC.ASSERT && ! (in2.getPos()==unpackedSize))
            throw new DBException.DataCorruption( "data were not fully read");
        return ret;
    }

    //TODO fixed size ser @Override
    public E deserialize(DataInput2 in, int available) throws IOException {
        final int unpackedSize = in.unpackInt()-1;
        if(unpackedSize==-1){
            //was not compressed
            return serializer.deserialize(in, available>0?available-1:available);
        }

        byte[] unpacked = new byte[unpackedSize];
        LZF.get().expand(in,unpacked,0,unpackedSize);
        DataInput2ByteArray in2 = new DataInput2ByteArray(unpacked);
        E ret =  serializer.deserialize(in2,unpackedSize);
        if(CC.ASSERT && ! (in2.getPos()==unpackedSize))
            throw new DBException.DataCorruption( "data were not fully read");
        return ret;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompressionSerializerWrapper that = (CompressionSerializerWrapper) o;
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
    public int valueArraySearch(G keys, E key) {
        return serializer.valueArraySearch(keys, key);
    }

    @Override
    public int valueArraySearch(G keys, E key, Comparator comparator) {
        return serializer.valueArraySearch(keys, key, comparator);
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, G vals) throws IOException {
        DataOutput2ByteArray out2 = new DataOutput2ByteArray();
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
    public G valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        if(size==0)
            return serializer.valueArrayEmpty();

        final int unpackedSize = in.unpackInt()-1;
        if(unpackedSize==-1){
            //was not compressed
            return serializer.valueArrayDeserialize(in,size);
        }

        byte[] unpacked = new byte[unpackedSize];
        LZF.get().expand(in,unpacked,0,unpackedSize);
        DataInput2ByteArray in2 = new DataInput2ByteArray(unpacked);
        G ret =  serializer.valueArrayDeserialize(in2, size);
        if(CC.ASSERT && ! (in2.getPos()==unpackedSize))
            throw new DBException.DataCorruption( "data were not fully read");
        return ret;
    }

    @Override
    public E valueArrayGet(G vals, int pos) {
        return serializer.valueArrayGet(vals, pos);
    }

    @Override
    public int valueArraySize(G vals) {
        return serializer.valueArraySize(vals);
    }

    @Override
    public G valueArrayEmpty() {
        return serializer.valueArrayEmpty();
    }

    @Override
    public G valueArrayPut(G vals, int pos, E newValue) {
        return serializer.valueArrayPut(vals, pos, newValue);
    }

    @Override
    public G valueArrayUpdateVal(G vals, int pos, E newValue) {
        return serializer.valueArrayUpdateVal(vals, pos, newValue);
    }

    @Override
    public G valueArrayFromArray(Object[] objects) {
        return serializer.valueArrayFromArray(objects);
    }

    @Override
    public G valueArrayCopyOfRange(G vals, int from, int to) {
        return  serializer.valueArrayCopyOfRange(vals, from, to);
    }

    @Override
    public G valueArrayDeleteValue(G vals, int pos) {
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
