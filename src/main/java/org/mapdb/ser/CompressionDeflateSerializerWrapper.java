package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.CC;
import org.mapdb.DBException;
import org.mapdb.io.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

/** wraps another serializer and (de)compresses its output/input using Deflate*/
public final class CompressionDeflateSerializerWrapper<E, G> implements GroupSerializer<E, G>, Serializable {

    private static final long serialVersionUID = 8529699349939823553L;
    protected final GroupSerializer<E,G> serializer;
    protected final int compressLevel;
    protected final byte[] dictionary;

    public CompressionDeflateSerializerWrapper(GroupSerializer<E,G> serializer) {
        this(serializer, Deflater.DEFAULT_STRATEGY, null);
    }

    public CompressionDeflateSerializerWrapper(GroupSerializer<E,G> serializer, int compressLevel, byte[] dictionary) {
        this.serializer = serializer;
        this.compressLevel = compressLevel;
        this.dictionary = dictionary==null || dictionary.length==0 ? null : dictionary;
    }

//        /** used for deserialization */
//        @SuppressWarnings("unchecked")
//        protected SerializerCompressionDeflateWrapper(SerializerBase serializerBase, DataInput2 is, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
//            objectStack.add(this);
//            this.serializer = (Serializer<E>) serializerBase.deserialize(is,objectStack);
//            this.compressLevel = is.readByte();
//            int dictlen = is.unpackInt();
//            if(dictlen==0) {
//                dictionary = null;
//            } else {
//                byte[] d = new byte[dictlen];
//                is.readFully(d);
//                dictionary = d;
//            }
//        }


    @Nullable
    @Override
    public Class serializedType() {
        return serializer.serializedType();
    }

    @Override
    public void serialize(DataOutput2 out, E value) throws IOException {
        DataOutput2ByteArray out2 = new DataOutput2ByteArray();
        serializer.serialize(out2,value);

        byte[] tmp = new byte[out2.pos+41];
        int newLen;
        try{
            Deflater deflater = new Deflater(compressLevel);
            if(dictionary!=null) {
                deflater.setDictionary(dictionary);
            }

            deflater.setInput(out2.buf,0,out2.pos);
            deflater.finish();
            newLen = deflater.deflate(tmp);
            //LZF.get().compress(out2.buf,out2.pos,tmp,0);
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
    public E deserialize(DataInput2 in) throws IOException {
        final int unpackedSize = in.unpackInt()-1;
        if(unpackedSize==-1){
            //was not compressed
            return serializer.deserialize(in); // TODO fixed size, available>0?available-1:available
        }

        Inflater inflater = new Inflater();
        if(dictionary!=null) {
            inflater.setDictionary(dictionary);
        }

        InflaterInputStream in4 = new InflaterInputStream(
                new DataInputToStream(in), inflater);

        byte[] unpacked = new byte[unpackedSize];
        in4.read(unpacked,0,unpackedSize);

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

        CompressionDeflateSerializerWrapper that = (CompressionDeflateSerializerWrapper) o;

        if (compressLevel != that.compressLevel) return false;
        if (!serializer.equals(that.serializer)) return false;
        return Arrays.equals(dictionary, that.dictionary);

    }

    @Override
    public int hashCode() {
        int result = serializer.hashCode();
        result = 31 * result + compressLevel;
        result = 31 * result + (dictionary != null ? Arrays.hashCode(dictionary) : 0);
        return result;
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
        serializer.valueArraySerialize(out2,vals);
        if(out2.pos==0)
            return;

        byte[] tmp = new byte[out2.pos+41];
        int newLen;
        try{
            Deflater deflater = new Deflater(compressLevel);
            if(dictionary!=null) {
                deflater.setDictionary(dictionary);
            }

            deflater.setInput(out2.buf,0,out2.pos);
            deflater.finish();
            newLen = deflater.deflate(tmp);
            //LZF.get().compress(out2.buf,out2.pos,tmp,0);
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
        if(size==0) {
            return serializer.valueArrayEmpty();
        }

        //decompress all values in single blob, it has better compressibility
        final int unpackedSize = in.unpackInt()-1;
        if(unpackedSize==-1){
            //was not compressed
            return serializer.valueArrayDeserialize(in,size);
        }

        Inflater inflater = new Inflater();
        if(dictionary!=null) {
            inflater.setDictionary(dictionary);
        }

        InflaterInputStream in4 = new InflaterInputStream(
                new DataInputToStream(in), inflater);

        byte[] unpacked = new byte[unpackedSize];
        in4.read(unpacked,0,unpackedSize);

        //now got data unpacked, so use serializer to deal with it

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
        return serializer.valueArrayCopyOfRange(vals, from, to);
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
